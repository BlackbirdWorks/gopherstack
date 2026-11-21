package backup_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

var (
	errRestoreFailed  = errors.New("DBClusterAlreadyExistsFault")
	errSnapshotAbsent = errors.New("snapshot not found")
	errBackupAbsent   = errors.New("BackupNotFound")
)

type fakeClusters struct {
	restoreEr error
	existing  map[string]bool
	snapshots map[string]string
	restored  []string
	mu        sync.Mutex
}

func newFakeClusters(ids ...string) *fakeClusters {
	f := &fakeClusters{existing: map[string]bool{}, snapshots: map[string]string{}}
	for _, id := range ids {
		f.existing[id] = true
	}

	return f
}

func (f *fakeClusters) HasCluster(_ context.Context, id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.existing[id]
}

func (f *fakeClusters) CreateSnapshot(_ context.Context, snapshotID, clusterID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snapshots[snapshotID] = clusterID

	return nil
}

func (f *fakeClusters) RestoreSnapshot(_ context.Context, snapshotID, clusterID string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.restoreEr != nil {
		return "", f.restoreEr
	}

	if _, ok := f.snapshots[snapshotID]; !ok {
		return "", errSnapshotAbsent
	}

	f.restored = append(f.restored, clusterID)

	return "arn:aws:rds:us-east-1:123456789012:cluster:" + clusterID, nil
}

func (f *fakeClusters) DeleteSnapshot(_ context.Context, snapshotID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.snapshots, snapshotID)

	return nil
}

func (f *fakeClusters) snapshotCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.snapshots)
}

func TestClusterEngine_ResolvesOwningService(t *testing.T) {
	t.Parallel()

	const clusterArn = "arn:aws:rds:us-east-1:123456789012:cluster:orders"

	tests := []struct {
		name     string
		owner    string
		wantType string
	}{
		{name: "aurora", owner: "Aurora", wantType: "Aurora"},
		{name: "documentdb", owner: "DocumentDB", wantType: "DocumentDB"},
		{name: "neptune", owner: "Neptune", wantType: "Neptune"},
		{name: "unowned", owner: "", wantType: "Aurora"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			fakes := map[string]*fakeClusters{}

			for _, rt := range []string{"Aurora", "DocumentDB", "Neptune"} {
				f := newFakeClusters()
				if rt == tt.owner {
					f = newFakeClusters("orders")
				}

				fakes[rt] = f
				b.SetClusterService(rt, f)
			}

			v := mustVault(t, b, "vault")
			rp := completedRecoveryPoint(t, b, v.BackupVaultName, clusterArn, "Aurora")

			got, err := b.DescribeRecoveryPoint(v.BackupVaultName, rp)
			require.NoError(t, err)
			assert.Equal(t, tt.wantType, got.ResourceType)

			if tt.owner == "" {
				assert.Empty(t, got.Snapshot)

				return
			}

			assert.Equal(t, 1, fakes[tt.owner].snapshotCount())
		})
	}
}

func TestClusterEngine_Restore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		restoreErr error
		metadata   map[string]string
		name       string
		wantStatus string
		wantTarget string
		wantMsg    string
	}{
		{name: "default_name", wantStatus: "COMPLETED", wantTarget: "orders-restored"},
		{
			name: "db_cluster_identifier", metadata: map[string]string{"dbclusteridentifier": "orders-copy"},
			wantStatus: "COMPLETED", wantTarget: "orders-copy",
		},
		{
			name: "cluster_identifier", metadata: map[string]string{"ClusterIdentifier": "wh-copy"},
			wantStatus: "COMPLETED", wantTarget: "wh-copy",
		},
		{
			name: "service_error", restoreErr: errRestoreFailed, wantStatus: "FAILED",
			wantMsg: "DBClusterAlreadyExistsFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			f := newFakeClusters("orders")
			b.SetClusterService("Aurora", f)
			v := mustVault(t, b, "vault")
			rp := completedRecoveryPoint(t, b, v.BackupVaultName,
				"arn:aws:rds:us-east-1:123456789012:cluster:orders", "Aurora")

			f.restoreEr = tt.restoreErr

			md := tt.metadata
			if md == nil {
				md = map[string]string{"k": "v"}
			}

			job, err := b.StartRestoreJob(rp, testRole, "Aurora", md)
			require.NoError(t, err)

			assert.Equal(t, tt.wantStatus, job.Status)
			assert.Contains(t, job.StatusMessage, tt.wantMsg)

			if tt.wantTarget != "" {
				assert.Equal(t, []string{tt.wantTarget}, f.restored)
				assert.Equal(t, "arn:aws:rds:us-east-1:123456789012:cluster:"+tt.wantTarget, job.CreatedResourceArn)
			}
		})
	}
}

func TestClusterEngine_DeleteRecoveryPointDiscardsSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		copyFirst      bool
		wantAfterFirst int
	}{
		{name: "sole_owner", wantAfterFirst: 0},
		{name: "copy_keeps_snapshot", copyFirst: true, wantAfterFirst: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			f := newFakeClusters("orders")
			b.SetClusterService("Aurora", f)
			src := mustVault(t, b, "src")
			dst := mustVault(t, b, "dst")
			rp := completedRecoveryPoint(t, b, src.BackupVaultName,
				"arn:aws:rds:us-east-1:123456789012:cluster:orders", "Aurora")

			if tt.copyFirst {
				_, err := b.StartCopyJob(rp, src.BackupVaultName, dst.BackupVaultArn, testRole)
				require.NoError(t, err)
			}

			require.NoError(t, b.DeleteRecoveryPoint(src.BackupVaultName, rp))
			assert.Equal(t, tt.wantAfterFirst, f.snapshotCount())

			if tt.copyFirst {
				rps, err := b.ListRecoveryPointsByBackupVault(dst.BackupVaultName)
				require.NoError(t, err)
				require.Len(t, rps, 1)
				require.NoError(t, b.DeleteRecoveryPoint(dst.BackupVaultName, rps[0].RecoveryPointArn))
				assert.Equal(t, 0, f.snapshotCount())
			}
		})
	}
}

type fakeFSx struct {
	backups   map[string]string
	existing  string
	restoreID string
	opts      backup.FileSystemRestore
}

func (f *fakeFSx) HasFileSystem(id string) bool { return id == f.existing }

func (f *fakeFSx) CreateBackup(fsID string) (string, error) {
	f.backups["backup-1"] = fsID

	return "backup-1", nil
}

func (f *fakeFSx) RestoreBackup(backupID string, opts backup.FileSystemRestore) (string, error) {
	if _, ok := f.backups[backupID]; !ok {
		return "", errBackupAbsent
	}

	f.opts = opts

	return "arn:aws:fsx:us-east-1:123456789012:file-system/" + f.restoreID, nil
}

func (f *fakeFSx) DeleteBackup(backupID string) error {
	delete(f.backups, backupID)

	return nil
}

func TestFSxEngine_BackupAndRestore(t *testing.T) {
	t.Parallel()

	const fsArn = "arn:aws:fsx:us-east-1:123456789012:file-system/fs-0123"

	tests := []struct {
		metadata    map[string]string
		name        string
		resource    string
		wantKms     string
		wantSubnets []string
		wantBackup  bool
	}{
		{
			name: "json_list", resource: fsArn, wantBackup: true,
			metadata:    map[string]string{"SubnetIds": `["subnet-a","subnet-b"]`, "KmsKeyId": "key-1"},
			wantSubnets: []string{"subnet-a", "subnet-b"}, wantKms: "key-1",
		},
		{
			name: "comma_list", resource: fsArn, wantBackup: true,
			metadata:    map[string]string{"subnetids": "subnet-a, subnet-b"},
			wantSubnets: []string{"subnet-a", "subnet-b"},
		},
		{name: "missing_file_system", resource: "arn:aws:fsx:us-east-1:123456789012:file-system/fs-none"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			f := &fakeFSx{backups: map[string]string{}, existing: "fs-0123", restoreID: "fs-9999"}
			b.SetFSxService(f)
			v := mustVault(t, b, "vault")
			rp := completedRecoveryPoint(t, b, v.BackupVaultName, tt.resource, "FSx")

			if !tt.wantBackup {
				assert.Empty(t, f.backups)

				return
			}

			assert.Equal(t, map[string]string{"backup-1": "fs-0123"}, f.backups)

			job, err := b.StartRestoreJob(rp, testRole, "FSx", tt.metadata)
			require.NoError(t, err)
			require.Equal(t, "COMPLETED", job.Status, job.StatusMessage)
			assert.Equal(t, "arn:aws:fsx:us-east-1:123456789012:file-system/fs-9999", job.CreatedResourceArn)
			assert.Equal(t, tt.wantSubnets, f.opts.SubnetIDs)
			assert.Equal(t, tt.wantKms, f.opts.KmsKeyID)

			require.NoError(t, b.DeleteRecoveryPoint(v.BackupVaultName, rp))
			assert.Empty(t, f.backups)
		})
	}
}

func TestStorageGatewayEngine(t *testing.T) {
	t.Parallel()

	const (
		gateway = "arn:aws:storagegateway:us-east-1:123456789012:gateway/sgw-1"
		volume  = gateway + "/volume/vol-AAAA1111"
	)

	tests := []struct {
		name         string
		resource     string
		metadata     map[string]string
		wantGateway  string
		wantSnapshot bool
	}{
		{name: "same_gateway", resource: volume, wantGateway: "sgw-1", wantSnapshot: true},
		{
			name: "gateway_override", resource: volume, wantSnapshot: true, wantGateway: "sgw-2",
			metadata: map[string]string{"gatewayArn": "arn:aws:storagegateway:us-east-1:123456789012:gateway/sgw-2"},
		},
		{name: "not_a_volume", resource: gateway},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			b.SetStorageGatewayRegion("us-east-1", "123456789012")
			v := mustVault(t, b, "vault")
			rp := completedRecoveryPoint(t, b, v.BackupVaultName, tt.resource, "Storage Gateway")

			got, err := b.DescribeRecoveryPoint(v.BackupVaultName, rp)
			require.NoError(t, err)
			assert.Equal(t, tt.wantSnapshot, len(got.Snapshot) > 0)

			if !tt.wantSnapshot {
				return
			}

			md := tt.metadata
			if md == nil {
				md = map[string]string{"k": "v"}
			}

			job, err := b.StartRestoreJob(rp, testRole, "Storage Gateway", md)
			require.NoError(t, err)
			require.Equal(t, "COMPLETED", job.Status, job.StatusMessage)
			want := `^arn:aws:storagegateway:us-east-1:123456789012:gateway/` + tt.wantGateway + `/volume/vol-[0-9a-f]{8}$`
			assert.Regexp(t, want, job.CreatedResourceArn)
		})
	}
}
