package efs //nolint:testpackage // needs ctxRegion (isolation_test.go) + the unexported region context key.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testTransitionToIA is the LifecyclePolicy transition value seeded/asserted
// by Test_InMemoryBackend_SnapshotRestore_FullState, factored into a single
// constant so the literal appears exactly once in this file (backend.go's
// own validation switches already repeat every valid transition value
// multiple times; a second literal site here would trip goconst).
const testTransitionToIA = "AFTER_30_DAYS"

// Test_InMemoryBackend_Restore_RejectsBadSnapshots is the Phase 3.3
// version-guard test: it verifies Restore never partially decodes a snapshot
// it cannot trust -- malformed JSON is reported as an error, and any version
// mismatch (including the pre-Phase-3.3 shape, which has no "version"/
// "tables" keys at all and so decodes with Version == 0) is discarded
// cleanly (backend reset to empty, no error) rather than misinterpreted.
func Test_InMemoryBackend_Restore_RejectsBadSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		data      []byte
		wantError bool
	}{
		{
			name:      "malformed JSON is an error",
			data:      []byte("not-valid-json"),
			wantError: true,
		},
		{
			name:      "version mismatch is discarded cleanly",
			data:      []byte(`{"version":999,"tables":{}}`),
			wantError: false,
		},
		{
			name: "pre-Phase-3.3 shape decodes Version as zero and is discarded",
			data: []byte(
				`{"fileSystems":{"us-east-1":{"fs-seed":{"fileSystemId":"fs-seed"}}},"mountTargets":{}}`,
			),
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateFileSystem(ctxRegion("us-east-1"), CreateFileSystemRequest{CreationToken: "seed"})
			require.NoError(t, err)

			err = b.Restore(t.Context(), tt.data)

			if tt.wantError {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, 0, FileSystemCount(b))
			assert.Equal(t, 0, ARNIndexSize(b))
		})
	}
}

// Test_InMemoryBackend_SnapshotRestore_EmptyBackend verifies Snapshot/Restore
// round trips cleanly on a backend with no data at all -- every registered
// table and every raw-left map must both decode back to empty, not
// nil-panic or error.
func Test_InMemoryBackend_SnapshotRestore_EmptyBackend(t *testing.T) {
	t.Parallel()

	original := NewInMemoryBackend("000000000000", "us-east-1")
	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	assert.Equal(t, 0, FileSystemCount(fresh))
	assert.Equal(t, 0, MountTargetCount(fresh))
	assert.Equal(t, 0, AccessPointCount(fresh))
	assert.Equal(t, 0, ReplicationConfigCount(fresh))
	assert.Equal(t, 0, BackupPolicyCount(fresh))
	assert.Equal(t, 0, FileSystemPolicyCount(fresh))
	assert.Equal(t, 0, ARNIndexSize(fresh))

	list, _, err := fresh.DescribeFileSystems(ctxRegion("us-east-1"), "", "", "", 0)
	require.NoError(t, err)
	assert.Empty(t, list)
}

// efsFullStateFixture is one region's worth of seeded EFS state, built by
// seedEFSFullState so Test_InMemoryBackend_SnapshotRestore_FullState can
// verify every field it cares about survives a round trip.
type efsFullStateFixture struct {
	fs        *FileSystem
	mt        *MountTarget
	ap        *AccessPoint
	subnetID  string
	clientTok string
}

// seedEFSFullState creates one file system (with a mount target, an access
// point with a ClientToken, a lifecycle policy, a backup policy, a resource
// policy, and a replication configuration) in region, exercising every
// store.Table and every raw-left persisted map this backend owns.
func seedEFSFullState(t *testing.T, b *InMemoryBackend, region string) efsFullStateFixture {
	t.Helper()

	ctx := ctxRegion(region)

	fs, err := b.CreateFileSystem(ctx, CreateFileSystemRequest{
		CreationToken:   "token-" + region,
		Tags:            map[string]string{"Name": "fs-" + region},
		ThroughputMode:  throughputModeBursting,
		PerformanceMode: performanceModeGeneral,
	})
	require.NoError(t, err)

	subnetID := "subnet-" + region
	mt, err := b.CreateMountTarget(ctx, CreateMountTargetRequest{
		FileSystemID:   fs.FileSystemID,
		SubnetID:       subnetID,
		SecurityGroups: []string{"sg-1"},
	})
	require.NoError(t, err)

	clientTok := "client-tok-" + region
	ap, err := b.CreateAccessPoint(ctx, CreateAccessPointRequest{
		FileSystemID: fs.FileSystemID,
		ClientToken:  clientTok,
		Tags:         map[string]string{"env": region},
	})
	require.NoError(t, err)

	_, err = b.PutLifecycleConfiguration(ctx, fs.FileSystemID, []LifecyclePolicy{
		{TransitionToIA: testTransitionToIA},
	})
	require.NoError(t, err)

	require.NoError(t, b.PutBackupPolicy(ctx, fs.FileSystemID, backupStatusEnabled))
	require.NoError(t, b.PutFileSystemPolicy(ctx, fs.FileSystemID, `{"Version":"2012-10-17","Statement":[]}`))

	_, err = b.CreateReplicationConfiguration(ctx, fs.FileSystemID, []ReplicationDestination{
		{Region: region},
	})
	require.NoError(t, err)

	return efsFullStateFixture{fs: fs, mt: mt, ap: ap, subnetID: subnetID, clientTok: clientTok}
}

// Test_InMemoryBackend_SnapshotRestore_FullState is the Phase 3.3 full-state
// round-trip test: it seeds every converted store.Table (fileSystems,
// mountTargets, accessPoints, replicationConfigs) and every raw-left
// persisted map (lifecyclePolicies, backupPolicies, fileSystemPolicies) in
// TWO regions, to prove the composite "region|id" key keeps same-shaped
// resources in different regions fully isolated across a round trip, and
// that every derived-cache table (fileSystemsByARN, mountTargetsByARN,
// accessPointsByARN, accessPointsByClientToken) and raw performance index
// (creationTokenIdx, mtSubnetIdx, apByFS) is correctly rebuilt afterward.
func Test_InMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := NewInMemoryBackend("111122223333", "us-east-1")

	east := seedEFSFullState(t, original, "us-east-1")
	west := seedEFSFullState(t, original, "us-west-2")

	snap := original.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	fresh := NewInMemoryBackend("111122223333", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	assert.Equal(t, 2, FileSystemCount(fresh))
	assert.Equal(t, 2, MountTargetCount(fresh))
	assert.Equal(t, 2, AccessPointCount(fresh))
	assert.Equal(t, 2, ReplicationConfigCount(fresh))
	assert.Equal(t, 2, BackupPolicyCount(fresh))
	assert.Equal(t, 2, FileSystemPolicyCount(fresh))
	assert.Equal(t, 6, ARNIndexSize(fresh)) // 2 file systems + 2 mount targets + 2 access points

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	// fileSystems: composite key + byRegion index kept the two file systems
	// (and their attached lifecycle/backup/resource policies) distinct.
	gotEastFS, err := fresh.DescribeFileSystemPolicy(ctxEast, east.fs.FileSystemID)
	require.NoError(t, err)
	assert.JSONEq(t, `{"Version":"2012-10-17","Statement":[]}`, gotEastFS)

	eastBackup, err := fresh.DescribeBackupPolicy(ctxEast, east.fs.FileSystemID)
	require.NoError(t, err)
	assert.Equal(t, backupStatusEnabled, eastBackup)

	eastLifecycle, err := fresh.DescribeLifecycleConfiguration(ctxEast, east.fs.FileSystemID)
	require.NoError(t, err)
	require.Len(t, eastLifecycle, 1)
	assert.Equal(t, testTransitionToIA, eastLifecycle[0].TransitionToIA)

	// The west file system is invisible from the east region context, and
	// vice versa -- the composite key round-tripped, not a flattened one.
	_, _, err = fresh.DescribeFileSystems(ctxEast, west.fs.FileSystemID, "", "", 0)
	require.Error(t, err)
	_, _, err = fresh.DescribeFileSystems(ctxWest, east.fs.FileSystemID, "", "", 0)
	require.Error(t, err)

	// fileSystemsByARN derived cache + creationTokenIdx: idempotent recreate
	// with the same (region-scoped) creation token returns the same file
	// system rather than erroring as a fresh AlreadyExists conflict.
	again, err := fresh.CreateFileSystem(ctxEast, CreateFileSystemRequest{CreationToken: "token-us-east-1"})
	require.ErrorIs(t, err, ErrCreationTokenExists)
	assert.Equal(t, east.fs.FileSystemID, again.FileSystemID)

	eastTags, err := fresh.ListTagsForResource(ctxEast, east.fs.FileSystemArn)
	require.NoError(t, err)
	assert.Equal(t, "fs-us-east-1", eastTags["Name"])

	// mountTargets: hidden `region` field survived the regionalDTO round
	// trip -- mtSubnetIdx conflict-check still fires for the same subnet in
	// the same region, and the mount target is still visible via the
	// byRegion index.
	_, err = fresh.CreateMountTarget(ctxEast, CreateMountTargetRequest{
		FileSystemID: east.fs.FileSystemID,
		SubnetID:     east.subnetID,
	})
	require.ErrorIs(t, err, ErrMountTargetConflict)

	eastMTs, _, err := fresh.DescribeMountTargets(ctxEast, "", "", "", 0)
	require.NoError(t, err)
	require.Len(t, eastMTs, 1)
	assert.Equal(t, east.mt.MountTargetID, eastMTs[0].MountTargetID)
	assert.Equal(t, []string{"sg-1"}, eastMTs[0].SecurityGroups)

	// mountTargetsByARN derived cache is region-scoped: the east mount
	// target's ARN must not resolve from the west region.
	err = fresh.ModifyMountTargetSecurityGroups(ctxWest, east.mt.MountTargetID, []string{"sg-2"})
	require.ErrorIs(t, err, ErrMountTargetNotFound)

	// DeleteFileSystem must still be blocked by the rebuilt mtSubnetIdx
	// (existing mount target), proving the raw performance index survived.
	err = fresh.DeleteFileSystem(ctxEast, east.fs.FileSystemID)
	require.ErrorIs(t, err, ErrFileSystemInUse)

	// accessPoints: hidden `region` field, ClientToken idempotency
	// (accessPointsByClientToken), and apByFS (via the FileSystemInUse
	// check) all survived the round trip.
	sameAP, err := fresh.CreateAccessPoint(ctxEast, CreateAccessPointRequest{
		FileSystemID: east.fs.FileSystemID,
		ClientToken:  east.clientTok,
	})
	require.NoError(t, err)
	assert.Equal(t, east.ap.AccessPointID, sameAP.AccessPointID)

	apTags, err := fresh.ListTagsForResource(ctxEast, east.ap.AccessPointArn)
	require.NoError(t, err)
	assert.Equal(t, "us-east-1", apTags["env"])

	// replicationConfigs: SourceFileSystemRegion round-tripped without a
	// hidden-field DTO, and stayed region-scoped.
	eastRCs, err := fresh.DescribeReplicationConfigurations(ctxEast, "")
	require.NoError(t, err)
	require.Len(t, eastRCs, 1)
	assert.Equal(t, east.fs.FileSystemID, eastRCs[0].SourceFileSystemID)

	westRCs, err := fresh.DescribeReplicationConfigurations(ctxWest, "")
	require.NoError(t, err)
	require.Len(t, westRCs, 1)
	assert.Equal(t, west.fs.FileSystemID, westRCs[0].SourceFileSystemID)

	// Now remove the blocking mount target/access point and confirm delete
	// finally succeeds, proving cleanup (not just conflict detection) still
	// works against the rebuilt indexes.
	require.NoError(t, fresh.DeleteAccessPoint(ctxEast, east.ap.AccessPointID))
	require.NoError(t, fresh.DeleteMountTarget(ctxEast, east.mt.MountTargetID))
	require.NoError(t, fresh.DeleteReplicationConfiguration(ctxEast, east.fs.FileSystemID))
	require.NoError(t, fresh.DeleteFileSystem(ctxEast, east.fs.FileSystemID))

	eastAfter, _, err := fresh.DescribeFileSystems(ctxEast, "", "", "", 0)
	require.NoError(t, err)
	assert.Empty(t, eastAfter)

	// The west region's resources are untouched by the east deletions.
	westAfter, _, err := fresh.DescribeFileSystems(ctxWest, "", "", "", 0)
	require.NoError(t, err)
	assert.Len(t, westAfter, 1)
}

// Test_Handler_SnapshotRestoreDelegate pins the Handler-level persistence
// wiring: Handler.Snapshot/Restore must delegate to the backend so the
// cli.go persistence manager (which only knows about persistence.Persistable,
// not InMemoryBackend) can drive EFS snapshots.
func Test_Handler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")
	h := NewHandler(backend)

	_, err := backend.CreateFileSystem(ctxRegion("us-east-1"), CreateFileSystemRequest{CreationToken: "delegate"})
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	backend2 := NewInMemoryBackend("000000000000", "us-east-1")
	h2 := NewHandler(backend2)
	require.NoError(t, h2.Restore(t.Context(), snap))

	assert.Equal(t, 1, FileSystemCount(backend2))
}
