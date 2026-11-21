package app

import (
	"log/slog"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/fsx"
	fsxtypes "github.com/aws/aws-sdk-go-v2/service/fsx/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	backupbk "github.com/blackbirdworks/gopherstack/services/backup"
	docdbbackend "github.com/blackbirdworks/gopherstack/services/docdb"
	fsxbackend "github.com/blackbirdworks/gopherstack/services/fsx"
	neptunebackend "github.com/blackbirdworks/gopherstack/services/neptune"
	rdsbackend "github.com/blackbirdworks/gopherstack/services/rds"
	redshiftbackend "github.com/blackbirdworks/gopherstack/services/redshift"
)

func TestBackupWiring_RestoreRecreatesClusters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, backup *backupbk.InMemoryBackend, byName map[string]service.Registerable)
		name string
	}{
		{name: "aurora", run: backupAuroraRestore},
		{name: "documentdb", run: backupDocDBRestore},
		{name: "neptune", run: backupNeptuneRestore},
		{name: "redshift", run: backupRedshiftRestore},
		{name: "fsx", run: backupFSxRestore},
		{name: "storage_gateway", run: backupStorageGatewayRestore},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cli := &CLI{AccountID: backupWiringAccount, Region: "us-east-1"}
			appCtx := &service.AppContext{Logger: slog.Default(), Config: cli, JanitorCtx: t.Context()}
			cli.faultStore = chaos.NewFaultStore()

			services, err := initializeServicesT(t, appCtx)
			require.NoError(t, err)

			byName := serviceByName(services)
			backupH, ok := byName["Backup"].(*backupbk.Handler)
			require.True(t, ok)

			tt.run(t, backupH.Backend, byName)
		})
	}
}

func restoreFromPoint(
	t *testing.T, backup *backupbk.InMemoryBackend, rp, resourceType string, md map[string]string,
) *backupbk.RestoreJob {
	t.Helper()

	job, err := backup.StartRestoreJob(rp, backupWiringRole, resourceType, md)
	require.NoError(t, err)
	require.Equal(t, "COMPLETED", job.Status, job.StatusMessage)

	return job
}

func recoveryPointType(t *testing.T, backup *backupbk.InMemoryBackend, vault, rpArn string) string {
	t.Helper()

	rp, err := backup.DescribeRecoveryPoint(vault, rpArn)
	require.NoError(t, err)

	return rp.ResourceType
}

func backupAuroraRestore(t *testing.T, backup *backupbk.InMemoryBackend, byName map[string]service.Registerable) {
	t.Helper()

	h, ok := byName["RDS"].(*rdsbackend.Handler)
	require.True(t, ok)

	src, err := h.Backend.CreateDBCluster("orders", "aurora-postgresql", "admin", "app", "", 0, nil,
		rdsbackend.DBClusterOptions{MasterUserPassword: "password123"})
	require.NoError(t, err)

	rp := backupRecoveryPointFor(t, backup, src.DBClusterArn, "Aurora")
	assert.Equal(t, "Aurora", recoveryPointType(t, backup, "v-Aurora", rp))

	_, err = h.Backend.DeleteDBCluster("orders")
	require.NoError(t, err)

	job := restoreFromPoint(t, backup, rp, "Aurora", map[string]string{"DBClusterIdentifier": "orders-copy"})

	got, err := h.Backend.DescribeDBClusters("orders-copy")
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, job.CreatedResourceArn, got[0].DBClusterArn)
	assert.Equal(t, "aurora-postgresql", got[0].Engine)
	assert.Equal(t, "admin", got[0].MasterUsername)
	assert.Equal(t, src.EngineVersion, got[0].EngineVersion)

	require.NoError(t, backup.DeleteRecoveryPoint("v-Aurora", rp))

	snaps, err := h.Backend.DescribeDBClusterSnapshots("", "")
	require.NoError(t, err)
	assert.Empty(t, snaps)
}

func backupDocDBRestore(t *testing.T, backup *backupbk.InMemoryBackend, byName map[string]service.Registerable) {
	t.Helper()

	h, ok := byName["DocDB"].(*docdbbackend.Handler)
	require.True(t, ok)

	src, err := h.Backend.CreateDBCluster(t.Context(), "docs", "docdb", "", "admin", "password123",
		"", "", "", 0, false, false, 0, "", "", nil, nil, nil)
	require.NoError(t, err)

	rp := backupRecoveryPointFor(t, backup, src.DBClusterArn, "DocumentDB")
	assert.Equal(t, "DocumentDB", recoveryPointType(t, backup, "v-DocumentDB", rp))

	_, err = h.Backend.DeleteDBCluster(t.Context(), "docs", &docdbbackend.DeleteDBClusterOptions{SkipFinalSnapshot: true})
	require.NoError(t, err)

	job := restoreFromPoint(t, backup, rp, "DocumentDB", map[string]string{"DBClusterIdentifier": "docs-copy"})

	got, err := h.Backend.DescribeDBClusters(t.Context(), "docs-copy")
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, job.CreatedResourceArn, got[0].DBClusterArn)
	assert.Equal(t, "admin", got[0].MasterUsername)
	assert.Equal(t, src.EngineVersion, got[0].EngineVersion)
}

func backupNeptuneRestore(t *testing.T, backup *backupbk.InMemoryBackend, byName map[string]service.Registerable) {
	t.Helper()

	h, ok := byName["Neptune"].(*neptunebackend.Handler)
	require.True(t, ok)

	bk, ok := h.Backend.(*neptunebackend.InMemoryBackend)
	require.True(t, ok)

	src, err := bk.CreateDBCluster(t.Context(), "graph", "", 0, neptunebackend.DBClusterCreateOptions{})
	require.NoError(t, err)

	rp := backupRecoveryPointFor(t, backup, src.DBClusterArn, "Neptune")
	assert.Equal(t, "Neptune", recoveryPointType(t, backup, "v-Neptune", rp))

	_, err = bk.DeleteDBCluster(t.Context(), "graph", neptunebackend.DBClusterDeleteOptions{SkipFinalSnapshot: true})
	require.NoError(t, err)

	job := restoreFromPoint(t, backup, rp, "Neptune", map[string]string{"DBClusterIdentifier": "graph-copy"})

	got, err := bk.DescribeDBClusters(t.Context(), "graph-copy", neptunebackend.DBClusterFilters{})
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, job.CreatedResourceArn, got[0].DBClusterArn)
	assert.Equal(t, src.Engine, got[0].Engine)
	assert.Equal(t, src.EngineVersion, got[0].EngineVersion)
}

func backupRedshiftRestore(t *testing.T, backup *backupbk.InMemoryBackend, byName map[string]service.Registerable) {
	t.Helper()

	h, ok := byName["Redshift"].(*redshiftbackend.Handler)
	require.True(t, ok)

	bk, ok := h.Backend.(*redshiftbackend.InMemoryBackend)
	require.True(t, ok)

	_, err := bk.CreateCluster("wh", "dc2.large", "analytics", "admin", nil, "",
		redshiftbackend.CreateClusterOptions{MasterUserPassword: "Password123"})
	require.NoError(t, err)

	srcArn := "arn:aws:redshift:us-east-1:000000000000:cluster:wh"
	rp := backupRecoveryPointFor(t, backup, srcArn, "Redshift")

	_, err = bk.DeleteCluster("wh")
	require.NoError(t, err)

	job := restoreFromPoint(t, backup, rp, "Redshift", map[string]string{"ClusterIdentifier": "wh-copy"})
	assert.Equal(t, "arn:aws:redshift:us-east-1:000000000000:cluster:wh-copy", job.CreatedResourceArn)

	got, _, err := bk.DescribeClusters("wh-copy", "", 0, nil, nil)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "analytics", got[0].DBName)
	assert.Equal(t, "admin", got[0].MasterUsername)
}

func backupFSxRestore(t *testing.T, backup *backupbk.InMemoryBackend, byName map[string]service.Registerable) {
	t.Helper()

	h, ok := byName["FSx"].(*fsxbackend.Handler)
	require.True(t, ok)

	bk, ok := h.Backend.(*fsxbackend.InMemoryBackend)
	require.True(t, ok)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awscfg.WithHTTPClient(sdktest.ServeInMemory(t, h)))
	require.NoError(t, err)

	client := fsx.NewFromConfig(cfg, func(o *fsx.Options) { o.BaseEndpoint = aws.String("http://fsx.local") })

	created, err := client.CreateFileSystem(t.Context(), &fsx.CreateFileSystemInput{
		FileSystemType:  fsxtypes.FileSystemTypeLustre,
		StorageCapacity: aws.Int32(2400),
		SubnetIds:       []string{"subnet-0123456789abcdef0"},
	})
	require.NoError(t, err)

	srcID := aws.ToString(created.FileSystem.FileSystemId)
	rp := backupRecoveryPointFor(t, backup, aws.ToString(created.FileSystem.ResourceARN), "FSx")

	_, err = client.DeleteFileSystem(t.Context(), &fsx.DeleteFileSystemInput{FileSystemId: aws.String(srcID)})
	require.NoError(t, err)

	job := restoreFromPoint(t, backup, rp, "FSx", map[string]string{"SubnetIds": "subnet-0123456789abcdef0"})

	list, _, err := bk.DescribeFileSystems(nil, 0, "")
	require.NoError(t, err)

	var restored *fsxbackend.FileSystem
	for _, fs := range list {
		if fs.ResourceARN == job.CreatedResourceArn {
			restored = fs
		}
	}

	require.NotNil(t, restored)
	assert.NotEqual(t, srcID, restored.FileSystemID)
	assert.Equal(t, "LUSTRE", restored.FileSystemType)
	assert.Equal(t, int32(2400), restored.StorageCapacityGiB)
	assert.Equal(t, []string{"subnet-0123456789abcdef0"}, restored.SubnetIDs)

	require.NoError(t, backup.DeleteRecoveryPoint("v-FSx", rp))

	backups, _, err := bk.DescribeBackups(nil, nil, 0, "")
	require.NoError(t, err)
	assert.Empty(t, backups)
}

func backupStorageGatewayRestore(
	t *testing.T, backup *backupbk.InMemoryBackend, _ map[string]service.Registerable,
) {
	t.Helper()

	src := "arn:aws:storagegateway:us-east-1:000000000000:gateway/sgw-12345678/volume/vol-AAAA1111"
	rp := backupRecoveryPointFor(t, backup, src, "Storage Gateway")

	job := restoreFromPoint(t, backup, rp, "Storage Gateway",
		map[string]string{"GatewayArn": "arn:aws:storagegateway:us-east-1:000000000000:gateway/sgw-12345678"})
	assert.Regexp(t,
		`^arn:aws:storagegateway:us-east-1:000000000000:gateway/sgw-12345678/volume/vol-[0-9a-f]{8}$`,
		job.CreatedResourceArn)
	assert.NotEqual(t, src, job.CreatedResourceArn)
}
