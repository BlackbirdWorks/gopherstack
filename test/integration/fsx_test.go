package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	fsxsdk "github.com/aws/aws-sdk-go-v2/service/fsx"
	fsxtypes "github.com/aws/aws-sdk-go-v2/service/fsx/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createFSxClient returns an FSx client pointed at the shared test container.
func createFSxClient(t *testing.T) *fsxsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return fsxsdk.NewFromConfig(cfg, func(o *fsxsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_FSx_FileSystemLifecycle drives create→describe→delete of a Lustre file system.
func TestIntegration_FSx_FileSystemLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name           string
		fileSystemType fsxtypes.FileSystemType
		capacity       int32
	}{
		{name: "lustre", fileSystemType: fsxtypes.FileSystemTypeLustre, capacity: 1200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createFSxClient(t)

			createOut, err := client.CreateFileSystem(ctx, &fsxsdk.CreateFileSystemInput{
				FileSystemType:  tt.fileSystemType,
				StorageCapacity: aws.Int32(tt.capacity),
				SubnetIds:       []string{"subnet-12345678"},
			})
			require.NoError(t, err, "CreateFileSystem should succeed")
			require.NotNil(t, createOut.FileSystem)
			fsID := aws.ToString(createOut.FileSystem.FileSystemId)
			require.NotEmpty(t, fsID, "file system id must be returned")
			assert.Equal(t, tt.fileSystemType, createOut.FileSystem.FileSystemType)

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteFileSystem(
					cleanupCtx,
					&fsxsdk.DeleteFileSystemInput{FileSystemId: aws.String(fsID)},
				)
			})

			descOut, err := client.DescribeFileSystems(ctx, &fsxsdk.DescribeFileSystemsInput{
				FileSystemIds: []string{fsID},
			})
			require.NoError(t, err, "DescribeFileSystems should succeed")
			require.Len(t, descOut.FileSystems, 1)
			assert.Equal(t, fsID, aws.ToString(descOut.FileSystems[0].FileSystemId))
			assert.Equal(t, tt.capacity, aws.ToInt32(descOut.FileSystems[0].StorageCapacity))

			_, err = client.DeleteFileSystem(ctx, &fsxsdk.DeleteFileSystemInput{FileSystemId: aws.String(fsID)})
			require.NoError(t, err, "DeleteFileSystem should succeed")
		})
	}
}

// TestIntegration_FSx_BackupLifecycle drives file-system→backup create→describe→delete.
func TestIntegration_FSx_BackupLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name string
	}{
		{name: "full_lifecycle"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createFSxClient(t)

			fsOut, err := client.CreateFileSystem(ctx, &fsxsdk.CreateFileSystemInput{
				FileSystemType:  fsxtypes.FileSystemTypeLustre,
				StorageCapacity: aws.Int32(1200),
				SubnetIds:       []string{"subnet-12345678"},
			})
			require.NoError(t, err, "CreateFileSystem should succeed")
			fsID := aws.ToString(fsOut.FileSystem.FileSystemId)

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteFileSystem(
					cleanupCtx,
					&fsxsdk.DeleteFileSystemInput{FileSystemId: aws.String(fsID)},
				)
			})

			backupOut, err := client.CreateBackup(ctx, &fsxsdk.CreateBackupInput{
				FileSystemId: aws.String(fsID),
			})
			require.NoError(t, err, "CreateBackup should succeed")
			require.NotNil(t, backupOut.Backup)
			backupID := aws.ToString(backupOut.Backup.BackupId)
			require.NotEmpty(t, backupID, "backup id must be returned")

			descOut, err := client.DescribeBackups(ctx, &fsxsdk.DescribeBackupsInput{
				BackupIds: []string{backupID},
			})
			require.NoError(t, err, "DescribeBackups should succeed")
			require.Len(t, descOut.Backups, 1)
			assert.Equal(t, backupID, aws.ToString(descOut.Backups[0].BackupId))

			_, err = client.DeleteBackup(ctx, &fsxsdk.DeleteBackupInput{BackupId: aws.String(backupID)})
			require.NoError(t, err, "DeleteBackup should succeed")
		})
	}
}
