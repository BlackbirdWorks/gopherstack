package fsx_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	fsxsdk "github.com/aws/aws-sdk-go-v2/service/fsx"
	"github.com/aws/aws-sdk-go-v2/service/fsx/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKmsKeyID_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	const key = "arn:aws:kms:us-east-1:000000000000:key/1234abcd-12ab-34cd-56ef-1234567890ab"

	tests := []struct {
		run  func(t *testing.T, client *fsxsdk.Client) *string
		name string
	}{
		{
			name: "create_file_system_and_backup",
			run: func(t *testing.T, client *fsxsdk.Client) *string {
				t.Helper()

				fs, err := client.CreateFileSystem(t.Context(), &fsxsdk.CreateFileSystemInput{
					FileSystemType:  types.FileSystemTypeLustre,
					SubnetIds:       []string{"subnet-0123abcd"},
					StorageCapacity: aws.Int32(1200),
					KmsKeyId:        aws.String(key),
					LustreConfiguration: &types.CreateFileSystemLustreConfiguration{
						DeploymentType:           types.LustreDeploymentTypePersistent1,
						PerUnitStorageThroughput: aws.Int32(50),
					},
				})
				require.NoError(t, err)
				assert.Equal(t, key, aws.ToString(fs.FileSystem.KmsKeyId))

				bk, err := client.CreateBackup(
					t.Context(),
					&fsxsdk.CreateBackupInput{FileSystemId: fs.FileSystem.FileSystemId},
				)
				require.NoError(t, err)

				return bk.Backup.KmsKeyId
			},
		},
		{
			name: "copy_backup_override",
			run: func(t *testing.T, client *fsxsdk.Client) *string {
				t.Helper()

				fs := createTestLustreFS(t, client)
				bk, err := client.CreateBackup(
					t.Context(),
					&fsxsdk.CreateBackupInput{FileSystemId: fs.FileSystem.FileSystemId},
				)
				require.NoError(t, err)

				cp, err := client.CopyBackup(t.Context(), &fsxsdk.CopyBackupInput{
					SourceBackupId: bk.Backup.BackupId,
					KmsKeyId:       aws.String(key),
				})
				require.NoError(t, err)

				return cp.Backup.KmsKeyId
			},
		},
		{
			name: "from_backup",
			run: func(t *testing.T, client *fsxsdk.Client) *string {
				t.Helper()

				fs := createTestLustreFS(t, client)
				bk, err := client.CreateBackup(
					t.Context(),
					&fsxsdk.CreateBackupInput{FileSystemId: fs.FileSystem.FileSystemId},
				)
				require.NoError(t, err)

				out, err := client.CreateFileSystemFromBackup(t.Context(), &fsxsdk.CreateFileSystemFromBackupInput{
					BackupId:  bk.Backup.BackupId,
					SubnetIds: []string{"subnet-0123abcd"},
					KmsKeyId:  aws.String(key),
				})
				require.NoError(t, err)

				return out.FileSystem.KmsKeyId
			},
		},
		{
			name: "file_cache",
			run: func(t *testing.T, client *fsxsdk.Client) *string {
				t.Helper()

				out, err := client.CreateFileCache(t.Context(), &fsxsdk.CreateFileCacheInput{
					FileCacheType:        types.FileCacheTypeLustre,
					FileCacheTypeVersion: aws.String("2.12"),
					StorageCapacity:      aws.Int32(1200),
					SubnetIds:            []string{"subnet-0123abcd"},
					KmsKeyId:             aws.String(key),
					LustreConfiguration:  testFileCacheLustreConfig(),
				})
				require.NoError(t, err)

				desc, err := client.DescribeFileCaches(t.Context(), &fsxsdk.DescribeFileCachesInput{
					FileCacheIds: []string{aws.ToString(out.FileCache.FileCacheId)},
				})
				require.NoError(t, err)
				require.Len(t, desc.FileCaches, 1)

				return desc.FileCaches[0].KmsKeyId
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.run(t, newTestFSxClient(t, newTestHandler(t)))
			assert.Equal(t, key, aws.ToString(got))
		})
	}
}

func TestCopyBackup_SDKCopyTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantKeys []string
		copyTags bool
	}{
		{name: "copy_tags_merges_source", copyTags: true, wantKeys: []string{"src", "new"}},
		{name: "default_only_new", copyTags: false, wantKeys: []string{"new"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestFSxClient(t, newTestHandler(t))
			fs := createTestLustreFS(t, client)

			bk, err := client.CreateBackup(t.Context(), &fsxsdk.CreateBackupInput{
				FileSystemId: fs.FileSystem.FileSystemId,
				Tags:         []types.Tag{{Key: aws.String("src"), Value: aws.String("1")}},
			})
			require.NoError(t, err)

			cp, err := client.CopyBackup(t.Context(), &fsxsdk.CopyBackupInput{
				SourceBackupId: bk.Backup.BackupId,
				CopyTags:       aws.Bool(tt.copyTags),
				Tags:           []types.Tag{{Key: aws.String("new"), Value: aws.String("2")}},
			})
			require.NoError(t, err)

			var keys []string
			for _, tg := range cp.Backup.Tags {
				keys = append(keys, aws.ToString(tg.Key))
			}

			assert.ElementsMatch(t, tt.wantKeys, keys)
		})
	}
}

func TestCreateDataRepositoryTask_SDKReleaseMembers(t *testing.T) {
	t.Parallel()

	client := newTestFSxClient(t, newTestHandler(t))
	fs := createTestLustreFS(t, client)

	created, err := client.CreateDataRepositoryTask(t.Context(), &fsxsdk.CreateDataRepositoryTaskInput{
		FileSystemId:      fs.FileSystem.FileSystemId,
		Type:              types.DataRepositoryTaskTypeEviction,
		Report:            &types.CompletionReport{Enabled: aws.Bool(false)},
		CapacityToRelease: aws.Int64(100),
		ReleaseConfiguration: &types.ReleaseConfiguration{
			DurationSinceLastAccess: &types.DurationSinceLastAccess{Unit: types.UnitDays, Value: aws.Int64(7)},
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeDataRepositoryTasks(t.Context(), &fsxsdk.DescribeDataRepositoryTasksInput{
		TaskIds: []string{aws.ToString(created.DataRepositoryTask.TaskId)},
	})
	require.NoError(t, err)
	require.Len(t, desc.DataRepositoryTasks, 1)

	got := desc.DataRepositoryTasks[0]
	assert.EqualValues(t, 100, aws.ToInt64(got.CapacityToRelease))
	require.NotNil(t, got.ReleaseConfiguration)
	assert.Equal(t, types.UnitDays, got.ReleaseConfiguration.DurationSinceLastAccess.Unit)
	assert.EqualValues(t, 7, aws.ToInt64(got.ReleaseConfiguration.DurationSinceLastAccess.Value))
}
