package fsx_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	fsxsdk "github.com/aws/aws-sdk-go-v2/service/fsx"
	"github.com/aws/aws-sdk-go-v2/service/fsx/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeleteFileSystem_FinalBackup(t *testing.T) {
	t.Parallel()

	tag := []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}}

	windows := func(t *testing.T, c *fsxsdk.Client) string {
		t.Helper()

		out, err := c.CreateFileSystem(t.Context(), &fsxsdk.CreateFileSystemInput{
			FileSystemType:       types.FileSystemTypeWindows,
			SubnetIds:            []string{"subnet-0123abcd"},
			StorageCapacity:      aws.Int32(32),
			WindowsConfiguration: &types.CreateFileSystemWindowsConfiguration{ThroughputCapacity: aws.Int32(8)},
		})
		require.NoError(t, err)

		return aws.ToString(out.FileSystem.FileSystemId)
	}
	lustre := func(t *testing.T, c *fsxsdk.Client) string {
		t.Helper()

		return aws.ToString(createTestLustreFS(t, c).FileSystem.FileSystemId)
	}

	tests := []struct {
		create     func(t *testing.T, c *fsxsdk.Client) string
		in         func(id string) *fsxsdk.DeleteFileSystemInput
		name       string
		wantTags   int
		wantBackup bool
	}{
		{name: "lustre_default_skips", create: lustre, in: func(id string) *fsxsdk.DeleteFileSystemInput {
			return &fsxsdk.DeleteFileSystemInput{FileSystemId: aws.String(id)}
		}},
		{
			name: "lustre_skip_false", create: lustre, wantBackup: true, wantTags: 1,
			in: func(id string) *fsxsdk.DeleteFileSystemInput {
				return &fsxsdk.DeleteFileSystemInput{
					FileSystemId: aws.String(id),
					LustreConfiguration: &types.DeleteFileSystemLustreConfiguration{
						SkipFinalBackup: aws.Bool(false), FinalBackupTags: tag,
					},
				}
			},
		},
		{
			name: "windows_default_takes", create: windows, wantBackup: true,
			in: func(id string) *fsxsdk.DeleteFileSystemInput {
				return &fsxsdk.DeleteFileSystemInput{FileSystemId: aws.String(id)}
			},
		},
		{
			name: "windows_skip_true", create: windows,
			in: func(id string) *fsxsdk.DeleteFileSystemInput {
				return &fsxsdk.DeleteFileSystemInput{
					FileSystemId:         aws.String(id),
					WindowsConfiguration: &types.DeleteFileSystemWindowsConfiguration{SkipFinalBackup: aws.Bool(true)},
				}
			},
		},
		{
			name: "windows_tags", create: windows, wantBackup: true, wantTags: 1,
			in: func(id string) *fsxsdk.DeleteFileSystemInput {
				return &fsxsdk.DeleteFileSystemInput{
					FileSystemId:         aws.String(id),
					WindowsConfiguration: &types.DeleteFileSystemWindowsConfiguration{FinalBackupTags: tag},
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestFSxClient(t, newTestHandler(t))
			id := tc.create(t, client)

			out, err := client.DeleteFileSystem(t.Context(), tc.in(id))
			require.NoError(t, err)

			var backupID *string
			var gotTags []types.Tag

			switch {
			case out.WindowsResponse != nil:
				backupID, gotTags = out.WindowsResponse.FinalBackupId, out.WindowsResponse.FinalBackupTags
			case out.LustreResponse != nil:
				backupID, gotTags = out.LustreResponse.FinalBackupId, out.LustreResponse.FinalBackupTags
			}

			bks, err := client.DescribeBackups(t.Context(), &fsxsdk.DescribeBackupsInput{})
			require.NoError(t, err)

			if !tc.wantBackup {
				assert.Nil(t, backupID)
				assert.Empty(t, bks.Backups)

				return
			}

			require.NotNil(t, backupID)
			assert.Len(t, gotTags, tc.wantTags)
			require.Len(t, bks.Backups, 1)
			assert.Equal(t, aws.ToString(backupID), aws.ToString(bks.Backups[0].BackupId))
			assert.Equal(t, id, aws.ToString(bks.Backups[0].FileSystem.FileSystemId))
			assert.Len(t, bks.Backups[0].Tags, tc.wantTags)
		})
	}
}
