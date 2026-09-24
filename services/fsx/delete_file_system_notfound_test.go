package fsx_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	fsxsdk "github.com/aws/aws-sdk-go-v2/service/fsx"
	"github.com/aws/aws-sdk-go-v2/service/fsx/types"
	"github.com/stretchr/testify/require"
)

// TestDeleteFileSystem_ThenDescribeIsTypedNotFound proves DeleteFileSystem's
// hard delete is immediately visible to DescribeFileSystems as the real
// typed types.FileSystemNotFound error, for every file system type. This is
// exactly what terraform-provider-aws's shared waitFileSystemDeleted/
// findFileSystems (internal/service/fsx/lustre_file_system.go, used by all
// of aws_fsx_lustre_file_system, _windows_file_system, _ontap_file_system,
// and _openzfs_file_system) requires from its first post-delete poll to
// treat the resource as gone (gopherstack-jtf4s: the emulator side was
// already correct -- the observed multi-minute destroy hang is the
// provider's own unconditional 10-minute pre-poll Delay in that waiter, not
// a slow or wrong response here).
func TestDeleteFileSystem_ThenDescribeIsTypedNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(t *testing.T, client *fsxsdk.Client) string
		name   string
	}{
		{name: "lustre", create: func(t *testing.T, client *fsxsdk.Client) string {
			t.Helper()

			return aws.ToString(createTestLustreFS(t, client).FileSystem.FileSystemId)
		}},
		{name: "windows", create: func(t *testing.T, client *fsxsdk.Client) string {
			t.Helper()

			out, err := client.CreateFileSystem(t.Context(), &fsxsdk.CreateFileSystemInput{
				FileSystemType:  types.FileSystemTypeWindows,
				SubnetIds:       []string{"subnet-0123abcd"},
				StorageCapacity: aws.Int32(32),
				WindowsConfiguration: &types.CreateFileSystemWindowsConfiguration{
					ThroughputCapacity: aws.Int32(8),
				},
			})
			require.NoError(t, err)

			return aws.ToString(out.FileSystem.FileSystemId)
		}},
		{name: "ontap", create: func(t *testing.T, client *fsxsdk.Client) string {
			t.Helper()

			return aws.ToString(createTestOntapFS(t, client).FileSystem.FileSystemId)
		}},
		{name: "openzfs", create: func(t *testing.T, client *fsxsdk.Client) string {
			t.Helper()

			out, err := client.CreateFileSystem(t.Context(), &fsxsdk.CreateFileSystemInput{
				FileSystemType:  types.FileSystemTypeOpenzfs,
				SubnetIds:       []string{"subnet-0123abcd"},
				StorageCapacity: aws.Int32(64),
				OpenZFSConfiguration: &types.CreateFileSystemOpenZFSConfiguration{
					DeploymentType:     types.OpenZFSDeploymentTypeSingleAz1,
					ThroughputCapacity: aws.Int32(64),
				},
			})
			require.NoError(t, err)

			return aws.ToString(out.FileSystem.FileSystemId)
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			client := newTestFSxClient(t, h)

			id := tc.create(t, client)

			_, err := client.DeleteFileSystem(t.Context(), &fsxsdk.DeleteFileSystemInput{FileSystemId: aws.String(id)})
			require.NoError(t, err)

			_, err = client.DescribeFileSystems(t.Context(), &fsxsdk.DescribeFileSystemsInput{
				FileSystemIds: []string{id},
			})
			require.Error(t, err)

			var nf *types.FileSystemNotFound
			require.ErrorAs(t, err, &nf, "expected a real types.FileSystemNotFound, got %v", err)
		})
	}
}
