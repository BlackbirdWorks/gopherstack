package integration_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
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

// mustCreateFSxSubnet creates a real VPC and subnet via a fresh EC2 client so
// CreateFileSystemInput.SubnetIds carries a subnet the EC2 backend actually
// knows about, rather than a fabricated literal that would break the moment
// FSx gains an EC2Resolver.SubnetExists check like EFS's (gopherstack-1o31);
// today FSx only checks the subnet ID's format (services/fsx/file_systems.go
// subnetIDPattern), not its existence. Cleanup for both is registered
// immediately, before the caller creates its file system, so teardown
// deletes the file system first and the VPC/subnet after.
func mustCreateFSxSubnet(t *testing.T, cidrBase string) string {
	t.Helper()

	ec2Client := createEC2Client(t)
	ctx := t.Context()

	vpcOut, err := ec2Client.CreateVpc(ctx, &ec2sdk.CreateVpcInput{
		CidrBlock: aws.String(cidrBase + ".0.0/16"),
	})
	require.NoError(t, err, "CreateVpc should succeed")
	vpcID := aws.ToString(vpcOut.Vpc.VpcId)

	subnetOut, err := ec2Client.CreateSubnet(ctx, &ec2sdk.CreateSubnetInput{
		VpcId:     aws.String(vpcID),
		CidrBlock: aws.String(cidrBase + ".1.0/24"),
	})
	require.NoError(t, err, "CreateSubnet should succeed")
	subnetID := aws.ToString(subnetOut.Subnet.SubnetId)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = ec2Client.DeleteSubnet(cleanupCtx, &ec2sdk.DeleteSubnetInput{SubnetId: aws.String(subnetID)})
		_, _ = ec2Client.DeleteVpc(cleanupCtx, &ec2sdk.DeleteVpcInput{VpcId: aws.String(vpcID)})
	})

	return subnetID
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

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createFSxClient(t)
			subnetID := mustCreateFSxSubnet(t, fmt.Sprintf("172.%d", 22+i))

			createOut, err := client.CreateFileSystem(ctx, &fsxsdk.CreateFileSystemInput{
				FileSystemType:  tt.fileSystemType,
				StorageCapacity: aws.Int32(tt.capacity),
				SubnetIds:       []string{subnetID},
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

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createFSxClient(t)
			subnetID := mustCreateFSxSubnet(t, fmt.Sprintf("172.%d", 23+i))

			fsOut, err := client.CreateFileSystem(ctx, &fsxsdk.CreateFileSystemInput{
				FileSystemType:  fsxtypes.FileSystemTypeLustre,
				StorageCapacity: aws.Int32(1200),
				SubnetIds:       []string{subnetID},
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
