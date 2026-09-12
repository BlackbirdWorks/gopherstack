package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	efstypes "github.com/aws/aws-sdk-go-v2/service/efs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_EFS_FileSystemLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createEFSClient(t)
	ctx := t.Context()

	creationToken := "token-" + uuid.NewString()[:8]

	// CreateFileSystem
	createOut, err := client.CreateFileSystem(ctx, &efs.CreateFileSystemInput{
		CreationToken:   aws.String(creationToken),
		PerformanceMode: efstypes.PerformanceModeGeneralPurpose,
	})
	require.NoError(t, err)
	require.NotNil(t, createOut)

	fsID := aws.ToString(createOut.FileSystemId)
	require.NotEmpty(t, fsID)
	assert.Equal(t, "available", string(createOut.LifeCycleState))

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteFileSystem(cleanupCtx, &efs.DeleteFileSystemInput{
			FileSystemId: aws.String(fsID),
		})
	})

	// DescribeFileSystems
	descOut, err := client.DescribeFileSystems(ctx, &efs.DescribeFileSystemsInput{
		FileSystemId: aws.String(fsID),
	})
	require.NoError(t, err)
	require.Len(t, descOut.FileSystems, 1)
	assert.Equal(t, fsID, aws.ToString(descOut.FileSystems[0].FileSystemId))

	// DescribeFileSystems - list all
	listOut, err := client.DescribeFileSystems(ctx, &efs.DescribeFileSystemsInput{})
	require.NoError(t, err)

	found := false

	for _, fs := range listOut.FileSystems {
		if aws.ToString(fs.FileSystemId) == fsID {
			found = true

			break
		}
	}

	assert.True(t, found, "created file system should appear in list")

	// DeleteFileSystem
	_, err = client.DeleteFileSystem(ctx, &efs.DeleteFileSystemInput{
		FileSystemId: aws.String(fsID),
	})
	require.NoError(t, err)

	// Verify deleted — list all and ensure FS is absent
	listOut2, err := client.DescribeFileSystems(ctx, &efs.DescribeFileSystemsInput{})
	require.NoError(t, err)

	for _, fs := range listOut2.FileSystems {
		assert.NotEqual(t, fsID, aws.ToString(fs.FileSystemId), "deleted FS should not appear in list")
	}
}

func TestIntegration_EFS_MountTargetLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createEFSClient(t)
	ec2Client := createEC2Client(t)
	ctx := t.Context()

	creationToken := "mt-token-" + uuid.NewString()[:8]

	// Create a VPC and subnet for the mount target to attach to.
	vpcOut, err := ec2Client.CreateVpc(ctx, &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.88.0.0/16")})
	require.NoError(t, err, "CreateVpc should succeed")

	vpcID := aws.ToString(vpcOut.Vpc.VpcId)

	subnetOut, err := ec2Client.CreateSubnet(ctx, &ec2sdk.CreateSubnetInput{
		VpcId:     aws.String(vpcID),
		CidrBlock: aws.String("10.88.1.0/24"),
	})
	require.NoError(t, err, "CreateSubnet should succeed")

	subnetID := aws.ToString(subnetOut.Subnet.SubnetId)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = ec2Client.DeleteSubnet(cleanupCtx, &ec2sdk.DeleteSubnetInput{SubnetId: aws.String(subnetID)})
		_, _ = ec2Client.DeleteVpc(cleanupCtx, &ec2sdk.DeleteVpcInput{VpcId: aws.String(vpcID)})
	})

	// Create a file system to attach mount target to
	createOut, err := client.CreateFileSystem(ctx, &efs.CreateFileSystemInput{
		CreationToken: aws.String(creationToken),
	})
	require.NoError(t, err)

	fsID := aws.ToString(createOut.FileSystemId)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		mts, mErr := client.DescribeMountTargets(cleanupCtx, &efs.DescribeMountTargetsInput{
			FileSystemId: aws.String(fsID),
		})
		if mErr == nil {
			for _, mt := range mts.MountTargets {
				_, _ = client.DeleteMountTarget(cleanupCtx, &efs.DeleteMountTargetInput{
					MountTargetId: mt.MountTargetId,
				})
			}
		}

		_, _ = client.DeleteFileSystem(cleanupCtx, &efs.DeleteFileSystemInput{
			FileSystemId: aws.String(fsID),
		})
	})

	// CreateMountTarget
	mtOut, err := client.CreateMountTarget(ctx, &efs.CreateMountTargetInput{
		FileSystemId: aws.String(fsID),
		SubnetId:     aws.String(subnetID),
	})
	require.NoError(t, err)
	require.NotNil(t, mtOut)

	mtID := aws.ToString(mtOut.MountTargetId)
	require.NotEmpty(t, mtID)
	assert.Equal(t, fsID, aws.ToString(mtOut.FileSystemId))

	// DescribeMountTargets
	descOut, err := client.DescribeMountTargets(ctx, &efs.DescribeMountTargetsInput{
		FileSystemId: aws.String(fsID),
	})
	require.NoError(t, err)
	require.Len(t, descOut.MountTargets, 1)
	assert.Equal(t, mtID, aws.ToString(descOut.MountTargets[0].MountTargetId))

	// DeleteMountTarget
	_, err = client.DeleteMountTarget(ctx, &efs.DeleteMountTargetInput{
		MountTargetId: aws.String(mtID),
	})
	require.NoError(t, err)

	// Verify deleted
	descOut2, err := client.DescribeMountTargets(ctx, &efs.DescribeMountTargetsInput{
		FileSystemId: aws.String(fsID),
	})
	require.NoError(t, err)
	assert.Empty(t, descOut2.MountTargets)
}
