package terraform_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	fsxsdk "github.com/aws/aws-sdk-go-v2/service/fsx"
	fsxtypes "github.com/aws/aws-sdk-go-v2/service/fsx/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_MegaBatch32 provisions FSx (OpenZFS file system + volume +
// snapshot, backup, ONTAP file system + storage virtual machine + volume,
// Windows file system, Lustre file system + data repository association,
// file cache) resources via Terraform and verifies each through the FSx
// SDK's Describe* path.
func TestTerraform_MegaBatch32(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "mega-batch-32",
			setup:   setupEndpoint,
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyMegaBatch32(ctx, t)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

func createFSxClient(t *testing.T) *fsxsdk.Client {
	t.Helper()

	return createClientWithEndpoint(t, fsxsdk.NewFromConfig, endpoint)
}

func verifyMegaBatch32(ctx context.Context, t *testing.T) {
	t.Helper()

	client := createFSxClient(t)

	fsOut, err := client.DescribeFileSystems(ctx, &fsxsdk.DescribeFileSystemsInput{})
	require.NoError(t, err, "DescribeFileSystems should succeed")

	var sawOpenZFS, sawOntap, sawWindows, sawPersistentLustre bool

	var ontapFSID, openzfsFSID string

	for _, fs := range fsOut.FileSystems {
		switch fs.FileSystemType {
		case fsxtypes.FileSystemTypeOpenzfs:
			sawOpenZFS = true
			openzfsFSID = aws.ToString(fs.FileSystemId)
		case fsxtypes.FileSystemTypeOntap:
			sawOntap = true
			ontapFSID = aws.ToString(fs.FileSystemId)
		case fsxtypes.FileSystemTypeWindows:
			sawWindows = true
		case fsxtypes.FileSystemTypeLustre:
			// Other fixtures in the shard create SCRATCH Lustre file systems.
			if fs.LustreConfiguration.DeploymentType == fsxtypes.LustreDeploymentTypePersistent2 {
				sawPersistentLustre = true
			}
		}
	}

	assert.True(t, sawOpenZFS, "an OpenZFS file system should be listed")
	assert.True(t, sawOntap, "an ONTAP file system should be listed")
	assert.True(t, sawWindows, "a Windows file system should be listed")
	assert.True(t, sawPersistentLustre, "a PERSISTENT_2 Lustre file system should be listed")

	volOut, err := client.DescribeVolumes(ctx, &fsxsdk.DescribeVolumesInput{})
	require.NoError(t, err, "DescribeVolumes should succeed")

	var foundOZVolume, foundOntapVolume bool

	for _, v := range volOut.Volumes {
		switch aws.ToString(v.Name) {
		case "mega-batch-32-oz-vol":
			foundOZVolume = true
			assert.Equal(t, fsxtypes.VolumeTypeOpenzfs, v.VolumeType)
		case "mb32vol":
			foundOntapVolume = true
			assert.Equal(t, fsxtypes.VolumeTypeOntap, v.VolumeType)
		}
	}

	assert.True(t, foundOZVolume, "mega-batch-32 OpenZFS volume should be listed")
	assert.True(t, foundOntapVolume, "mega-batch-32 ONTAP volume should be listed")

	svmOut, err := client.DescribeStorageVirtualMachines(ctx, &fsxsdk.DescribeStorageVirtualMachinesInput{})
	require.NoError(t, err, "DescribeStorageVirtualMachines should succeed")

	var foundSVM bool

	for _, svm := range svmOut.StorageVirtualMachines {
		if aws.ToString(svm.Name) == "mb32svm" {
			foundSVM = true
			assert.Equal(t, ontapFSID, aws.ToString(svm.FileSystemId))
		}
	}

	assert.True(t, foundSVM, "mega-batch-32 storage virtual machine should be listed")

	snapOut, err := client.DescribeSnapshots(ctx, &fsxsdk.DescribeSnapshotsInput{})
	require.NoError(t, err, "DescribeSnapshots should succeed")

	var foundSnapshot bool

	for _, s := range snapOut.Snapshots {
		if aws.ToString(s.Name) == "mega-batch-32-oz-snap" {
			foundSnapshot = true
		}
	}

	assert.True(t, foundSnapshot, "mega-batch-32 snapshot should be listed")

	backupOut, err := client.DescribeBackups(ctx, &fsxsdk.DescribeBackupsInput{})
	require.NoError(t, err, "DescribeBackups should succeed")

	var foundBackup bool

	for _, b := range backupOut.Backups {
		if b.FileSystem != nil && aws.ToString(b.FileSystem.FileSystemId) == openzfsFSID {
			foundBackup = true
			assert.Equal(t, fsxtypes.BackupTypeUserInitiated, b.Type)
		}
	}

	assert.True(t, foundBackup, "mega-batch-32 backup should be listed")

	draOut, err := client.DescribeDataRepositoryAssociations(ctx, &fsxsdk.DescribeDataRepositoryAssociationsInput{})
	require.NoError(t, err, "DescribeDataRepositoryAssociations should succeed")

	var foundDRA bool

	for _, d := range draOut.Associations {
		if aws.ToString(d.DataRepositoryPath) == "s3://mega-batch-32-dra-bucket" {
			foundDRA = true
		}
	}

	assert.True(t, foundDRA, "mega-batch-32 data repository association should be listed")

	cacheOut, err := client.DescribeFileCaches(ctx, &fsxsdk.DescribeFileCachesInput{})
	require.NoError(t, err, "DescribeFileCaches should succeed")
	require.Len(t, cacheOut.FileCaches, 1)
	assert.Equal(t, fsxtypes.FileCacheTypeLustre, cacheOut.FileCaches[0].FileCacheType)
}
