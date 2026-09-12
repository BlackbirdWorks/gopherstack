package fsx_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	fsxsdk "github.com/aws/aws-sdk-go-v2/service/fsx"
	"github.com/aws/aws-sdk-go-v2/service/fsx/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fsx"
)

// TestTypedSlice27RealClient drives fsx's remaining typed-client-blind ops
// through the real aws-sdk-go-v2 client (gopherstack-n3zi typed slice 27).
func TestTypedSlice27RealClient(t *testing.T) {
	t.Parallel()

	t.Run("shared vpc configuration describe update", func(t *testing.T) {
		t.Parallel()

		backend := fsx.NewInMemoryBackend("000000000000", tagsRTRegion)
		client := newTestFSxClient(t, fsx.NewHandler(backend))
		ctx := t.Context()

		descOut, err := client.DescribeSharedVpcConfiguration(ctx, &fsxsdk.DescribeSharedVpcConfigurationInput{})
		require.NoError(t, err)
		assert.Equal(t, "false", aws.ToString(descOut.EnableFsxRouteTableUpdatesFromParticipantAccounts))

		updOut, err := client.UpdateSharedVpcConfiguration(ctx, &fsxsdk.UpdateSharedVpcConfigurationInput{
			EnableFsxRouteTableUpdatesFromParticipantAccounts: aws.String("true"),
		})
		require.NoError(t, err)
		assert.Equal(t, "true", aws.ToString(updOut.EnableFsxRouteTableUpdatesFromParticipantAccounts))

		descOut2, err := client.DescribeSharedVpcConfiguration(ctx, &fsxsdk.DescribeSharedVpcConfigurationInput{})
		require.NoError(t, err)
		assert.Equal(t, "true", aws.ToString(descOut2.EnableFsxRouteTableUpdatesFromParticipantAccounts))
	})

	t.Run("file system aliases and lifecycle ops", func(t *testing.T) {
		t.Parallel()

		backend := fsx.NewInMemoryBackend("000000000000", tagsRTRegion)
		client := newTestFSxClient(t, fsx.NewHandler(backend))
		ctx := t.Context()

		fsOut := createTestLustreFS(t, client)
		fsID := fsOut.FileSystem.FileSystemId

		assocOut, err := client.AssociateFileSystemAliases(ctx, &fsxsdk.AssociateFileSystemAliasesInput{
			FileSystemId: fsID,
			Aliases:      []string{"accounting.corp.example.com"},
		})
		require.NoError(t, err)
		require.Len(t, assocOut.Aliases, 1)
		assert.Equal(t, "accounting.corp.example.com", aws.ToString(assocOut.Aliases[0].Name))
		assert.Equal(t, types.AliasLifecycleAvailable, assocOut.Aliases[0].Lifecycle)

		descAliasOut, err := client.DescribeFileSystemAliases(ctx, &fsxsdk.DescribeFileSystemAliasesInput{
			FileSystemId: fsID,
		})
		require.NoError(t, err)
		require.Len(t, descAliasOut.Aliases, 1)

		disOut, err := client.DisassociateFileSystemAliases(ctx, &fsxsdk.DisassociateFileSystemAliasesInput{
			FileSystemId: fsID,
			Aliases:      []string{"accounting.corp.example.com"},
		})
		require.NoError(t, err)
		require.Len(t, disOut.Aliases, 1)
		assert.Equal(t, types.AliasLifecycleDeleting, disOut.Aliases[0].Lifecycle)

		descAliasOut2, err := client.DescribeFileSystemAliases(ctx, &fsxsdk.DescribeFileSystemAliasesInput{
			FileSystemId: fsID,
		})
		require.NoError(t, err)
		assert.Empty(t, descAliasOut2.Aliases)

		relOut, err := client.ReleaseFileSystemNfsV3Locks(ctx, &fsxsdk.ReleaseFileSystemNfsV3LocksInput{
			FileSystemId: fsID,
		})
		require.NoError(t, err)
		require.NotNil(t, relOut.FileSystem)
		assert.Equal(t, aws.ToString(fsID), aws.ToString(relOut.FileSystem.FileSystemId))

		recOut, err := client.StartMisconfiguredStateRecovery(ctx, &fsxsdk.StartMisconfiguredStateRecoveryInput{
			FileSystemId: fsID,
		})
		require.NoError(t, err)
		require.NotNil(t, recOut.FileSystem)
		assert.Equal(t, aws.ToString(fsID), aws.ToString(recOut.FileSystem.FileSystemId))

		updOut, err := client.UpdateFileSystem(ctx, &fsxsdk.UpdateFileSystemInput{
			FileSystemId:    fsID,
			StorageCapacity: aws.Int32(2400),
		})
		require.NoError(t, err)
		assert.Equal(t, int32(2400), aws.ToInt32(updOut.FileSystem.StorageCapacity))

		_, err = client.TagResource(ctx, &fsxsdk.TagResourceInput{
			ResourceARN: fsOut.FileSystem.ResourceARN,
			Tags:        []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
		})
		require.NoError(t, err)

		_, err = client.UntagResource(ctx, &fsxsdk.UntagResourceInput{
			ResourceARN: fsOut.FileSystem.ResourceARN,
			TagKeys:     []string{"env"},
		})
		require.NoError(t, err)

		tagsOut, err := client.ListTagsForResource(ctx, &fsxsdk.ListTagsForResourceInput{
			ResourceARN: fsOut.FileSystem.ResourceARN,
		})
		require.NoError(t, err)
		assert.Empty(t, tagsOut.Tags)
	})

	t.Run("data repository association update and delete", func(t *testing.T) {
		t.Parallel()

		backend := fsx.NewInMemoryBackend("000000000000", tagsRTRegion)
		client := newTestFSxClient(t, fsx.NewHandler(backend))
		ctx := t.Context()

		fsOut := createTestLustreFS(t, client)

		assocOut, err := client.CreateDataRepositoryAssociation(ctx, &fsxsdk.CreateDataRepositoryAssociationInput{
			FileSystemId:       fsOut.FileSystem.FileSystemId,
			DataRepositoryPath: aws.String("s3://original-bucket"),
			FileSystemPath:     aws.String("/data"),
		})
		require.NoError(t, err)
		assocID := assocOut.Association.AssociationId

		updOut, err := client.UpdateDataRepositoryAssociation(ctx, &fsxsdk.UpdateDataRepositoryAssociationInput{
			AssociationId:         assocID,
			ImportedFileChunkSize: aws.Int32(2048),
		})
		require.NoError(t, err)
		require.NotNil(t, updOut.Association)
		assert.Equal(t, aws.ToString(assocID), aws.ToString(updOut.Association.AssociationId))

		delOut, err := client.DeleteDataRepositoryAssociation(ctx, &fsxsdk.DeleteDataRepositoryAssociationInput{
			AssociationId: assocID,
		})
		require.NoError(t, err)
		assert.Equal(t, aws.ToString(assocID), aws.ToString(delOut.AssociationId))
		assert.Equal(t, types.DataRepositoryLifecycleDeleting, delOut.Lifecycle)
	})

	t.Run("file cache describe update delete", func(t *testing.T) {
		t.Parallel()

		backend := fsx.NewInMemoryBackend("000000000000", tagsRTRegion)
		client := newTestFSxClient(t, fsx.NewHandler(backend))
		ctx := t.Context()

		createOut, err := client.CreateFileCache(ctx, &fsxsdk.CreateFileCacheInput{
			FileCacheType:        types.FileCacheTypeLustre,
			FileCacheTypeVersion: aws.String("2.12"),
			StorageCapacity:      aws.Int32(1200),
			SubnetIds:            []string{"subnet-0123abcd"},
			LustreConfiguration: &types.CreateFileCacheLustreConfiguration{
				DeploymentType: types.FileCacheLustreDeploymentTypeCache1,
				MetadataConfiguration: &types.FileCacheLustreMetadataConfiguration{
					StorageCapacity: aws.Int32(2400),
				},
				PerUnitStorageThroughput: aws.Int32(1000),
			},
		})
		require.NoError(t, err)
		cacheID := createOut.FileCache.FileCacheId

		descOut, err := client.DescribeFileCaches(ctx, &fsxsdk.DescribeFileCachesInput{
			FileCacheIds: []string{aws.ToString(cacheID)},
		})
		require.NoError(t, err)
		require.Len(t, descOut.FileCaches, 1)
		assert.Equal(t, aws.ToString(cacheID), aws.ToString(descOut.FileCaches[0].FileCacheId))

		updOut, err := client.UpdateFileCache(ctx, &fsxsdk.UpdateFileCacheInput{
			FileCacheId: cacheID,
		})
		require.NoError(t, err)
		require.NotNil(t, updOut.FileCache)
		assert.Equal(t, aws.ToString(cacheID), aws.ToString(updOut.FileCache.FileCacheId))

		delOut, err := client.DeleteFileCache(ctx, &fsxsdk.DeleteFileCacheInput{FileCacheId: cacheID})
		require.NoError(t, err)
		assert.Equal(t, aws.ToString(cacheID), aws.ToString(delOut.FileCacheId))
		assert.Equal(t, types.FileCacheLifecycleDeleting, delOut.Lifecycle)
	})

	t.Run("storage virtual machine update delete", func(t *testing.T) {
		t.Parallel()

		backend := fsx.NewInMemoryBackend("000000000000", tagsRTRegion)
		client := newTestFSxClient(t, fsx.NewHandler(backend))
		ctx := t.Context()

		fsOut := createTestOntapFS(t, client)

		svmOut, err := client.CreateStorageVirtualMachine(ctx, &fsxsdk.CreateStorageVirtualMachineInput{
			FileSystemId: fsOut.FileSystem.FileSystemId,
			Name:         aws.String("update-delete-svm"),
		})
		require.NoError(t, err)
		svmID := svmOut.StorageVirtualMachine.StorageVirtualMachineId

		updOut, err := client.UpdateStorageVirtualMachine(ctx, &fsxsdk.UpdateStorageVirtualMachineInput{
			StorageVirtualMachineId: svmID,
			SvmAdminPassword:        aws.String("newpassword123"),
		})
		require.NoError(t, err)
		require.NotNil(t, updOut.StorageVirtualMachine)
		assert.Equal(t, aws.ToString(svmID), aws.ToString(updOut.StorageVirtualMachine.StorageVirtualMachineId))

		delOut, err := client.DeleteStorageVirtualMachine(ctx, &fsxsdk.DeleteStorageVirtualMachineInput{
			StorageVirtualMachineId: svmID,
		})
		require.NoError(t, err)
		assert.Equal(t, aws.ToString(svmID), aws.ToString(delOut.StorageVirtualMachineId))
		assert.Equal(t, types.StorageVirtualMachineLifecycleDeleting, delOut.Lifecycle)
	})

	t.Run("volume update and snapshot update delete", func(t *testing.T) {
		t.Parallel()

		backend := fsx.NewInMemoryBackend("000000000000", tagsRTRegion)
		client := newTestFSxClient(t, fsx.NewHandler(backend))
		ctx := t.Context()

		volOut := createTestOntapVolume(t, client, "s27-volume")
		volID := volOut.Volume.VolumeId

		updVolOut, err := client.UpdateVolume(ctx, &fsxsdk.UpdateVolumeInput{
			VolumeId: volID,
			Name:     aws.String("s27-volume-renamed"),
		})
		require.NoError(t, err)
		assert.Equal(t, "s27-volume-renamed", aws.ToString(updVolOut.Volume.Name))

		snapOut, err := client.CreateSnapshot(ctx, &fsxsdk.CreateSnapshotInput{
			Name:     aws.String("s27-snapshot"),
			VolumeId: volID,
		})
		require.NoError(t, err)
		snapID := snapOut.Snapshot.SnapshotId

		updSnapOut, err := client.UpdateSnapshot(ctx, &fsxsdk.UpdateSnapshotInput{
			SnapshotId: snapID,
			Name:       aws.String("s27-snapshot-renamed"),
		})
		require.NoError(t, err)
		assert.Equal(t, "s27-snapshot-renamed", aws.ToString(updSnapOut.Snapshot.Name))

		delSnapOut, err := client.DeleteSnapshot(ctx, &fsxsdk.DeleteSnapshotInput{SnapshotId: snapID})
		require.NoError(t, err)
		assert.Equal(t, aws.ToString(snapID), aws.ToString(delSnapOut.SnapshotId))
		assert.Equal(t, types.SnapshotLifecycleDeleting, delSnapOut.Lifecycle)
	})
}
