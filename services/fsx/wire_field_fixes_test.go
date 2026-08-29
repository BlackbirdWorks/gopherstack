package fsx_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	fsxsdk "github.com/aws/aws-sdk-go-v2/service/fsx"
	"github.com/aws/aws-sdk-go-v2/service/fsx/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateFileSystemFromBackup_SubnetIdsRoundTrip proves the write-only-state
// bug found sweeping CreateFileSystemFromBackup: SubnetIds is a required real
// CreateFileSystemFromBackupInput member (fsx@v1.68.4
// api_op_CreateFileSystemFromBackup.go) that gopherstack's request struct
// previously had no field for at all, so a real client's subnet placement was
// silently discarded -- the restored FileSystem always came back with an
// empty SubnetIds/NetworkInterfaceIds regardless of what was sent. Drives the
// real typed SDK client end to end (CreateFileSystem -> CreateBackup ->
// CreateFileSystemFromBackup -> DescribeFileSystems) and asserts the restored
// file system's SubnetIds match what was supplied to the restore call, not
// the source file system's.
func TestCreateFileSystemFromBackup_SubnetIdsRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestFSxClient(t, h)

	fsOut := createTestOntapFS(t, client)

	backupOut, err := client.CreateBackup(t.Context(), &fsxsdk.CreateBackupInput{
		FileSystemId: fsOut.FileSystem.FileSystemId,
	})
	require.NoError(t, err)

	restoreSubnets := []string{"subnet-0789abcd"}

	restoreOut, err := client.CreateFileSystemFromBackup(t.Context(), &fsxsdk.CreateFileSystemFromBackupInput{
		BackupId:  backupOut.Backup.BackupId,
		SubnetIds: restoreSubnets,
	})
	require.NoError(t, err)
	require.NotNil(t, restoreOut.FileSystem)
	assert.Equal(t, restoreSubnets, restoreOut.FileSystem.SubnetIds,
		"CreateFileSystemFromBackup must store the caller's SubnetIds, not silently drop them")
	assert.Len(t, restoreOut.FileSystem.NetworkInterfaceIds, 1,
		"a network interface should be synthesized per restored subnet")

	descOut, err := client.DescribeFileSystems(t.Context(), &fsxsdk.DescribeFileSystemsInput{
		FileSystemIds: []string{aws.ToString(restoreOut.FileSystem.FileSystemId)},
	})
	require.NoError(t, err)
	require.Len(t, descOut.FileSystems, 1)
	assert.Equal(t, restoreSubnets, descOut.FileSystems[0].SubnetIds,
		"SubnetIds must also read back correctly through DescribeFileSystems")
}

// TestCopySnapshotAndUpdateVolume_SourceSnapshotARNValidated proves the
// second write-only-state bug: SourceSnapshotARN is a required real
// CopySnapshotAndUpdateVolumeInput member (fsx@v1.68.4
// api_op_CopySnapshotAndUpdateVolume.go) that was decoded off the wire but
// never read anywhere in the handler -- any ARN, including one naming a
// snapshot that doesn't exist, silently "succeeded". A real client's typed
// SDK call with a bogus ARN must now be rejected, matching
// RestoreVolumeFromSnapshot's already-correct sibling behavior for its own
// SnapshotId parameter.
func TestCopySnapshotAndUpdateVolume_SourceSnapshotARNValidated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestFSxClient(t, h)

	volOut := createTestOntapVolume(t, client, "copy-validate-vol")

	_, err := client.CopySnapshotAndUpdateVolume(t.Context(), &fsxsdk.CopySnapshotAndUpdateVolumeInput{
		VolumeId:          volOut.Volume.VolumeId,
		SourceSnapshotARN: aws.String("arn:aws:fsx:us-east-1:123456789012:snapshot/fsvolsnap-doesnotexist"),
	})
	require.Error(t, err, "CopySnapshotAndUpdateVolume must reject a SourceSnapshotARN naming a nonexistent snapshot")
}

// TestCreateVolumeFromBackup_StorageVirtualMachineIdRoundTrip proves the
// third bug: real CreateVolumeFromBackupInput carries the target SVM nested
// under OntapConfiguration.StorageVirtualMachineId (fsx@v1.68.4
// api_op_CreateVolumeFromBackup.go, types.CreateOntapVolumeConfiguration) --
// there is no top-level StorageVirtualMachineId or VolumeType member at all.
// gopherstack's pre-fix request struct only had the flat top-level field, so
// no real client could ever populate it: every restored volume came back
// with an empty StorageVirtualMachineId and FileSystemId, regardless of what
// was requested. Mirrors the CreateVolume fix from gopherstack batch8
// (2026-08-23) on this sibling op.
func TestCreateVolumeFromBackup_StorageVirtualMachineIdRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestFSxClient(t, h)

	fsOut := createTestOntapFS(t, client)

	svmOut, err := client.CreateStorageVirtualMachine(t.Context(), &fsxsdk.CreateStorageVirtualMachineInput{
		FileSystemId: fsOut.FileSystem.FileSystemId,
		Name:         aws.String("svm-for-restore"),
	})
	require.NoError(t, err)

	backupOut, err := client.CreateBackup(t.Context(), &fsxsdk.CreateBackupInput{
		FileSystemId: fsOut.FileSystem.FileSystemId,
	})
	require.NoError(t, err)

	restoreOut, err := client.CreateVolumeFromBackup(t.Context(), &fsxsdk.CreateVolumeFromBackupInput{
		BackupId: backupOut.Backup.BackupId,
		Name:     aws.String("restored-vol"),
		OntapConfiguration: &types.CreateOntapVolumeConfiguration{
			StorageVirtualMachineId: svmOut.StorageVirtualMachine.StorageVirtualMachineId,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, restoreOut.Volume)
	require.NotNil(t, restoreOut.Volume.OntapConfiguration,
		"real types.Volume carries StorageVirtualMachineId nested under OntapConfiguration, not a top-level field")
	assert.Equal(t, aws.ToString(svmOut.StorageVirtualMachine.StorageVirtualMachineId),
		aws.ToString(restoreOut.Volume.OntapConfiguration.StorageVirtualMachineId),
		"CreateVolumeFromBackup must resolve the SVM nested under OntapConfiguration, not a top-level field")
	assert.Equal(t, aws.ToString(fsOut.FileSystem.FileSystemId), aws.ToString(restoreOut.Volume.FileSystemId),
		"FileSystemId must be derived from the resolved SVM")

	// Negative case: missing OntapConfiguration is a documented required
	// anchor for this ONTAP-only operation.
	_, err = client.CreateVolumeFromBackup(t.Context(), &fsxsdk.CreateVolumeFromBackupInput{
		BackupId: backupOut.Backup.BackupId,
		Name:     aws.String("restored-vol-2"),
	})
	require.Error(t, err, "CreateVolumeFromBackup must reject a request with no OntapConfiguration")
}

// TestVolume_StorageVirtualMachineIdWireShape proves the fourth wire-shape
// bug this sweep found: real types.Volume (fsx@v1.68.4 types/types.go) has
// no top-level StorageVirtualMachineId member at all -- it's nested under
// OntapConfiguration.StorageVirtualMachineId (deserializers.go:12447's
// OntapVolumeConfiguration case list; deserializers.go:15307's Volume case
// switch confirms no top-level case exists). A prior pass emitted it as a
// fabricated top-level key on every op returning a Volume (CreateVolume,
// DescribeVolumes, UpdateVolume, and the AdministrativeAction.TargetVolumeValues
// nested Volume on RestoreVolumeFromSnapshot/CopySnapshotAndUpdateVolume),
// which a real typed SDK client silently drops -- a volume's SVM association
// was unreadable through any op, even though CreateVolume's *request* side
// already correctly resolves and stores it (gopherstack batch8, 2026-08-23).
// Drives CreateVolume and DescribeVolumes through the real client and asserts
// the SVM is readable at the real nested location.
func TestVolume_StorageVirtualMachineIdWireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestFSxClient(t, h)

	volOut := createTestOntapVolume(t, client, "wire-shape-vol")
	require.NotNil(t, volOut.Volume.OntapConfiguration)
	svmID := aws.ToString(volOut.Volume.OntapConfiguration.StorageVirtualMachineId)
	assert.NotEmpty(t, svmID)

	descOut, err := client.DescribeVolumes(t.Context(), &fsxsdk.DescribeVolumesInput{
		VolumeIds: []string{aws.ToString(volOut.Volume.VolumeId)},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Volumes, 1)
	require.NotNil(t, descOut.Volumes[0].OntapConfiguration,
		"DescribeVolumes must also nest StorageVirtualMachineId under OntapConfiguration")
	assert.Equal(t, svmID, aws.ToString(descOut.Volumes[0].OntapConfiguration.StorageVirtualMachineId))
}
