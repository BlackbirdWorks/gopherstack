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

// TestDescribeBackups_Filters proves DescribeBackupsInput.Filters (fsx@v1.68.4
// api_op_DescribeBackups.go, supported names file-system-id/backup-type/
// file-system-type per its own doc comment) was declared on the real wire but
// had no field for it anywhere in gopherstack's describeBackupsInput struct --
// a real client's filter silently no-op'd and always got the unfiltered list.
func TestDescribeBackups_Filters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestFSxClient(t, h)

	lustreFS := createTestLustreFS(t, client)
	ontapFS := createTestOntapFS(t, client)

	lustreBackup, setupErr := client.CreateBackup(t.Context(), &fsxsdk.CreateBackupInput{
		FileSystemId: lustreFS.FileSystem.FileSystemId,
	})
	require.NoError(t, setupErr)

	ontapBackup, setupErr := client.CreateBackup(t.Context(), &fsxsdk.CreateBackupInput{
		FileSystemId: ontapFS.FileSystem.FileSystemId,
	})
	require.NoError(t, setupErr)

	t.Run("file-system-id", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeBackups(t.Context(), &fsxsdk.DescribeBackupsInput{
			Filters: []types.Filter{{
				Name:   types.FilterNameFileSystemId,
				Values: []string{aws.ToString(lustreFS.FileSystem.FileSystemId)},
			}},
		})
		require.NoError(t, err)
		require.Len(t, out.Backups, 1)
		assert.Equal(t, aws.ToString(lustreBackup.Backup.BackupId), aws.ToString(out.Backups[0].BackupId))
	})

	t.Run("file-system-type", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeBackups(t.Context(), &fsxsdk.DescribeBackupsInput{
			Filters: []types.Filter{{
				Name:   types.FilterNameFileSystemType,
				Values: []string{"ONTAP"},
			}},
		})
		require.NoError(t, err)
		require.Len(t, out.Backups, 1)
		assert.Equal(t, aws.ToString(ontapBackup.Backup.BackupId), aws.ToString(out.Backups[0].BackupId))
	})

	t.Run("backup-type excludes non-matching", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeBackups(t.Context(), &fsxsdk.DescribeBackupsInput{
			Filters: []types.Filter{{
				Name:   types.FilterNameBackupType,
				Values: []string{"AWS_BACKUP"},
			}},
		})
		require.NoError(t, err)
		assert.Empty(t, out.Backups, "no backup in this backend is ever AWS_BACKUP-typed")

		out, err = client.DescribeBackups(t.Context(), &fsxsdk.DescribeBackupsInput{
			Filters: []types.Filter{{
				Name:   types.FilterNameBackupType,
				Values: []string{"USER_INITIATED"},
			}},
		})
		require.NoError(t, err)
		assert.Len(t, out.Backups, 2)
	})
}

// TestDescribeDataRepositoryAssociations_FileSystemIDFilter proves
// DescribeDataRepositoryAssociationsInput.Filters (fsx@v1.68.4
// api_op_DescribeDataRepositoryAssociations.go) had no field at all in
// gopherstack's request struct.
func TestDescribeDataRepositoryAssociations_FileSystemIDFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestFSxClient(t, h)

	fs1 := createTestLustreFS(t, client)
	fs2 := createTestLustreFS(t, client)

	assoc1, err := client.CreateDataRepositoryAssociation(t.Context(), &fsxsdk.CreateDataRepositoryAssociationInput{
		FileSystemId:       fs1.FileSystem.FileSystemId,
		DataRepositoryPath: aws.String("s3://bucket-one"),
		FileSystemPath:     aws.String("/data1"),
	})
	require.NoError(t, err)

	_, err = client.CreateDataRepositoryAssociation(t.Context(), &fsxsdk.CreateDataRepositoryAssociationInput{
		FileSystemId:       fs2.FileSystem.FileSystemId,
		DataRepositoryPath: aws.String("s3://bucket-two"),
		FileSystemPath:     aws.String("/data2"),
	})
	require.NoError(t, err)

	out, err := client.DescribeDataRepositoryAssociations(t.Context(), &fsxsdk.DescribeDataRepositoryAssociationsInput{
		Filters: []types.Filter{{
			Name:   types.FilterNameFileSystemId,
			Values: []string{aws.ToString(fs1.FileSystem.FileSystemId)},
		}},
	})
	require.NoError(t, err)
	require.Len(t, out.Associations, 1)
	assert.Equal(t, aws.ToString(assoc1.Association.AssociationId), aws.ToString(out.Associations[0].AssociationId))
}

// TestDescribeDataRepositoryTasks_Filters proves
// DescribeDataRepositoryTasksInput.Filters (fsx@v1.68.4
// api_op_DescribeDataRepositoryTasks.go) had no field at all in gopherstack's
// request struct.
func TestDescribeDataRepositoryTasks_Filters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestFSxClient(t, h)

	fs1 := createTestLustreFS(t, client)
	fs2 := createTestLustreFS(t, client)

	task1, setupErr := client.CreateDataRepositoryTask(t.Context(), &fsxsdk.CreateDataRepositoryTaskInput{
		FileSystemId: fs1.FileSystem.FileSystemId,
		Type:         types.DataRepositoryTaskTypeExport,
		Report:       &types.CompletionReport{Enabled: aws.Bool(false)},
	})
	require.NoError(t, setupErr)

	task2, setupErr := client.CreateDataRepositoryTask(t.Context(), &fsxsdk.CreateDataRepositoryTaskInput{
		FileSystemId: fs2.FileSystem.FileSystemId,
		Type:         types.DataRepositoryTaskTypeExport,
		Report:       &types.CompletionReport{Enabled: aws.Bool(false)},
	})
	require.NoError(t, setupErr)

	_, setupErr = client.CancelDataRepositoryTask(t.Context(), &fsxsdk.CancelDataRepositoryTaskInput{
		TaskId: task2.DataRepositoryTask.TaskId,
	})
	require.NoError(t, setupErr)

	t.Run("file-system-id", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeDataRepositoryTasks(t.Context(), &fsxsdk.DescribeDataRepositoryTasksInput{
			Filters: []types.DataRepositoryTaskFilter{{
				Name:   types.DataRepositoryTaskFilterNameFileSystemId,
				Values: []string{aws.ToString(fs1.FileSystem.FileSystemId)},
			}},
		})
		require.NoError(t, err)
		require.Len(t, out.DataRepositoryTasks, 1)
		assert.Equal(t, aws.ToString(task1.DataRepositoryTask.TaskId), aws.ToString(out.DataRepositoryTasks[0].TaskId))
	})

	t.Run("task-lifecycle", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeDataRepositoryTasks(t.Context(), &fsxsdk.DescribeDataRepositoryTasksInput{
			Filters: []types.DataRepositoryTaskFilter{{
				Name:   types.DataRepositoryTaskFilterNameTaskLifecycle,
				Values: []string{"CANCELING"},
			}},
		})
		require.NoError(t, err)
		require.Len(t, out.DataRepositoryTasks, 1)
		assert.Equal(t, aws.ToString(task2.DataRepositoryTask.TaskId), aws.ToString(out.DataRepositoryTasks[0].TaskId))
	})
}

// TestDescribeSnapshots_Filters proves DescribeSnapshotsInput.Filters
// (fsx@v1.68.4 api_op_DescribeSnapshots.go, supported names file-system-id/
// volume-id) had no field at all in gopherstack's request struct.
func TestDescribeSnapshots_Filters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestFSxClient(t, h)

	vol1 := createTestOntapVolume(t, client, "snap-filter-vol-1")
	vol2 := createTestOntapVolume(t, client, "snap-filter-vol-2")

	snap1, setupErr := client.CreateSnapshot(t.Context(), &fsxsdk.CreateSnapshotInput{
		Name:     aws.String("snap-1"),
		VolumeId: vol1.Volume.VolumeId,
	})
	require.NoError(t, setupErr)

	_, setupErr = client.CreateSnapshot(t.Context(), &fsxsdk.CreateSnapshotInput{
		Name:     aws.String("snap-2"),
		VolumeId: vol2.Volume.VolumeId,
	})
	require.NoError(t, setupErr)

	t.Run("volume-id", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeSnapshots(t.Context(), &fsxsdk.DescribeSnapshotsInput{
			Filters: []types.SnapshotFilter{{
				Name:   types.SnapshotFilterNameVolumeId,
				Values: []string{aws.ToString(vol1.Volume.VolumeId)},
			}},
		})
		require.NoError(t, err)
		require.Len(t, out.Snapshots, 1)
		assert.Equal(t, aws.ToString(snap1.Snapshot.SnapshotId), aws.ToString(out.Snapshots[0].SnapshotId))
	})

	t.Run("file-system-id", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeSnapshots(t.Context(), &fsxsdk.DescribeSnapshotsInput{
			Filters: []types.SnapshotFilter{{
				Name:   types.SnapshotFilterNameFileSystemId,
				Values: []string{aws.ToString(vol1.Volume.FileSystemId)},
			}},
		})
		require.NoError(t, err)
		require.Len(t, out.Snapshots, 1)
		assert.Equal(t, aws.ToString(snap1.Snapshot.SnapshotId), aws.ToString(out.Snapshots[0].SnapshotId))
	})
}

// TestDescribeVolumes_Filters proves DescribeVolumesInput.Filters (fsx@v1.68.4
// api_op_DescribeVolumes.go, supported names file-system-id/
// storage-virtual-machine-id) had no field at all in gopherstack's request
// struct.
func TestDescribeVolumes_Filters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestFSxClient(t, h)

	vol1 := createTestOntapVolume(t, client, "vol-filter-1")
	vol2 := createTestOntapVolume(t, client, "vol-filter-2")

	t.Run("file-system-id", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeVolumes(t.Context(), &fsxsdk.DescribeVolumesInput{
			Filters: []types.VolumeFilter{{
				Name:   types.VolumeFilterNameFileSystemId,
				Values: []string{aws.ToString(vol1.Volume.FileSystemId)},
			}},
		})
		require.NoError(t, err)
		require.Len(t, out.Volumes, 1)
		assert.Equal(t, aws.ToString(vol1.Volume.VolumeId), aws.ToString(out.Volumes[0].VolumeId))
	})

	t.Run("storage-virtual-machine-id", func(t *testing.T) {
		t.Parallel()

		svmID := vol2.Volume.OntapConfiguration.StorageVirtualMachineId

		out, err := client.DescribeVolumes(t.Context(), &fsxsdk.DescribeVolumesInput{
			Filters: []types.VolumeFilter{{
				Name:   types.VolumeFilterNameStorageVirtualMachineId,
				Values: []string{aws.ToString(svmID)},
			}},
		})
		require.NoError(t, err)
		require.Len(t, out.Volumes, 1)
		assert.Equal(t, aws.ToString(vol2.Volume.VolumeId), aws.ToString(out.Volumes[0].VolumeId))
	})
}

// TestDescribeStorageVirtualMachines_FileSystemIDFilter proves
// DescribeStorageVirtualMachinesInput.Filters (fsx@v1.68.4
// api_op_DescribeStorageVirtualMachines.go) had no field at all in
// gopherstack's request struct.
func TestDescribeStorageVirtualMachines_FileSystemIDFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestFSxClient(t, h)

	fs1 := createTestOntapFS(t, client)
	fs2 := createTestOntapFS(t, client)

	svm1, err := client.CreateStorageVirtualMachine(t.Context(), &fsxsdk.CreateStorageVirtualMachineInput{
		FileSystemId: fs1.FileSystem.FileSystemId,
		Name:         aws.String("svm-filter-1"),
	})
	require.NoError(t, err)

	_, err = client.CreateStorageVirtualMachine(t.Context(), &fsxsdk.CreateStorageVirtualMachineInput{
		FileSystemId: fs2.FileSystem.FileSystemId,
		Name:         aws.String("svm-filter-2"),
	})
	require.NoError(t, err)

	out, err := client.DescribeStorageVirtualMachines(t.Context(), &fsxsdk.DescribeStorageVirtualMachinesInput{
		Filters: []types.StorageVirtualMachineFilter{{
			Name:   types.StorageVirtualMachineFilterNameFileSystemId,
			Values: []string{aws.ToString(fs1.FileSystem.FileSystemId)},
		}},
	})
	require.NoError(t, err)
	require.Len(t, out.StorageVirtualMachines, 1)
	assert.Equal(t, aws.ToString(svm1.StorageVirtualMachine.StorageVirtualMachineId),
		aws.ToString(out.StorageVirtualMachines[0].StorageVirtualMachineId))
}

// TestDescribeS3AccessPointAttachments_Filters proves
// DescribeS3AccessPointAttachmentsInput.Filters (fsx@v1.68.4
// api_op_DescribeS3AccessPointAttachments.go, supported names
// file-system-id/volume-id/type) had no field at all in gopherstack's request
// struct.
func TestDescribeS3AccessPointAttachments_Filters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestFSxClient(t, h)

	ontapVol := createTestOntapVolume(t, client, "s3ap-ontap-vol")
	otherVol := createTestOntapVolume(t, client, "s3ap-other-vol")

	ontapFileSystemIdentity := &types.OntapFileSystemIdentity{
		Type: types.OntapFileSystemUserTypeUnix,
		UnixUser: &types.OntapUnixFileSystemUser{
			Name: aws.String("root"),
		},
	}

	ontapAP, setupErr := client.CreateAndAttachS3AccessPoint(t.Context(), &fsxsdk.CreateAndAttachS3AccessPointInput{
		Name: aws.String("s3ap-ontap"),
		Type: types.S3AccessPointAttachmentTypeOntap,
		OntapConfiguration: &types.CreateAndAttachS3AccessPointOntapConfiguration{
			VolumeId:           ontapVol.Volume.VolumeId,
			FileSystemIdentity: ontapFileSystemIdentity,
		},
	})
	require.NoError(t, setupErr)

	_, setupErr = client.CreateAndAttachS3AccessPoint(t.Context(), &fsxsdk.CreateAndAttachS3AccessPointInput{
		Name: aws.String("s3ap-other"),
		Type: types.S3AccessPointAttachmentTypeOntap,
		OntapConfiguration: &types.CreateAndAttachS3AccessPointOntapConfiguration{
			VolumeId:           otherVol.Volume.VolumeId,
			FileSystemIdentity: ontapFileSystemIdentity,
		},
	})
	require.NoError(t, setupErr)

	t.Run("volume-id", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeS3AccessPointAttachments(t.Context(), &fsxsdk.DescribeS3AccessPointAttachmentsInput{
			Filters: []types.S3AccessPointAttachmentsFilter{{
				Name:   types.S3AccessPointAttachmentsFilterNameVolumeId,
				Values: []string{aws.ToString(ontapVol.Volume.VolumeId)},
			}},
		})
		require.NoError(t, err)
		require.Len(t, out.S3AccessPointAttachments, 1)
		assert.Equal(
			t,
			aws.ToString(ontapAP.S3AccessPointAttachment.Name),
			aws.ToString(out.S3AccessPointAttachments[0].Name),
		)
	})

	t.Run("type excludes non-matching", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeS3AccessPointAttachments(t.Context(), &fsxsdk.DescribeS3AccessPointAttachmentsInput{
			Filters: []types.S3AccessPointAttachmentsFilter{{
				Name:   types.S3AccessPointAttachmentsFilterNameType,
				Values: []string{"OPENZFS"},
			}},
		})
		require.NoError(t, err)
		assert.Empty(t, out.S3AccessPointAttachments)
	})

	t.Run("file-system-id", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeS3AccessPointAttachments(t.Context(), &fsxsdk.DescribeS3AccessPointAttachmentsInput{
			Filters: []types.S3AccessPointAttachmentsFilter{{
				Name:   types.S3AccessPointAttachmentsFilterNameFileSystemId,
				Values: []string{aws.ToString(ontapVol.Volume.FileSystemId)},
			}},
		})
		require.NoError(t, err)
		require.Len(t, out.S3AccessPointAttachments, 1)
		assert.Equal(
			t,
			aws.ToString(ontapAP.S3AccessPointAttachment.Name),
			aws.ToString(out.S3AccessPointAttachments[0].Name),
		)
	})
}

// TestDescribeVolumes_Filters_MultipleValuesInOneFilter proves
// matchesFilters (filters.go) ORs every element of a single filter's Values
// list against the resource's field, not just Values[0] -- the confirmed
// "first-element-only" bug shape (bd gopherstack-uox6) found four times
// elsewhere in this campaign. Every existing fsx filter test (this file)
// passes exactly one value per filter, so none of them can distinguish
// "matched Values[0]" from "matched anywhere in Values" -- a filter naming
// two file systems must return volumes from BOTH and exclude a third.
func TestDescribeVolumes_Filters_MultipleValuesInOneFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestFSxClient(t, h)

	vol1 := createTestOntapVolume(t, client, "multi-filter-vol-1")
	vol2 := createTestOntapVolume(t, client, "multi-filter-vol-2")
	vol3 := createTestOntapVolume(t, client, "multi-filter-vol-3")

	out, err := client.DescribeVolumes(t.Context(), &fsxsdk.DescribeVolumesInput{
		Filters: []types.VolumeFilter{{
			Name: types.VolumeFilterNameFileSystemId,
			Values: []string{
				aws.ToString(vol1.Volume.FileSystemId),
				aws.ToString(vol3.Volume.FileSystemId),
			},
		}},
	})
	require.NoError(t, err)

	gotIDs := make([]string, len(out.Volumes))
	for i, v := range out.Volumes {
		gotIDs[i] = aws.ToString(v.VolumeId)
	}

	assert.ElementsMatch(
		t, []string{aws.ToString(vol1.Volume.VolumeId), aws.ToString(vol3.Volume.VolumeId)}, gotIDs,
		"a filter naming two file-system-id values must match volumes on EITHER, not just the first element",
	)
	assert.NotContains(
		t, gotIDs, aws.ToString(vol2.Volume.VolumeId), "the file system not named in Values must be excluded",
	)
}
