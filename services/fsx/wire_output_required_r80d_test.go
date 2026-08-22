package fsx_test

import (
	"testing"

	fsxsdk "github.com/aws/aws-sdk-go-v2/service/fsx"
	fsxtypes "github.com/aws/aws-sdk-go-v2/service/fsx/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fsx"
)

// Test_SDKRoundTrip_Backup_FileSystem_SurvivesFileSystemDeletion proves
// gopherstack-r80d's batch-33 wrapped-type-shape finding for fsx:
// CreateBackupOutput/DescribeBackupsOutput/CopyBackupOutput declare zero
// required members of their own (invisible to cmd/requiredoutputfields'
// flat op-level scan), but each wraps types.Backup one level down, which
// marks FileSystem (*types.FileSystem, aws-sdk-go-v2/service/fsx@v1.68.4/
// types/types.go) "This member is required." -- and that field's own doc
// comment states the metadata "is persisted even if the file system is
// deleted."
//
// Before the fix, gopherstack's toBackup derived FileSystem from a live
// lookup in the fileSystems table at read time; once the source file system
// was deleted, the lookup missed and the omitempty-tagged FileSystem key was
// dropped entirely, decoding to nil on any real client even though real FSx
// keeps serving it. DeleteFileSystem not cascading to backups (already
// covered by TestFSx_DeleteFileSystem_DoesNotCascadeToBackups) makes this a
// genuinely reachable state, not a hypothetical one.
func Test_SDKRoundTrip_Backup_FileSystem_SurvivesFileSystemDeletion(t *testing.T) {
	t.Parallel()

	backend := fsx.NewInMemoryBackend("000000000000", "us-east-1")
	h := fsx.NewHandler(backend)
	client := newTestFSxClient(t, h)
	ctx := t.Context()

	createOut, err := client.CreateFileSystem(ctx, &fsxsdk.CreateFileSystemInput{
		FileSystemType: fsxtypes.FileSystemTypeLustre,
		SubnetIds:      []string{"subnet-01234567"},
	})
	require.NoError(t, err)
	fsID := createOut.FileSystem.FileSystemId

	backupOut, err := client.CreateBackup(ctx, &fsxsdk.CreateBackupInput{
		FileSystemId: fsID,
	})
	require.NoError(t, err)
	require.NotNil(t, backupOut.Backup.FileSystem,
		"FileSystem is required on Backup and must be present immediately after creation")
	require.NotNil(t, backupOut.Backup.FileSystem.FileSystemId)
	require.Equal(t, *fsID, *backupOut.Backup.FileSystem.FileSystemId)

	backupID := backupOut.Backup.BackupId

	_, err = client.DeleteFileSystem(ctx, &fsxsdk.DeleteFileSystemInput{FileSystemId: fsID})
	require.NoError(t, err)

	descOut, err := client.DescribeBackups(ctx, &fsxsdk.DescribeBackupsInput{
		BackupIds: []string{*backupID},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Backups, 1)
	require.NotNil(t, descOut.Backups[0].FileSystem,
		"Backup.FileSystem is required and must survive deletion of the source file system, "+
			"matching real FSx's own documented behavior")
	require.NotNil(t, descOut.Backups[0].FileSystem.FileSystemId)
	require.Equal(t, *fsID, *descOut.Backups[0].FileSystem.FileSystemId)

	copyOut, err := client.CopyBackup(ctx, &fsxsdk.CopyBackupInput{
		SourceBackupId: backupID,
	})
	require.NoError(t, err)
	require.NotNil(t, copyOut.Backup.FileSystem,
		"CopyBackup must propagate the source backup's own FileSystem snapshot")
	require.NotNil(t, copyOut.Backup.FileSystem.FileSystemId)
	require.Equal(t, *fsID, *copyOut.Backup.FileSystem.FileSystemId)
}
