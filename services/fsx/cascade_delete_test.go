package fsx_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fsx"
)

// TestFSx_DeleteFileSystem_CascadesToChildren verifies that DeleteFileSystem
// removes every child resource real AWS also tears down when a file system
// is deleted: storage virtual machines, the volumes hosted on them, those
// volumes' snapshots, directly-attached volumes (e.g. the OpenZFS root
// volume), and data repository associations. Before this test was added,
// DeleteFileSystem only removed the file system + its own tags, leaving
// ghost rows in every one of those tables.
func TestFSx_DeleteFileSystem_CascadesToChildren(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := fsx.GetBackend(h)
	fsID := createFS(t, h, "ONTAP")

	svmRec := doFSxRequest(t, h, "CreateStorageVirtualMachine", map[string]any{
		"FileSystemId": fsID,
		"Name":         "svm1",
	})
	require.Equal(t, http.StatusOK, svmRec.Code)
	svmID := decodeField(t, svmRec, "StorageVirtualMachine")["StorageVirtualMachineId"].(string)

	volRec := doFSxRequest(t, h, "CreateVolume", map[string]any{
		"VolumeType":         "ONTAP",
		"Name":               "vol1",
		"OntapConfiguration": map[string]any{"StorageVirtualMachineId": svmID},
	})
	require.Equal(t, http.StatusOK, volRec.Code)
	volID := decodeField(t, volRec, "Volume")["VolumeId"].(string)

	snapRec := doFSxRequest(t, h, "CreateSnapshot", map[string]any{
		"VolumeId": volID,
		"Name":     "snap1",
	})
	require.Equal(t, http.StatusOK, snapRec.Code)

	draRec := doFSxRequest(t, h, "CreateDataRepositoryAssociation", map[string]any{
		"FileSystemId":       fsID,
		"FileSystemPath":     "/data",
		"DataRepositoryPath": "s3://bucket/data",
	})
	require.Equal(t, http.StatusOK, draRec.Code)

	require.Equal(t, 1, fsx.SVMCount(b))
	require.Equal(t, 1, fsx.VolumeCount(b))
	require.Equal(t, 1, fsx.SnapshotCount(b))
	require.Equal(t, 1, fsx.DRACount(b))

	delRec := doFSxRequest(t, h, "DeleteFileSystem", map[string]any{"FileSystemId": fsID})
	require.Equal(t, http.StatusOK, delRec.Code)

	assert.Equal(t, 0, fsx.FileSystemCount(b))
	assert.Equal(t, 0, fsx.SVMCount(b), "SVM must be cascade-deleted")
	assert.Equal(t, 0, fsx.VolumeCount(b), "volume must be cascade-deleted")
	assert.Equal(t, 0, fsx.SnapshotCount(b), "snapshot must be cascade-deleted")
	assert.Equal(t, 0, fsx.DRACount(b), "data repository association must be cascade-deleted")
}

// TestFSx_DeleteFileSystem_DoesNotCascadeToBackups verifies that
// DeleteFileSystem leaves backups alone: real AWS FSx backups persist
// independently of the file system they were taken from.
func TestFSx_DeleteFileSystem_DoesNotCascadeToBackups(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := fsx.GetBackend(h)
	fsID := createFS(t, h, "LUSTRE")

	backupRec := doFSxRequest(t, h, "CreateBackup", map[string]any{"FileSystemId": fsID})
	require.Equal(t, http.StatusOK, backupRec.Code)

	delRec := doFSxRequest(t, h, "DeleteFileSystem", map[string]any{"FileSystemId": fsID})
	require.Equal(t, http.StatusOK, delRec.Code)

	assert.Equal(t, 1, fsx.BackupCount(b), "backups must survive file system deletion")
}

// TestFSx_DeleteVolume_CascadesToSnapshots verifies that DeleteVolume removes
// snapshots of that volume, so no ghost Snapshot row (pointing at a
// now-nonexistent VolumeId) survives.
func TestFSx_DeleteVolume_CascadesToSnapshots(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := fsx.GetBackend(h)
	fsID := createFS(t, h, "OPENZFS")

	volRec := doFSxRequest(t, h, "CreateVolume", map[string]any{
		"VolumeType":           "OPENZFS",
		"Name":                 "vol1",
		"OpenZFSConfiguration": map[string]any{"ParentVolumeId": openZFSRootVolumeID(t, h, fsID)},
	})
	require.Equal(t, http.StatusOK, volRec.Code)
	volID := decodeField(t, volRec, "Volume")["VolumeId"].(string)

	snapRec := doFSxRequest(t, h, "CreateSnapshot", map[string]any{
		"VolumeId": volID,
		"Name":     "snap1",
	})
	require.Equal(t, http.StatusOK, snapRec.Code)
	require.Equal(t, 1, fsx.SnapshotCount(b))

	delRec := doFSxRequest(t, h, "DeleteVolume", map[string]any{"VolumeId": volID})
	require.Equal(t, http.StatusOK, delRec.Code)

	assert.Equal(t, 0, fsx.SnapshotCount(b), "snapshot must be cascade-deleted")
}

// TestFSx_DeleteStorageVirtualMachine_CascadesToVolumes verifies that
// DeleteStorageVirtualMachine removes every volume hosted on that SVM.
func TestFSx_DeleteStorageVirtualMachine_CascadesToVolumes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := fsx.GetBackend(h)
	fsID := createFS(t, h, "ONTAP")

	svmRec := doFSxRequest(t, h, "CreateStorageVirtualMachine", map[string]any{
		"FileSystemId": fsID,
		"Name":         "svm1",
	})
	require.Equal(t, http.StatusOK, svmRec.Code)
	svmID := decodeField(t, svmRec, "StorageVirtualMachine")["StorageVirtualMachineId"].(string)

	volRec := doFSxRequest(t, h, "CreateVolume", map[string]any{
		"VolumeType":         "ONTAP",
		"Name":               "vol1",
		"OntapConfiguration": map[string]any{"StorageVirtualMachineId": svmID},
	})
	require.Equal(t, http.StatusOK, volRec.Code)
	require.Equal(t, 1, fsx.VolumeCount(b))

	delRec := doFSxRequest(t, h, "DeleteStorageVirtualMachine", map[string]any{"StorageVirtualMachineId": svmID})
	require.Equal(t, http.StatusOK, delRec.Code)

	assert.Equal(t, 0, fsx.VolumeCount(b), "volume must be cascade-deleted")
}
