package fsx_test

// persistence_test.go covers the Phase 3.3 pkgs/store datalayer conversion:
// a Snapshot->Restore round trip across every store.Table-backed resource
// family (fileSystems, backups, dataRepositoryAssocs, dataRepositoryTasks,
// fileCaches, snapshots, storageVirtualMachines, volumes, s3AccessPoints)
// plus every plain map left un-converted (tags, aliases) and the
// sharedVpcEnabled scalar, and a snapshot-version-mismatch test.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fsx"
)

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := fsx.NewInMemoryBackend("123456789012", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createFS(t, h, "LUSTRE")

	b := fsx.GetBackend(h)

	// A syntactically valid but version-mismatched snapshot.
	err := b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Equal(t, 0, fsx.FileSystemCount(b))
}

// TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero verifies that a
// snapshot in the pre-Phase-3.3 shape (a raw fileSystems map, no
// version/tables keys) decodes with Version == 0, which mismatches
// fsxSnapshotVersion and is discarded the same way any other incompatible
// version is -- not partially applied.
func TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createFS(t, h, "LUSTRE")

	b := fsx.GetBackend(h)

	err := b.Restore(t.Context(), []byte(`{"fileSystems":{"fs-seed":{"fileSystemId":"fs-seed"}}}`))
	require.NoError(t, err)

	assert.Equal(t, 0, fsx.FileSystemCount(b))
}

// TestHandler_SnapshotRestoreDelegate verifies the Handler-level Snapshot and
// Restore -- which the persistence layer actually calls via the
// service.Registerable duck-typed Persistable check, not the backend's
// directly. Before this Phase 3.3 conversion, Handler did not implement
// these methods at all, so fsx was silently never registered for
// persistence; see handler.go.
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createFS(t, h, "LUSTRE")

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := newTestHandler(t)
	require.NoError(t, h2.Restore(t.Context(), snap))

	assert.Equal(t, 1, fsx.FileSystemCount(fsx.GetBackend(h2)))
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot->Restore
// round trip across every resource family the Phase 3.3 pkgs/store
// conversion touched, plus every plain map left un-converted (tags,
// aliases) and the sharedVpcEnabled scalar.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	fsID := createTaggedFS(t, h)
	backupID := createBackupForFS(t, h, fsID)

	rec := doFSxRequest(t, h, "AssociateFileSystemAliases", map[string]any{
		"FileSystemId": fsID,
		"Aliases":      []string{"alias1.example.com"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	assocID := createDRA(t, h, fsID)
	taskID := createDRT(t, h, fsID)
	fileCacheID := createFileCache(t, h, "LUSTRE")
	svmID := createSVM(t, h, fsID, "svm1")
	volumeID := createVolume(t, h, fsID, "ONTAP", "vol1")
	snapshotID := createVolSnapshot(t, h, volumeID)
	apName := createS3AP(t, h, fsID)

	rec = doFSxRequest(t, h, "UpdateSharedVpcConfiguration", map[string]any{
		"EnableSharedVpcOnFileSystemCreation": "true",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	b := fsx.GetBackend(h)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := fsx.NewInMemoryBackend("999999999999", "eu-west-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	assert.Equal(t, fsx.FileSystemCount(b), fsx.FileSystemCount(fresh))
	assert.Equal(t, fsx.BackupCount(b), fsx.BackupCount(fresh))
	assert.Equal(t, fsx.DRACount(b), fsx.DRACount(fresh))
	assert.Equal(t, fsx.DataRepositoryTaskCount(b), fsx.DataRepositoryTaskCount(fresh))
	assert.Equal(t, fsx.FileCacheCount(b), fsx.FileCacheCount(fresh))
	assert.Equal(t, fsx.SnapshotCount(b), fsx.SnapshotCount(fresh))
	assert.Equal(t, fsx.SVMCount(b), fsx.SVMCount(fresh))
	assert.Equal(t, fsx.VolumeCount(b), fsx.VolumeCount(fresh))
	assert.Equal(t, fsx.S3AccessPointCount(b), fsx.S3AccessPointCount(fresh))

	// Prove createFileSystemTokens (ClientRequestToken dedup state) survived
	// the round trip, not just the resource tables: replaying the identical
	// CreateFileSystem request against the restored backend must return the
	// SAME file system rather than creating a second one.
	dedupRec := doFSxRequest(t, fsx.NewHandler(fresh), "CreateFileSystem", map[string]any{
		"FileSystemType":     "LUSTRE",
		"Tags":               []map[string]string{{"Key": "env", "Value": "prod"}},
		"ClientRequestToken": taggedFSClientRequestToken,
	})
	require.Equal(t, http.StatusOK, dedupRec.Code)

	var dedupOut map[string]any
	require.NoError(t, json.Unmarshal(dedupRec.Body.Bytes(), &dedupOut))
	assert.Equal(t, fsID, dedupOut["FileSystem"].(map[string]any)["FileSystemId"])
	assert.Equal(
		t, fsx.FileSystemCount(b), fsx.FileSystemCount(fresh), "dedup replay must not create a new file system",
	)

	freshHandler := fsx.NewHandler(fresh)
	assertFullStateRestored(t, freshHandler, fullStateIDs{
		fsID:        fsID,
		backupID:    backupID,
		assocID:     assocID,
		taskID:      taskID,
		fileCacheID: fileCacheID,
		svmID:       svmID,
		volumeID:    volumeID,
		snapshotID:  snapshotID,
		apName:      apName,
	})
}

// fullStateIDs holds every resource ID created by
// TestInMemoryBackend_SnapshotRestore_FullState, passed to
// assertFullStateRestored to keep that function's signature small.
type fullStateIDs struct {
	fsID        string
	backupID    string
	assocID     string
	taskID      string
	fileCacheID string
	svmID       string
	volumeID    string
	snapshotID  string
	apName      string
}

func assertFullStateRestored(t *testing.T, h *fsx.Handler, ids fullStateIDs) {
	t.Helper()

	rec := doFSxRequest(t, h, "DescribeFileSystems", map[string]any{"FileSystemIds": []string{ids.fsID}})
	require.Equal(t, http.StatusOK, rec.Code)

	var fsOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fsOut))
	fsList, ok := fsOut["FileSystems"].([]any)
	require.True(t, ok)
	require.Len(t, fsList, 1)

	fsRecord, ok := fsList[0].(map[string]any)
	require.True(t, ok)
	fsTags, ok := fsRecord["Tags"].([]any)
	require.True(t, ok)
	require.Len(t, fsTags, 1)
	firstTag, ok := fsTags[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "prod", firstTag["Value"])

	rec = doFSxRequest(t, h, "DescribeBackups", map[string]any{"BackupIds": []string{ids.backupID}})
	require.Equal(t, http.StatusOK, rec.Code)
	assertOutputLen(t, rec, "Backups")

	rec = doFSxRequest(t, h, "DescribeFileSystemAliases", map[string]any{"FileSystemId": ids.fsID})
	require.Equal(t, http.StatusOK, rec.Code)

	var aliasOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &aliasOut))
	aliasList, ok := aliasOut["Aliases"].([]any)
	require.True(t, ok)
	require.Len(t, aliasList, 1)
	aliasRecord, ok := aliasList[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "alias1.example.com", aliasRecord["Name"])

	rec = doFSxRequest(t, h, "DescribeDataRepositoryAssociations",
		map[string]any{"AssociationIds": []string{ids.assocID}})
	require.Equal(t, http.StatusOK, rec.Code)
	assertOutputLen(t, rec, "Associations")

	rec = doFSxRequest(t, h, "DescribeDataRepositoryTasks", map[string]any{"TaskIds": []string{ids.taskID}})
	require.Equal(t, http.StatusOK, rec.Code)
	assertOutputLen(t, rec, "DataRepositoryTasks")

	rec = doFSxRequest(t, h, "DescribeFileCaches", map[string]any{"FileCacheIds": []string{ids.fileCacheID}})
	require.Equal(t, http.StatusOK, rec.Code)
	assertOutputLen(t, rec, "FileCaches")

	rec = doFSxRequest(t, h, "DescribeStorageVirtualMachines",
		map[string]any{"StorageVirtualMachineIds": []string{ids.svmID}})
	require.Equal(t, http.StatusOK, rec.Code)
	assertOutputLen(t, rec, "StorageVirtualMachines")

	rec = doFSxRequest(t, h, "DescribeVolumes", map[string]any{"VolumeIds": []string{ids.volumeID}})
	require.Equal(t, http.StatusOK, rec.Code)
	assertOutputLen(t, rec, "Volumes")

	rec = doFSxRequest(t, h, "DescribeSnapshots", map[string]any{"SnapshotIds": []string{ids.snapshotID}})
	require.Equal(t, http.StatusOK, rec.Code)
	assertOutputLen(t, rec, "Snapshots")

	rec = doFSxRequest(t, h, "DescribeS3AccessPointAttachments", map[string]any{"Names": []string{ids.apName}})
	require.Equal(t, http.StatusOK, rec.Code)
	assertOutputLen(t, rec, "S3AccessPoints")

	rec = doFSxRequest(t, h, "DescribeSharedVpcConfiguration", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var vpcOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &vpcOut))
	assert.Equal(t, "true", vpcOut["EnableSharedVpcOnFileSystemCreation"])

	rec = doFSxRequest(t, h, "ListTagsForResource", map[string]any{"ResourceARN": fsRecord["ResourceARN"]})
	require.Equal(t, http.StatusOK, rec.Code)
	assertOutputLen(t, rec, "Tags")
}

// assertOutputLen decodes rec's JSON body and asserts that the array under
// field has exactly one element -- every Describe* call in
// assertFullStateRestored looks up a single just-restored resource by ID.
func assertOutputLen(t *testing.T, rec *httptest.ResponseRecorder, field string) {
	t.Helper()

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	list, ok := out[field].([]any)
	require.True(t, ok, "field %q missing or not an array in response %s", field, rec.Body.String())
	assert.Len(t, list, 1)
}

// taggedFSClientRequestToken is the ClientRequestToken createTaggedFS sends,
// exposed so TestInMemoryBackend_SnapshotRestore_FullState can replay the
// identical CreateFileSystem request post-restore to prove the
// ClientRequestToken dedup map (createFileSystemTokens) survived the
// Snapshot/Restore round trip, not just the resource tables.
const taggedFSClientRequestToken = "tok-full-state-persist"

// createTaggedFS creates a LUSTRE file system with a single "env=prod" tag
// and returns its ID. Unlike the createFS helper shared with other test
// files, this exercises the tags map (resourceARN -> tags) round trip.
func createTaggedFS(t *testing.T, h *fsx.Handler) string {
	t.Helper()
	rec := doFSxRequest(t, h, "CreateFileSystem", map[string]any{
		"FileSystemType":     "LUSTRE",
		"Tags":               []map[string]string{{"Key": "env", "Value": "prod"}},
		"ClientRequestToken": taggedFSClientRequestToken,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	fs, ok := out["FileSystem"].(map[string]any)
	require.True(t, ok)

	id, ok := fs["FileSystemId"].(string)
	require.True(t, ok)

	return id
}

// createBackupForFS creates a backup of fsID with a single "env=prod" tag and
// returns the new backup's ID.
func createBackupForFS(t *testing.T, h *fsx.Handler, fsID string) string {
	t.Helper()
	rec := doFSxRequest(t, h, "CreateBackup", map[string]any{
		"FileSystemId": fsID,
		"Tags":         []map[string]string{{"Key": "env", "Value": "prod"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	backup, ok := out["Backup"].(map[string]any)
	require.True(t, ok)

	id, ok := backup["BackupId"].(string)
	require.True(t, ok)

	return id
}

// createDRA creates a data repository association on fsID and returns its ID.
func createDRA(t *testing.T, h *fsx.Handler, fsID string) string {
	t.Helper()
	rec := doFSxRequest(t, h, "CreateDataRepositoryAssociation", map[string]any{
		"FileSystemId":       fsID,
		"FileSystemPath":     "/data",
		"DataRepositoryPath": "s3://bucket/data",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	assoc, ok := out["Association"].(map[string]any)
	require.True(t, ok)

	id, ok := assoc["AssociationId"].(string)
	require.True(t, ok)

	return id
}

// createDRT creates a data repository task on fsID and returns its ID.
func createDRT(t *testing.T, h *fsx.Handler, fsID string) string {
	t.Helper()
	rec := doFSxRequest(t, h, "CreateDataRepositoryTask", map[string]any{
		"FileSystemId": fsID,
		"Type":         "EXPORT_TO_REPOSITORY",
		"Paths":        []string{"/data"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	task, ok := out["DataRepositoryTask"].(map[string]any)
	require.True(t, ok)

	id, ok := task["TaskId"].(string)
	require.True(t, ok)

	return id
}

// createVolSnapshot creates a snapshot of volumeID and returns its ID.
func createVolSnapshot(t *testing.T, h *fsx.Handler, volumeID string) string {
	t.Helper()
	rec := doFSxRequest(t, h, "CreateSnapshot", map[string]any{
		"VolumeId": volumeID,
		"Name":     "snap1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	snap, ok := out["Snapshot"].(map[string]any)
	require.True(t, ok)

	id, ok := snap["SnapshotId"].(string)
	require.True(t, ok)

	return id
}

// createS3AP creates and attaches an S3 access point to fsID, returning its
// name (S3AccessPoint's primary key).
func createS3AP(t *testing.T, h *fsx.Handler, fsID string) string {
	t.Helper()
	rec := doFSxRequest(t, h, "CreateAndAttachS3AccessPoint", map[string]any{
		"Name":         "ap1",
		"FileSystemId": fsID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	ap, ok := out["S3AccessPoint"].(map[string]any)
	require.True(t, ok)

	name, ok := ap["Name"].(string)
	require.True(t, ok)

	return name
}
