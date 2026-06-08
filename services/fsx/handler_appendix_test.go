package fsx_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fsx"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func createVolume(t *testing.T, h *fsx.Handler, fsID, volType, name string) string {
	t.Helper()
	rec := doFSxRequest(t, h, "CreateVolume", map[string]any{
		"VolumeType":   volType,
		"FileSystemId": fsID,
		"Name":         name,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out["Volume"].(map[string]any)["VolumeId"].(string)
}

func createSVM(t *testing.T, h *fsx.Handler, fsID, name string) string {
	t.Helper()
	rec := doFSxRequest(t, h, "CreateStorageVirtualMachine", map[string]any{
		"FileSystemId": fsID,
		"Name":         name,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out["StorageVirtualMachine"].(map[string]any)["StorageVirtualMachineId"].(string)
}

func createFileCache(t *testing.T, h *fsx.Handler, cacheType string) string {
	t.Helper()
	rec := doFSxRequest(t, h, "CreateFileCache", map[string]any{
		"FileCacheType": cacheType,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out["FileCache"].(map[string]any)["FileCacheId"].(string)
}

// ---------------------------------------------------------------------------
// CopyBackup
// ---------------------------------------------------------------------------

func TestFSx_CopyBackup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setup    func(h *fsx.Handler) string
		wantCode int
		wantErr  string
	}{
		{
			name: "copies backup and returns new BackupId",
			setup: func(h *fsx.Handler) string {
				return createFSandBackup(t, h, "WINDOWS")
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "unknown source backup returns 400",
			setup:    func(_ *fsx.Handler) string { return "backup-does-not-exist" },
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			srcID := tc.setup(h)

			rec := doFSxRequest(t, h, "CopyBackup", map[string]any{"SourceBackupId": srcID})
			require.Equal(t, tc.wantCode, rec.Code)

			if tc.wantCode == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				bk := out["Backup"].(map[string]any)
				assert.Contains(t, bk["BackupId"].(string), "backup-")
				newID := bk["BackupId"].(string)
				assert.NotEqual(t, srcID, newID)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Aliases
// ---------------------------------------------------------------------------

func TestFSx_Aliases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		aliases   []string
		wantCount int
		wantCode  int
		lifecycle string
	}{
		{
			name:      "associate single alias",
			aliases:   []string{"myfs.example.com"},
			wantCount: 1,
			wantCode:  http.StatusOK,
			lifecycle: "AVAILABLE",
		},
		{
			name:      "associate multiple aliases",
			aliases:   []string{"a.example.com", "b.example.com"},
			wantCount: 2,
			wantCode:  http.StatusOK,
			lifecycle: "AVAILABLE",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			fsID := createFS(t, h, "WINDOWS")

			rec := doFSxRequest(t, h, "AssociateFileSystemAliases", map[string]any{
				"FileSystemId": fsID,
				"Aliases":      tc.aliases,
			})
			require.Equal(t, tc.wantCode, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			aliases := out["Aliases"].([]any)
			assert.Len(t, aliases, tc.wantCount)
			assert.Equal(t, tc.lifecycle, aliases[0].(map[string]any)["Lifecycle"])
		})
	}
}

func TestFSx_AliasesLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("describe returns associated aliases", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		fsID := createFS(t, h, "WINDOWS")

		doFSxRequest(t, h, "AssociateFileSystemAliases", map[string]any{
			"FileSystemId": fsID,
			"Aliases":      []string{"x.example.com"},
		})

		rec := doFSxRequest(t, h, "DescribeFileSystemAliases", map[string]any{"FileSystemId": fsID})
		require.Equal(t, http.StatusOK, rec.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Len(t, out["Aliases"].([]any), 1)
	})

	t.Run("disassociate removes alias", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		fsID := createFS(t, h, "WINDOWS")

		doFSxRequest(t, h, "AssociateFileSystemAliases", map[string]any{
			"FileSystemId": fsID,
			"Aliases":      []string{"x.example.com"},
		})

		rec := doFSxRequest(t, h, "DisassociateFileSystemAliases", map[string]any{
			"FileSystemId": fsID,
			"Aliases":      []string{"x.example.com"},
		})
		require.Equal(t, http.StatusOK, rec.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		aliases := out["Aliases"].([]any)
		assert.Len(t, aliases, 1)
		assert.Equal(t, "DELETING", aliases[0].(map[string]any)["Lifecycle"])

		rec2 := doFSxRequest(t, h, "DescribeFileSystemAliases", map[string]any{"FileSystemId": fsID})
		var out2 map[string]any
		require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
		assert.Len(t, out2["Aliases"].([]any), 0)
	})
}

// ---------------------------------------------------------------------------
// Data Repository Associations
// ---------------------------------------------------------------------------

func TestFSx_DataRepositoryAssociation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
		fsPath   string
		repoPath string
	}{
		{
			name:     "create returns AssociationId",
			wantCode: http.StatusOK,
			fsPath:   "/data",
			repoPath: "s3://my-bucket/prefix",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			fsID := createFS(t, h, "LUSTRE")

			rec := doFSxRequest(t, h, "CreateDataRepositoryAssociation", map[string]any{
				"FileSystemId":       fsID,
				"FileSystemPath":     tc.fsPath,
				"DataRepositoryPath": tc.repoPath,
			})
			require.Equal(t, tc.wantCode, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			a := out["Association"].(map[string]any)
			assert.Contains(t, a["AssociationId"].(string), "dra-")
			assert.Equal(t, tc.fsPath, a["FileSystemPath"])
			assert.Equal(t, tc.repoPath, a["DataRepositoryPath"])
		})
	}
}

func TestFSx_DataRepositoryAssociationLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("describe/update/delete cycle", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		fsID := createFS(t, h, "LUSTRE")
		b := fsx.GetBackend(h)

		rec := doFSxRequest(t, h, "CreateDataRepositoryAssociation", map[string]any{
			"FileSystemId":       fsID,
			"FileSystemPath":     "/data",
			"DataRepositoryPath": "s3://bucket/prefix",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		var cr map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
		id := cr["Association"].(map[string]any)["AssociationId"].(string)

		assert.Equal(t, 1, fsx.DRACount(b))

		// update
		rec2 := doFSxRequest(t, h, "UpdateDataRepositoryAssociation", map[string]any{
			"AssociationId":      id,
			"DataRepositoryPath": "s3://bucket/new",
		})
		require.Equal(t, http.StatusOK, rec2.Code)
		var ur map[string]any
		require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &ur))
		assert.Equal(t, "s3://bucket/new", ur["Association"].(map[string]any)["DataRepositoryPath"])

		// describe
		rec3 := doFSxRequest(t, h, "DescribeDataRepositoryAssociations", map[string]any{
			"AssociationIds": []string{id},
		})
		require.Equal(t, http.StatusOK, rec3.Code)
		var dr map[string]any
		require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &dr))
		assert.Len(t, dr["Associations"].([]any), 1)

		// delete
		rec4 := doFSxRequest(t, h, "DeleteDataRepositoryAssociation", map[string]any{"AssociationId": id})
		require.Equal(t, http.StatusOK, rec4.Code)
		assert.Equal(t, 0, fsx.DRACount(b))
	})
}

// ---------------------------------------------------------------------------
// Data Repository Tasks
// ---------------------------------------------------------------------------

func TestFSx_DataRepositoryTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		taskType string
		wantCode int
	}{
		{
			name:     "create EXPORT_TO_REPOSITORY task",
			taskType: "EXPORT_TO_REPOSITORY",
			wantCode: http.StatusOK,
		},
		{
			name:     "create IMPORT_METADATA_FROM_REPOSITORY task",
			taskType: "IMPORT_METADATA_FROM_REPOSITORY",
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			fsID := createFS(t, h, "LUSTRE")

			rec := doFSxRequest(t, h, "CreateDataRepositoryTask", map[string]any{
				"FileSystemId": fsID,
				"Type":         tc.taskType,
				"Paths":        []string{"/data"},
			})
			require.Equal(t, tc.wantCode, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			task := out["DataRepositoryTask"].(map[string]any)
			assert.Contains(t, task["TaskId"].(string), "task-")
			assert.Equal(t, "EXECUTING", task["Lifecycle"])
		})
	}
}

func TestFSx_DataRepositoryTaskLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("describe and cancel cycle", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		fsID := createFS(t, h, "LUSTRE")

		rec := doFSxRequest(t, h, "CreateDataRepositoryTask", map[string]any{
			"FileSystemId": fsID,
			"Type":         "EXPORT_TO_REPOSITORY",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		var cr map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
		taskID := cr["DataRepositoryTask"].(map[string]any)["TaskId"].(string)

		// describe
		rec2 := doFSxRequest(t, h, "DescribeDataRepositoryTasks", map[string]any{
			"TaskIds": []string{taskID},
		})
		require.Equal(t, http.StatusOK, rec2.Code)
		var dr map[string]any
		require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &dr))
		assert.Len(t, dr["DataRepositoryTasks"].([]any), 1)

		// cancel
		rec3 := doFSxRequest(t, h, "CancelDataRepositoryTask", map[string]any{"TaskId": taskID})
		require.Equal(t, http.StatusOK, rec3.Code)
		var car map[string]any
		require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &car))
		assert.Equal(t, "CANCELING", car["Lifecycle"])
	})
}

// ---------------------------------------------------------------------------
// File Caches
// ---------------------------------------------------------------------------

func TestFSx_FileCache(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cacheType string
		capacity  int
		wantCode  int
		wantErr   bool
	}{
		{
			name:      "create LUSTRE cache",
			cacheType: "LUSTRE",
			capacity:  1200,
			wantCode:  http.StatusOK,
		},
		{
			name:     "missing cache type returns 400",
			wantCode: http.StatusBadRequest,
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			body := map[string]any{"StorageCapacityGiB": tc.capacity}
			if tc.cacheType != "" {
				body["FileCacheType"] = tc.cacheType
			}

			rec := doFSxRequest(t, h, "CreateFileCache", body)
			require.Equal(t, tc.wantCode, rec.Code)

			if !tc.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				c := out["FileCache"].(map[string]any)
				assert.Contains(t, c["FileCacheId"].(string), "fc-")
				assert.Equal(t, "AVAILABLE", c["Lifecycle"])
			}
		})
	}
}

func TestFSx_FileCacheLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("describe/update/delete cycle", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		b := fsx.GetBackend(h)
		fcID := createFileCache(t, h, "LUSTRE")

		assert.Equal(t, 1, fsx.FileCacheCount(b))

		// describe by id
		rec := doFSxRequest(t, h, "DescribeFileCaches", map[string]any{
			"FileCacheIds": []string{fcID},
		})
		require.Equal(t, http.StatusOK, rec.Code)
		var dr map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &dr))
		assert.Len(t, dr["FileCaches"].([]any), 1)

		// update
		rec2 := doFSxRequest(t, h, "UpdateFileCache", map[string]any{
			"FileCacheId":        fcID,
			"StorageCapacityGiB": 2400,
		})
		require.Equal(t, http.StatusOK, rec2.Code)
		var ur map[string]any
		require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &ur))
		assert.Equal(t, float64(2400), ur["FileCache"].(map[string]any)["StorageCapacityGiB"])

		// delete
		rec3 := doFSxRequest(t, h, "DeleteFileCache", map[string]any{"FileCacheId": fcID})
		require.Equal(t, http.StatusOK, rec3.Code)
		var del map[string]any
		require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &del))
		assert.Equal(t, fcID, del["FileCacheId"])
		assert.Equal(t, "DELETING", del["Lifecycle"])
		assert.Equal(t, 0, fsx.FileCacheCount(b))
	})
}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

func TestFSx_Snapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
		wantErr  bool
	}{
		{
			name:     "create snapshot returns SnapshotId",
			wantCode: http.StatusOK,
		},
		{
			name:     "missing VolumeId returns 400",
			wantCode: http.StatusBadRequest,
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			var body map[string]any
			if !tc.wantErr {
				fsID := createFS(t, h, "ONTAP")
				volID := createVolume(t, h, fsID, "ONTAP", "vol1")
				body = map[string]any{"VolumeId": volID, "Name": "snap1"}
			} else {
				body = map[string]any{"Name": "snap1"}
			}

			rec := doFSxRequest(t, h, "CreateSnapshot", body)
			require.Equal(t, tc.wantCode, rec.Code)

			if !tc.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				s := out["Snapshot"].(map[string]any)
				assert.Contains(t, s["SnapshotId"].(string), "fsvolsnap-")
				assert.Equal(t, "AVAILABLE", s["Lifecycle"])
				assert.Equal(t, "snap1", s["Name"])
			}
		})
	}
}

func TestFSx_SnapshotLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("describe/update/delete cycle", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		b := fsx.GetBackend(h)
		fsID := createFS(t, h, "ONTAP")
		volID := createVolume(t, h, fsID, "ONTAP", "vol1")

		rec := doFSxRequest(t, h, "CreateSnapshot", map[string]any{
			"VolumeId": volID,
			"Name":     "snap1",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		var cr map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
		snapID := cr["Snapshot"].(map[string]any)["SnapshotId"].(string)

		assert.Equal(t, 1, fsx.SnapshotCount(b))

		// describe
		rec2 := doFSxRequest(t, h, "DescribeSnapshots", map[string]any{
			"SnapshotIds": []string{snapID},
		})
		require.Equal(t, http.StatusOK, rec2.Code)
		var dr map[string]any
		require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &dr))
		assert.Len(t, dr["Snapshots"].([]any), 1)

		// update
		rec3 := doFSxRequest(t, h, "UpdateSnapshot", map[string]any{
			"SnapshotId": snapID,
			"Name":       "snap-renamed",
		})
		require.Equal(t, http.StatusOK, rec3.Code)
		var ur map[string]any
		require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &ur))
		assert.Equal(t, "snap-renamed", ur["Snapshot"].(map[string]any)["Name"])

		// delete
		rec4 := doFSxRequest(t, h, "DeleteSnapshot", map[string]any{"SnapshotId": snapID})
		require.Equal(t, http.StatusOK, rec4.Code)
		var del map[string]any
		require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &del))
		assert.Equal(t, snapID, del["SnapshotId"])
		assert.Equal(t, "DELETING", del["Lifecycle"])
		assert.Equal(t, 0, fsx.SnapshotCount(b))
	})
}

// ---------------------------------------------------------------------------
// Storage Virtual Machines
// ---------------------------------------------------------------------------

func TestFSx_StorageVirtualMachine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svmName  string
		wantCode int
		wantErr  bool
	}{
		{
			name:     "create SVM returns StorageVirtualMachineId",
			svmName:  "svm1",
			wantCode: http.StatusOK,
		},
		{
			name:     "missing FileSystemId returns 400",
			wantCode: http.StatusBadRequest,
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			var body map[string]any
			if !tc.wantErr {
				fsID := createFS(t, h, "ONTAP")
				body = map[string]any{"FileSystemId": fsID, "Name": tc.svmName}
			} else {
				body = map[string]any{"Name": "svm1"}
			}

			rec := doFSxRequest(t, h, "CreateStorageVirtualMachine", body)
			require.Equal(t, tc.wantCode, rec.Code)

			if !tc.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				svm := out["StorageVirtualMachine"].(map[string]any)
				assert.Contains(t, svm["StorageVirtualMachineId"].(string), "svm-")
				assert.Equal(t, "AVAILABLE", svm["Lifecycle"])
				assert.Equal(t, tc.svmName, svm["Name"])
			}
		})
	}
}

func TestFSx_SVMLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("describe/update/delete cycle", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		b := fsx.GetBackend(h)
		fsID := createFS(t, h, "ONTAP")
		svmID := createSVM(t, h, fsID, "svm1")

		assert.Equal(t, 1, fsx.SVMCount(b))

		// describe
		rec := doFSxRequest(t, h, "DescribeStorageVirtualMachines", map[string]any{
			"StorageVirtualMachineIds": []string{svmID},
		})
		require.Equal(t, http.StatusOK, rec.Code)
		var dr map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &dr))
		assert.Len(t, dr["StorageVirtualMachines"].([]any), 1)

		// update
		rec2 := doFSxRequest(t, h, "UpdateStorageVirtualMachine", map[string]any{
			"StorageVirtualMachineId": svmID,
			"Subtype":                 "DP_DESTINATION",
		})
		require.Equal(t, http.StatusOK, rec2.Code)
		var ur map[string]any
		require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &ur))
		assert.Equal(t, "DP_DESTINATION", ur["StorageVirtualMachine"].(map[string]any)["Subtype"])

		// delete
		rec3 := doFSxRequest(t, h, "DeleteStorageVirtualMachine", map[string]any{
			"StorageVirtualMachineId": svmID,
		})
		require.Equal(t, http.StatusOK, rec3.Code)
		var del map[string]any
		require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &del))
		assert.Equal(t, svmID, del["StorageVirtualMachineId"])
		assert.Equal(t, "DELETING", del["Lifecycle"])
		assert.Equal(t, 0, fsx.SVMCount(b))
	})
}

// ---------------------------------------------------------------------------
// Volumes
// ---------------------------------------------------------------------------

func TestFSx_Volume(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		volType  string
		volName  string
		wantCode int
		wantErr  bool
	}{
		{
			name:     "create ONTAP volume",
			volType:  "ONTAP",
			volName:  "vol1",
			wantCode: http.StatusOK,
		},
		{
			name:     "create OPENZFS volume",
			volType:  "OPENZFS",
			volName:  "vol2",
			wantCode: http.StatusOK,
		},
		{
			name:     "missing VolumeType returns 400",
			wantCode: http.StatusBadRequest,
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			var body map[string]any
			if !tc.wantErr {
				fsID := createFS(t, h, "ONTAP")
				body = map[string]any{
					"VolumeType":   tc.volType,
					"FileSystemId": fsID,
					"Name":         tc.volName,
				}
			} else {
				body = map[string]any{"Name": "vol1"}
			}

			rec := doFSxRequest(t, h, "CreateVolume", body)
			require.Equal(t, tc.wantCode, rec.Code)

			if !tc.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				v := out["Volume"].(map[string]any)
				assert.Contains(t, v["VolumeId"].(string), "fsvol-")
				assert.Equal(t, "AVAILABLE", v["Lifecycle"])
				assert.Equal(t, tc.volType, v["VolumeType"])
			}
		})
	}
}

func TestFSx_VolumeLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("describe/update/delete cycle", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		b := fsx.GetBackend(h)
		fsID := createFS(t, h, "ONTAP")
		volID := createVolume(t, h, fsID, "ONTAP", "vol1")

		assert.Equal(t, 1, fsx.VolumeCount(b))

		// describe
		rec := doFSxRequest(t, h, "DescribeVolumes", map[string]any{
			"VolumeIds": []string{volID},
		})
		require.Equal(t, http.StatusOK, rec.Code)
		var dr map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &dr))
		assert.Len(t, dr["Volumes"].([]any), 1)

		// update
		rec2 := doFSxRequest(t, h, "UpdateVolume", map[string]any{
			"VolumeId": volID,
			"Name":     "vol-renamed",
		})
		require.Equal(t, http.StatusOK, rec2.Code)
		var ur map[string]any
		require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &ur))
		assert.Equal(t, "vol-renamed", ur["Volume"].(map[string]any)["Name"])

		// delete
		rec3 := doFSxRequest(t, h, "DeleteVolume", map[string]any{"VolumeId": volID})
		require.Equal(t, http.StatusOK, rec3.Code)
		var del map[string]any
		require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &del))
		assert.Equal(t, volID, del["VolumeId"])
		assert.Equal(t, "DELETING", del["Lifecycle"])
		assert.Equal(t, 0, fsx.VolumeCount(b))
	})
}

func TestFSx_CreateVolumeFromBackup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
		wantErr  bool
	}{
		{
			name:     "creates volume from backup",
			wantCode: http.StatusOK,
		},
		{
			name:     "unknown backup returns 400",
			wantCode: http.StatusBadRequest,
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			var backupID string
			if !tc.wantErr {
				backupID = createFSandBackup(t, h, "ONTAP")
			} else {
				backupID = "backup-does-not-exist"
			}

			rec := doFSxRequest(t, h, "CreateVolumeFromBackup", map[string]any{
				"BackupId": backupID,
				"Name":     "restored-vol",
			})
			require.Equal(t, tc.wantCode, rec.Code)

			if !tc.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				v := out["Volume"].(map[string]any)
				assert.Contains(t, v["VolumeId"].(string), "fsvol-")
				assert.Equal(t, "restored-vol", v["Name"])
			}
		})
	}
}

func TestFSx_RestoreVolumeFromSnapshot(t *testing.T) {
	t.Parallel()

	t.Run("restore volume from snapshot", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		fsID := createFS(t, h, "ONTAP")
		volID := createVolume(t, h, fsID, "ONTAP", "vol1")

		// create snapshot
		rec := doFSxRequest(t, h, "CreateSnapshot", map[string]any{
			"VolumeId": volID,
			"Name":     "snap1",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		var cr map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
		snapID := cr["Snapshot"].(map[string]any)["SnapshotId"].(string)

		// restore
		rec2 := doFSxRequest(t, h, "RestoreVolumeFromSnapshot", map[string]any{
			"VolumeId":   volID,
			"SnapshotId": snapID,
		})
		require.Equal(t, http.StatusOK, rec2.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out))
		assert.Equal(t, volID, out["Volume"].(map[string]any)["VolumeId"])
	})
}

func TestFSx_CopySnapshotAndUpdateVolume(t *testing.T) {
	t.Parallel()

	t.Run("copy snapshot and update volume", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		fsID := createFS(t, h, "ONTAP")
		volID := createVolume(t, h, fsID, "ONTAP", "vol1")

		rec := doFSxRequest(t, h, "CreateSnapshot", map[string]any{
			"VolumeId": volID,
			"Name":     "snap1",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		var cr map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
		snapARN := cr["Snapshot"].(map[string]any)["ResourceARN"].(string)

		rec2 := doFSxRequest(t, h, "CopySnapshotAndUpdateVolume", map[string]any{
			"VolumeId":          volID,
			"SourceSnapshotARN": snapARN,
		})
		require.Equal(t, http.StatusOK, rec2.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out))
		assert.Equal(t, volID, out["Volume"].(map[string]any)["VolumeId"])
	})
}

// ---------------------------------------------------------------------------
// S3 Access Points
// ---------------------------------------------------------------------------

func TestFSx_S3AccessPoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		apName   string
		wantCode int
		wantErr  bool
	}{
		{
			name:     "create access point",
			apName:   "my-ap",
			wantCode: http.StatusOK,
		},
		{
			name:     "missing Name returns 400",
			wantCode: http.StatusBadRequest,
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			var body map[string]any
			if !tc.wantErr {
				fsID := createFS(t, h, "ONTAP")
				body = map[string]any{"Name": tc.apName, "FileSystemId": fsID}
			} else {
				fsID := createFS(t, h, "ONTAP")
				body = map[string]any{"FileSystemId": fsID}
			}

			rec := doFSxRequest(t, h, "CreateAndAttachS3AccessPoint", body)
			require.Equal(t, tc.wantCode, rec.Code)

			if !tc.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				ap := out["S3AccessPoint"].(map[string]any)
				assert.Equal(t, tc.apName, ap["Name"])
				assert.Equal(t, "AVAILABLE", ap["Lifecycle"])
			}
		})
	}
}

func TestFSx_S3AccessPointLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("describe/detach-delete cycle", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		fsID := createFS(t, h, "ONTAP")

		// create
		doFSxRequest(t, h, "CreateAndAttachS3AccessPoint", map[string]any{
			"Name":         "my-ap",
			"FileSystemId": fsID,
		})

		// describe
		rec := doFSxRequest(t, h, "DescribeS3AccessPointAttachments", map[string]any{
			"Names": []string{"my-ap"},
		})
		require.Equal(t, http.StatusOK, rec.Code)
		var dr map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &dr))
		assert.Len(t, dr["S3AccessPoints"].([]any), 1)

		// detach
		rec2 := doFSxRequest(t, h, "DetachAndDeleteS3AccessPoint", map[string]any{
			"Name":         "my-ap",
			"FileSystemId": fsID,
		})
		require.Equal(t, http.StatusOK, rec2.Code)

		// describe empty
		rec3 := doFSxRequest(t, h, "DescribeS3AccessPointAttachments", map[string]any{})
		require.Equal(t, http.StatusOK, rec3.Code)
		var dr2 map[string]any
		require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &dr2))
		assert.Len(t, dr2["S3AccessPoints"].([]any), 0)
	})
}

// ---------------------------------------------------------------------------
// Shared VPC Configuration
// ---------------------------------------------------------------------------

func TestFSx_SharedVpcConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		enable      string
		wantEnabled string
		wantCode    int
	}{
		{
			name:        "default is false",
			enable:      "",
			wantEnabled: "false",
			wantCode:    http.StatusOK,
		},
		{
			name:        "update to true",
			enable:      "true",
			wantEnabled: "true",
			wantCode:    http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			if tc.enable != "" {
				rec := doFSxRequest(t, h, "UpdateSharedVpcConfiguration", map[string]any{
					"EnableSharedVpcOnFileSystemCreation": tc.enable,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doFSxRequest(t, h, "DescribeSharedVpcConfiguration", map[string]any{})
			require.Equal(t, tc.wantCode, rec.Code)
			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tc.wantEnabled, out["EnableSharedVpcOnFileSystemCreation"])
		})
	}
}

// ---------------------------------------------------------------------------
// Miscellaneous
// ---------------------------------------------------------------------------

func TestFSx_ReleaseFileSystemNfsV3Locks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		fsType   string
		wantCode int
		wantErr  bool
	}{
		{
			name:     "release locks on existing file system",
			fsType:   "ONTAP",
			wantCode: http.StatusOK,
		},
		{
			name:     "unknown file system returns 400",
			wantCode: http.StatusBadRequest,
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			var fsID string
			if !tc.wantErr {
				fsID = createFS(t, h, tc.fsType)
			} else {
				fsID = "fs-does-not-exist"
			}

			rec := doFSxRequest(t, h, "ReleaseFileSystemNfsV3Locks", map[string]any{
				"FileSystemId": fsID,
			})
			require.Equal(t, tc.wantCode, rec.Code)

			if !tc.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotNil(t, out["FileSystem"])
			}
		})
	}
}

func TestFSx_StartMisconfiguredStateRecovery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		fsType   string
		wantCode int
		wantErr  bool
	}{
		{
			name:     "start recovery on existing file system",
			fsType:   "LUSTRE",
			wantCode: http.StatusOK,
		},
		{
			name:     "unknown file system returns 400",
			wantCode: http.StatusBadRequest,
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			var fsID string
			if !tc.wantErr {
				fsID = createFS(t, h, tc.fsType)
			} else {
				fsID = "fs-does-not-exist"
			}

			rec := doFSxRequest(t, h, "StartMisconfiguredStateRecovery", map[string]any{
				"FileSystemId": fsID,
			})
			require.Equal(t, tc.wantCode, rec.Code)

			if !tc.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotNil(t, out["FileSystem"])
			}
		})
	}
}
