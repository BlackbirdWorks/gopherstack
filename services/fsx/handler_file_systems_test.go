package fsx_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fsx"
)

func TestFSx_FileSystem(t *testing.T) {
	t.Parallel()

	t.Run("CreateFileSystem returns id and arn", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doFSxRequest(
			t,
			h,
			"CreateFileSystem",
			map[string]any{"FileSystemType": "LUSTRE", "StorageCapacity": 1200},
		)
		require.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		fs := resp["FileSystem"].(map[string]any)
		assert.Contains(t, fs["FileSystemId"].(string), "fs-")
		assert.Contains(t, fs["ResourceARN"].(string), "arn:aws:fsx:")
		assert.Equal(t, "AVAILABLE", fs["Lifecycle"])
		assert.Equal(t, "LUSTRE", fs["FileSystemType"])
	})

	t.Run("CreateFileSystem missing type returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doFSxRequest(t, h, "CreateFileSystem", map[string]any{})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("DescribeFileSystems returns all file systems", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		createFS(t, h, "WINDOWS")
		createFS(t, h, "LUSTRE")
		rec := doFSxRequest(t, h, "DescribeFileSystems", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		fss := resp["FileSystems"].([]any)
		assert.Len(t, fss, 2)
	})

	t.Run("DescribeFileSystems by id returns specific file system", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		id := createFS(t, h, "OPENZFS")
		createFS(t, h, "WINDOWS")
		rec := doFSxRequest(t, h, "DescribeFileSystems", map[string]any{"FileSystemIds": []string{id}})
		require.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		fss := resp["FileSystems"].([]any)
		require.Len(t, fss, 1)
		assert.Equal(t, id, fss[0].(map[string]any)["FileSystemId"])
	})

	t.Run("DescribeFileSystems unknown id returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doFSxRequest(t, h, "DescribeFileSystems", map[string]any{"FileSystemIds": []string{"fs-notexist"}})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("DeleteFileSystem returns 200", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		id := createFS(t, h, "LUSTRE")
		rec := doFSxRequest(t, h, "DeleteFileSystem", map[string]any{"FileSystemId": id})
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, 0, fsx.FileSystemCount(fsx.GetBackend(h)))
	})

	t.Run("DeleteFileSystem unknown id returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doFSxRequest(t, h, "DeleteFileSystem", map[string]any{"FileSystemId": "fs-notexist"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("UpdateFileSystem returns updated FileSystem", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		id := createFS(t, h, "WINDOWS")
		rec := doFSxRequest(t, h, "UpdateFileSystem", map[string]any{"FileSystemId": id, "StorageCapacity": 64})
		require.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		fs := resp["FileSystem"].(map[string]any)
		assert.Equal(t, id, fs["FileSystemId"])
	})

	t.Run("UpdateFileSystem unknown id returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doFSxRequest(t, h, "UpdateFileSystem", map[string]any{"FileSystemId": "fs-notexist"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

// TestCreateFileSystem_FileSystemTypeValidation verifies that
// CreateFileSystem rejects unknown FileSystemType values with a 400 error.
// Real AWS FSx accepts only LUSTRE, WINDOWS, ONTAP, and OPENZFS; the emulator
// previously accepted any non-empty string.
func TestCreateFileSystem_FileSystemTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		fsType   string
		capacity int
		wantCode int
	}{
		{
			name:     "invalid_type_rejected",
			fsType:   "INVALID",
			capacity: 1200,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "lowercase_rejected",
			fsType:   "lustre",
			capacity: 1200,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "LUSTRE_accepted",
			fsType:   "LUSTRE",
			capacity: 1200,
			wantCode: http.StatusOK,
		},
		{
			name:     "WINDOWS_accepted",
			fsType:   "WINDOWS",
			capacity: 32,
			wantCode: http.StatusOK,
		},
		{
			name:     "ONTAP_accepted",
			fsType:   "ONTAP",
			capacity: 1024,
			wantCode: http.StatusOK,
		},
		{
			name:     "OPENZFS_accepted",
			fsType:   "OPENZFS",
			capacity: 64,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := fileSystemCreateBody(tt.fsType)
			body["StorageCapacity"] = tt.capacity
			rec := doFSxRequest(t, h, "CreateFileSystem", body)

			assert.Equal(t, tt.wantCode, rec.Code, "FileSystemType=%q", tt.fsType)
		})
	}
}

// TestCreateFileSystem_StorageCapacityMinimum verifies that CreateFileSystem
// enforces minimum storage capacity per file system type.
// Real AWS FSx rejects below-minimum values with a ValidationError.
func TestCreateFileSystem_StorageCapacityMinimum(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		fsType   string
		capacity int
		wantCode int
	}{
		{
			name:     "lustre_below_minimum_rejected",
			fsType:   "LUSTRE",
			capacity: 1199,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "lustre_at_minimum_accepted",
			fsType:   "LUSTRE",
			capacity: 1200,
			wantCode: http.StatusOK,
		},
		{
			name:     "windows_below_minimum_rejected",
			fsType:   "WINDOWS",
			capacity: 31,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "windows_at_minimum_accepted",
			fsType:   "WINDOWS",
			capacity: 32,
			wantCode: http.StatusOK,
		},
		{
			name:     "openzfs_below_minimum_rejected",
			fsType:   "OPENZFS",
			capacity: 63,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "openzfs_at_minimum_accepted",
			fsType:   "OPENZFS",
			capacity: 64,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := fileSystemCreateBody(tt.fsType)
			body["StorageCapacity"] = tt.capacity
			rec := doFSxRequest(t, h, "CreateFileSystem", body)

			assert.Equal(t, tt.wantCode, rec.Code,
				"FileSystemType=%q StorageCapacity=%d", tt.fsType, tt.capacity)
		})
	}
}

func TestFSx_DeleteFileSystem_Response(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		fsType   string
		wantLC   string
		wantCode int
		wantID   bool
	}{
		{
			name:     "returns FileSystemId and DELETING lifecycle",
			fsType:   "LUSTRE",
			wantCode: http.StatusOK,
			wantID:   true,
			wantLC:   "DELETING",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			id := createFS(t, h, tc.fsType)

			rec := doFSxRequest(t, h, "DeleteFileSystem", map[string]any{"FileSystemId": id})
			require.Equal(t, tc.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			if tc.wantID {
				assert.Equal(t, id, resp["FileSystemId"])
			}

			assert.Equal(t, tc.wantLC, resp["Lifecycle"])
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
		lifecycle string
		aliases   []string
		wantCount int
		wantCode  int
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
		assert.Empty(t, out2["Aliases"].([]any))
	})
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
