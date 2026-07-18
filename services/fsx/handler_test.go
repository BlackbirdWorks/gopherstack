package fsx_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fsx"
)

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

func newTestHandler(t *testing.T) *fsx.Handler {
	t.Helper()
	backend := fsx.NewInMemoryBackend("000000000000", "us-east-1")

	return fsx.NewHandler(backend)
}

func doFSxRequest(t *testing.T, h *fsx.Handler, op string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var marshalErr error

		bodyBytes, marshalErr = json.Marshal(body)
		require.NoError(t, marshalErr)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSSimbaAPIService_v20180301."+op)
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	handlerErr := h.Handler()(c)
	require.NoError(t, handlerErr)

	return rec
}

func createFS(t *testing.T, h *fsx.Handler, fsType string) string {
	t.Helper()
	rec := doFSxRequest(t, h, "CreateFileSystem", map[string]any{"FileSystemType": fsType})
	require.Equal(t, http.StatusOK, rec.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out["FileSystem"].(map[string]any)["FileSystemId"].(string)
}

func createFSandBackup(t *testing.T, h *fsx.Handler, fsType string) string {
	t.Helper()
	fsID := createFS(t, h, fsType)
	rec := doFSxRequest(t, h, "CreateBackup", map[string]any{"FileSystemId": fsID})
	require.Equal(t, http.StatusOK, rec.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out["Backup"].(map[string]any)["BackupId"].(string)
}

func createVolume(t *testing.T, h *fsx.Handler, fsID, volType, name string) string { //nolint:unparam // existing issue.
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

// decodeField requires a 200 response and returns the named top-level field
// decoded as a JSON object.
func decodeField(t *testing.T, rec *httptest.ResponseRecorder, field string) map[string]any {
	t.Helper()

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	obj, ok := out[field].(map[string]any)
	require.True(t, ok, "response must contain object field %q, got %v", field, out)

	return obj
}

// ---------------------------------------------------------------------------
// Core dispatch tests
// ---------------------------------------------------------------------------

func TestFSx_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doFSxRequest(t, h, "CreateVolume", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// Test_CreationTime_IsEpochSecondsNumber verifies that every FSx resource's
// CreationTime field serializes as a JSON number (epoch seconds), matching
// the real aws-sdk-go-v2/service/fsx deserializer, which hard-fails
// ("expected CreationTime to be a JSON Number, got string instead") on an
// RFC3339 string. Previously DataRepositoryAssociation, DataRepositoryTask,
// FileCache, Snapshot, StorageVirtualMachine, Volume, and S3AccessPoint used
// a bare time.Time field, which Go's encoding/json renders as an RFC3339
// string.
func Test_CreationTime_IsEpochSecondsNumber(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(t *testing.T, h *fsx.Handler) map[string]any
		name   string
		field  string
	}{
		{
			name:  "FileSystem",
			field: "FileSystem",
			create: func(t *testing.T, h *fsx.Handler) map[string]any {
				t.Helper()

				return decodeField(t, doFSxRequest(t, h, "CreateFileSystem",
					map[string]any{"FileSystemType": "LUSTRE"}), "FileSystem")
			},
		},
		{
			name:  "Backup",
			field: "Backup",
			create: func(t *testing.T, h *fsx.Handler) map[string]any {
				t.Helper()
				fsID := createFS(t, h, "LUSTRE")

				return decodeField(t, doFSxRequest(t, h, "CreateBackup",
					map[string]any{"FileSystemId": fsID}), "Backup")
			},
		},
		{
			name:  "DataRepositoryAssociation",
			field: "Association",
			create: func(t *testing.T, h *fsx.Handler) map[string]any {
				t.Helper()
				fsID := createFS(t, h, "LUSTRE")

				return decodeField(t, doFSxRequest(t, h, "CreateDataRepositoryAssociation", map[string]any{
					"FileSystemId":       fsID,
					"FileSystemPath":     "/data",
					"DataRepositoryPath": "s3://bucket/prefix",
				}), "Association")
			},
		},
		{
			name:  "DataRepositoryTask",
			field: "DataRepositoryTask",
			create: func(t *testing.T, h *fsx.Handler) map[string]any {
				t.Helper()
				fsID := createFS(t, h, "LUSTRE")

				return decodeField(t, doFSxRequest(t, h, "CreateDataRepositoryTask", map[string]any{
					"FileSystemId": fsID,
					"Type":         "EXPORT_TO_REPOSITORY",
				}), "DataRepositoryTask")
			},
		},
		{
			name:  "FileCache",
			field: "FileCache",
			create: func(t *testing.T, h *fsx.Handler) map[string]any {
				t.Helper()

				return decodeField(t, doFSxRequest(t, h, "CreateFileCache",
					map[string]any{"FileCacheType": "LUSTRE"}), "FileCache")
			},
		},
		{
			name:  "Snapshot",
			field: "Snapshot",
			create: func(t *testing.T, h *fsx.Handler) map[string]any {
				t.Helper()
				volID := decodeField(t, doFSxRequest(t, h, "CreateVolume",
					map[string]any{"VolumeType": "ONTAP", "Name": "vol1"}), "Volume")["VolumeId"].(string)

				return decodeField(t, doFSxRequest(t, h, "CreateSnapshot", map[string]any{
					"VolumeId": volID,
					"Name":     "snap1",
				}), "Snapshot")
			},
		},
		{
			name:  "StorageVirtualMachine",
			field: "StorageVirtualMachine",
			create: func(t *testing.T, h *fsx.Handler) map[string]any {
				t.Helper()
				fsID := createFS(t, h, "ONTAP")

				return decodeField(t, doFSxRequest(t, h, "CreateStorageVirtualMachine", map[string]any{
					"FileSystemId": fsID,
					"Name":         "svm1",
				}), "StorageVirtualMachine")
			},
		},
		{
			name:  "Volume",
			field: "Volume",
			create: func(t *testing.T, h *fsx.Handler) map[string]any {
				t.Helper()

				return decodeField(t, doFSxRequest(t, h, "CreateVolume",
					map[string]any{"VolumeType": "ONTAP", "Name": "vol2"}), "Volume")
			},
		},
		{
			name:  "S3AccessPoint",
			field: "S3AccessPoint",
			create: func(t *testing.T, h *fsx.Handler) map[string]any {
				t.Helper()
				fsID := createFS(t, h, "LUSTRE")

				return decodeField(t, doFSxRequest(t, h, "CreateAndAttachS3AccessPoint", map[string]any{
					"Name":         "ap1",
					"FileSystemId": fsID,
				}), "S3AccessPoint")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resource := tt.create(t, h)

			raw, ok := resource["CreationTime"]
			require.True(t, ok, "%s response must include CreationTime", tt.field)

			switch v := raw.(type) {
			case float64:
				assert.Positive(t, v, "CreationTime must be a positive epoch-seconds number")
			default:
				t.Fatalf("%s.CreationTime must decode as a JSON number (epoch seconds), got %T: %v",
					tt.field, raw, raw)
			}
		})
	}
}
