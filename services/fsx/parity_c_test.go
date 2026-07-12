package fsx_test

// Parity batch-C: CreationTime wire-format fix (epoch-seconds JSON number,
// not RFC3339 string) across every FSx resource type, and the generic
// ResourceNotFound error code for Tag/Untag/ListTagsForResource on an
// unrecognized ARN.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fsx"
)

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

// Test_TagOps_UnknownARN_ReturnsResourceNotFound verifies that TagResource,
// UntagResource, and ListTagsForResource return the generic ResourceNotFound
// error (not the file-system-specific FileSystemNotFound) when given an ARN
// that does not match any known FSx resource. Real FSx's TagResource family
// operates across every resource type and returns the generic
// types.ResourceNotFound exception for an unrecognized ResourceARN.
func Test_TagOps_UnknownARN_ReturnsResourceNotFound(t *testing.T) {
	t.Parallel()

	const unknownARN = "arn:aws:fsx:us-east-1:000000000000:backup/backup-doesnotexist"

	tests := []struct {
		body map[string]any
		name string
		op   string
	}{
		{
			name: "TagResource",
			op:   "TagResource",
			body: map[string]any{
				"ResourceARN": unknownARN,
				"Tags":        []map[string]string{{"Key": "k", "Value": "v"}},
			},
		},
		{
			name: "UntagResource",
			op:   "UntagResource",
			body: map[string]any{"ResourceARN": unknownARN, "TagKeys": []string{"k"}},
		},
		{
			name: "ListTagsForResource",
			op:   "ListTagsForResource",
			body: map[string]any{"ResourceARN": unknownARN},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doFSxRequest(t, h, tt.op, tt.body)

			require.Equal(t, http.StatusBadRequest, rec.Code)

			var errBody map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errBody))
			assert.Equal(t, "ResourceNotFound", errBody["__type"],
				"%s on an unknown ARN must return the generic ResourceNotFound code", tt.op)
		})
	}
}
