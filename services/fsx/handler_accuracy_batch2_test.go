package fsx_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestFSx_DeleteBackup_Response(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantLC   string
		wantCode int
	}{
		{
			name:     "returns BackupId and DELETED lifecycle",
			wantCode: http.StatusOK,
			wantLC:   "DELETED",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			bkID := createFSandBackup(t, h, "WINDOWS")

			rec := doFSxRequest(t, h, "DeleteBackup", map[string]any{"BackupId": bkID})
			require.Equal(t, tc.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, bkID, resp["BackupId"])
			assert.Equal(t, tc.wantLC, resp["Lifecycle"])
		})
	}
}

func TestFSx_BackupNotFound_ErrorCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		op       string
		wantType string
		wantCode int
	}{
		{
			name:     "DeleteBackup unknown id returns BackupNotFound code",
			op:       "DeleteBackup",
			body:     map[string]any{"BackupId": "backup-notexist"},
			wantCode: http.StatusBadRequest,
			wantType: "BackupNotFound",
		},
		{
			name:     "DescribeBackups unknown id returns BackupNotFound code",
			op:       "DescribeBackups",
			body:     map[string]any{"BackupIds": []string{"backup-notexist"}},
			wantCode: http.StatusBadRequest,
			wantType: "BackupNotFound",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			rec := doFSxRequest(t, h, tc.op, tc.body)
			require.Equal(t, tc.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tc.wantType, resp["__type"])
		})
	}
}

func TestFSx_TagValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantType string
		tags     []map[string]string
		wantCode int
	}{
		{
			name:     "empty key rejected",
			tags:     []map[string]string{{"Key": "", "Value": "v"}},
			wantCode: http.StatusBadRequest,
			wantType: "BadRequest",
		},
		{
			name:     "key over 128 chars rejected",
			tags:     []map[string]string{{"Key": strings.Repeat("x", 129), "Value": "v"}},
			wantCode: http.StatusBadRequest,
			wantType: "BadRequest",
		},
		{
			name:     "aws: prefix key rejected",
			tags:     []map[string]string{{"Key": "aws:reserved", "Value": "v"}},
			wantCode: http.StatusBadRequest,
			wantType: "BadRequest",
		},
		{
			name:     "value over 256 chars rejected",
			tags:     []map[string]string{{"Key": "k", "Value": strings.Repeat("x", 257)}},
			wantCode: http.StatusBadRequest,
			wantType: "BadRequest",
		},
		{
			name:     "valid 128-char key accepted",
			tags:     []map[string]string{{"Key": strings.Repeat("k", 128), "Value": "v"}},
			wantCode: http.StatusOK,
		},
		{
			name:     "valid 256-char value accepted",
			tags:     []map[string]string{{"Key": "k", "Value": strings.Repeat("v", 256)}},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			rec := doFSxRequest(t, h, "CreateFileSystem", map[string]any{"FileSystemType": "LUSTRE"})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			arn := out["FileSystem"].(map[string]any)["ResourceARN"].(string)

			rec = doFSxRequest(t, h, "TagResource", map[string]any{
				"ResourceARN": arn,
				"Tags":        tc.tags,
			})
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.wantType != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tc.wantType, resp["__type"])
			}
		})
	}
}

func TestFSx_TagValidation_OnCreate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantType string
		tags     []map[string]string
		wantCode int
	}{
		{
			name:     "aws: prefix key rejected at CreateFileSystem",
			tags:     []map[string]string{{"Key": "aws:bad", "Value": "v"}},
			wantCode: http.StatusBadRequest,
			wantType: "BadRequest",
		},
		{
			name:     "key too long rejected at CreateFileSystem",
			tags:     []map[string]string{{"Key": strings.Repeat("k", 129), "Value": "v"}},
			wantCode: http.StatusBadRequest,
			wantType: "BadRequest",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			rec := doFSxRequest(t, h, "CreateFileSystem", map[string]any{
				"FileSystemType": "LUSTRE",
				"Tags":           tc.tags,
			})
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.wantType != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tc.wantType, resp["__type"])
			}
		})
	}
}

func TestFSx_TagLimit(t *testing.T) {
	t.Parallel()

	t.Run("51st tag returns ServiceLimitExceeded", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)

		rec := doFSxRequest(t, h, "CreateFileSystem", map[string]any{"FileSystemType": "LUSTRE"})
		require.Equal(t, http.StatusOK, rec.Code)

		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

		arn := out["FileSystem"].(map[string]any)["ResourceARN"].(string)

		// Add 50 tags (should succeed).
		tags := make([]map[string]string, 50)
		for i := range tags {
			tags[i] = map[string]string{"Key": "k" + strings.Repeat("x", i), "Value": "v"}
		}

		rec = doFSxRequest(t, h, "TagResource", map[string]any{"ResourceARN": arn, "Tags": tags})
		require.Equal(t, http.StatusOK, rec.Code)

		// Adding one more unique key must fail.
		rec = doFSxRequest(t, h, "TagResource", map[string]any{
			"ResourceARN": arn,
			"Tags":        []map[string]string{{"Key": "overflow", "Value": "v"}},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "ServiceLimitExceeded", resp["__type"])
	})

	t.Run("updating existing key does not count toward limit", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)

		rec := doFSxRequest(t, h, "CreateFileSystem", map[string]any{"FileSystemType": "LUSTRE"})
		require.Equal(t, http.StatusOK, rec.Code)

		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

		arn := out["FileSystem"].(map[string]any)["ResourceARN"].(string)

		// Add 50 tags.
		tags := make([]map[string]string, 50)
		for i := range tags {
			tags[i] = map[string]string{"Key": "k" + strings.Repeat("x", i), "Value": "v"}
		}

		rec = doFSxRequest(t, h, "TagResource", map[string]any{"ResourceARN": arn, "Tags": tags})
		require.Equal(t, http.StatusOK, rec.Code)

		// Updating an existing key (same key as tags[0]) must succeed.
		rec = doFSxRequest(t, h, "TagResource", map[string]any{
			"ResourceARN": arn,
			"Tags":        []map[string]string{{"Key": tags[0]["Key"], "Value": "updated"}},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	})
}
