package fsx_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fsx"
)

func TestFSx_Tags(t *testing.T) {
	t.Parallel()

	fsARN := func(t *testing.T, h *fsx.Handler) string {
		t.Helper()
		rec := doFSxRequest(t, h, "CreateFileSystem", map[string]any{"FileSystemType": "LUSTRE"})
		require.Equal(t, http.StatusOK, rec.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

		return out["FileSystem"].(map[string]any)["ResourceARN"].(string)
	}

	t.Run("TagResource adds tags", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		arn := fsARN(t, h)
		rec := doFSxRequest(t, h, "TagResource", map[string]any{
			"ResourceARN": arn,
			"Tags":        []map[string]string{{"Key": "env", "Value": "test"}},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("ListTagsForResource returns tags", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		arn := fsARN(t, h)
		doFSxRequest(t, h, "TagResource", map[string]any{
			"ResourceARN": arn,
			"Tags":        []map[string]string{{"Key": "Name", "Value": "myfs"}},
		})
		rec := doFSxRequest(t, h, "ListTagsForResource", map[string]any{"ResourceARN": arn})
		require.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		tags := resp["Tags"].([]any)
		assert.Len(t, tags, 1)
		tag := tags[0].(map[string]any)
		assert.Equal(t, "Name", tag["Key"])
		assert.Equal(t, "myfs", tag["Value"])
	})

	t.Run("UntagResource removes tag", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		arn := fsARN(t, h)
		doFSxRequest(t, h, "TagResource", map[string]any{
			"ResourceARN": arn,
			"Tags":        []map[string]string{{"Key": "env", "Value": "prod"}, {"Key": "Name", "Value": "x"}},
		})
		doFSxRequest(t, h, "UntagResource", map[string]any{"ResourceARN": arn, "TagKeys": []string{"env"}})
		rec := doFSxRequest(t, h, "ListTagsForResource", map[string]any{"ResourceARN": arn})
		require.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		tags := resp["Tags"].([]any)
		assert.Len(t, tags, 1)
		assert.Equal(t, "Name", tags[0].(map[string]any)["Key"])
	})

	t.Run("TagResource unknown arn returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doFSxRequest(t, h, "TagResource", map[string]any{
			"ResourceARN": "arn:aws:fsx:us-east-1:000000000000:file-system/fs-notexist",
			"Tags":        []map[string]string{{"Key": "k", "Value": "v"}},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
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

	t.Run("51st tag returns BadRequest", func(t *testing.T) {
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
		// TagResource's own switch (fsx@v1.68.4 deserializers.go
		// deserializeOpErrorTagResource) does not declare
		// ServiceLimitExceeded; BadRequest is the correct wire type
		// (gopherstack-6flj/uox6 error-envelope sweep).
		assert.Equal(t, "BadRequest", resp["__type"])
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

// TestListTagsForResource_EmptyIsArray verifies that ListTagsForResource
// returns a JSON array (not null) when no tags are set on a resource.
// Real AWS FSx always returns "Tags": [] for resources with no tags.
func TestListTagsForResource_EmptyIsArray(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createFunc func(h *fsx.Handler) string
		name       string
	}{
		{
			name: "file_system_no_tags",
			createFunc: func(h *fsx.Handler) string {
				rec := doFSxRequest(t, h, "CreateFileSystem", map[string]any{"FileSystemType": "LUSTRE"})
				require.Equal(t, http.StatusOK, rec.Code)
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				return out["FileSystem"].(map[string]any)["ResourceARN"].(string)
			},
		},
		{
			name: "backup_no_tags",
			createFunc: func(h *fsx.Handler) string {
				fsID := createFS(t, h, "LUSTRE")
				rec := doFSxRequest(t, h, "CreateBackup", map[string]any{"FileSystemId": fsID})
				require.Equal(t, http.StatusOK, rec.Code)
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				return out["Backup"].(map[string]any)["ResourceARN"].(string)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourceARN := tc.createFunc(h)

			rec := doFSxRequest(t, h, "ListTagsForResource", map[string]any{"ResourceARN": resourceARN})
			require.Equal(t, http.StatusOK, rec.Code)

			// Parse the raw JSON to verify "Tags" is an array, not null.
			var raw map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

			tagsRaw, ok := raw["Tags"]
			require.True(t, ok, "Tags key must be present")
			assert.Equal(t, "[]", string(tagsRaw), "Tags must be JSON array [], not null")
		})
	}
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
