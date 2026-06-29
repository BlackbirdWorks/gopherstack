package fsx_test

// Parity batch-B: fixes for DataRepositoryTask tag storage, arnExists DRT check,
// and ListTagsForResource returning [] instead of null for empty tag sets.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fsx"
)

// TestParity_DataRepositoryTask_TagsStoredAtCreation verifies that tags passed
// to CreateDataRepositoryTask are persisted and retrievable via ListTagsForResource.
// Previously CreateDataRepositoryTask did not populate b.tags[arn], so creation-time
// tags were silently dropped.
func TestParity_DataRepositoryTask_TagsStoredAtCreation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tags []map[string]string
	}{
		{
			name: "single_tag",
			tags: []map[string]string{{"Key": "env", "Value": "test"}},
		},
		{
			name: "multiple_tags",
			tags: []map[string]string{
				{"Key": "env", "Value": "prod"},
				{"Key": "team", "Value": "data"},
			},
		},
		{
			name: "no_tags",
			tags: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			fsID := createFS(t, h, "LUSTRE")

			body := map[string]any{
				"FileSystemId": fsID,
				"Type":         "EXPORT_TO_REPOSITORY",
			}
			if tc.tags != nil {
				body["Tags"] = tc.tags
			}

			rec := doFSxRequest(t, h, "CreateDataRepositoryTask", body)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			task := out["DataRepositoryTask"].(map[string]any)
			taskARN := task["ResourceARN"].(string)
			require.NotEmpty(t, taskARN)

			// Tags must be retrievable via ListTagsForResource.
			rec2 := doFSxRequest(t, h, "ListTagsForResource", map[string]any{"ResourceARN": taskARN})
			require.Equal(t, http.StatusOK, rec2.Code, "ListTagsForResource on DRT must succeed")

			var tagOut map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &tagOut))

			tags, ok := tagOut["Tags"].([]any)
			require.True(t, ok, "Tags must be a JSON array")
			assert.Len(t, tags, len(tc.tags))
		})
	}
}

// TestParity_DataRepositoryTask_TagResource verifies that TagResource works on
// DataRepositoryTask ARNs. Previously arnExists() did not check DRT ARNs,
// causing TagResource to return FileSystemNotFound for task ARNs.
func TestParity_DataRepositoryTask_TagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	fsID := createFS(t, h, "LUSTRE")

	rec := doFSxRequest(t, h, "CreateDataRepositoryTask", map[string]any{
		"FileSystemId": fsID,
		"Type":         "EXPORT_TO_REPOSITORY",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	taskARN := out["DataRepositoryTask"].(map[string]any)["ResourceARN"].(string)

	// TagResource on a DRT ARN must succeed.
	rec2 := doFSxRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": taskARN,
		"Tags":        []map[string]string{{"Key": "added", "Value": "after"}},
	})
	assert.Equal(t, http.StatusOK, rec2.Code, "TagResource on DRT must succeed")

	// Verify the tag is now visible.
	rec3 := doFSxRequest(t, h, "ListTagsForResource", map[string]any{"ResourceARN": taskARN})
	require.Equal(t, http.StatusOK, rec3.Code)

	var tagOut map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &tagOut))
	tags := tagOut["Tags"].([]any)
	require.Len(t, tags, 1)
	assert.Equal(t, "added", tags[0].(map[string]any)["Key"])
}

// TestParity_ListTagsForResource_EmptyIsArray verifies that ListTagsForResource
// returns a JSON array (not null) when no tags are set on a resource.
// Real AWS FSx always returns "Tags": [] for resources with no tags.
func TestParity_ListTagsForResource_EmptyIsArray(t *testing.T) {
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
