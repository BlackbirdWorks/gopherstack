package fsx_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// TestDataRepositoryTask_TagsStoredAtCreation verifies that tags passed
// to CreateDataRepositoryTask are persisted and retrievable via ListTagsForResource.
// Previously CreateDataRepositoryTask did not populate b.tags[arn], so creation-time
// tags were silently dropped.
func TestDataRepositoryTask_TagsStoredAtCreation(t *testing.T) {
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

// TestDataRepositoryTask_TagResource verifies that TagResource works on
// DataRepositoryTask ARNs. Previously arnExists() did not check DRT ARNs,
// causing TagResource to return FileSystemNotFound for task ARNs.
func TestDataRepositoryTask_TagResource(t *testing.T) {
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
