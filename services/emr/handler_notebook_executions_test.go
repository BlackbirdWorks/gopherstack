package emr_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emr"
)

func TestNotebookExecution(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Start a notebook execution.
	startRec := doEMRRequest(t, h, "StartNotebookExecution", map[string]any{
		"EditorId":              "e-EXAMPLEEDITORID",
		"NotebookExecutionName": "test-run",
		"NotebookParams":        `{"key":"value"}`,
		"ExecutionEngine": map[string]any{
			"Id": "j-EXAMPLECLUSTERID",
		},
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startOut struct {
		NotebookExecutionID string `json:"NotebookExecutionId"`
	}
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
	assert.NotEmpty(t, startOut.NotebookExecutionID)

	// Describe it.
	descRec := doEMRRequest(t, h, "DescribeNotebookExecution", map[string]any{
		"NotebookExecutionId": startOut.NotebookExecutionID,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut struct {
		NotebookExecution struct {
			NotebookExecutionID   string `json:"NotebookExecutionId"`
			Status                string `json:"Status"`
			NotebookExecutionName string `json:"NotebookExecutionName"`
		} `json:"NotebookExecution"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	assert.Equal(t, startOut.NotebookExecutionID, descOut.NotebookExecution.NotebookExecutionID)
	assert.Equal(t, "RUNNING", descOut.NotebookExecution.Status)
	assert.Equal(t, "test-run", descOut.NotebookExecution.NotebookExecutionName)

	// List it.
	listRec := doEMRRequest(t, h, "ListNotebookExecutions", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut struct {
		NotebookExecutions []struct {
			NotebookExecutionID string `json:"NotebookExecutionId"`
			Status              string `json:"Status"`
		} `json:"NotebookExecutions"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	require.Len(t, listOut.NotebookExecutions, 1)
	assert.Equal(t, startOut.NotebookExecutionID, listOut.NotebookExecutions[0].NotebookExecutionID)

	// Stop it.
	stopRec := doEMRRequest(t, h, "StopNotebookExecution", map[string]any{
		"NotebookExecutionId": startOut.NotebookExecutionID,
	})
	assert.Equal(t, http.StatusOK, stopRec.Code)

	// Describe again - should be STOPPED.
	descRec2 := doEMRRequest(t, h, "DescribeNotebookExecution", map[string]any{
		"NotebookExecutionId": startOut.NotebookExecutionID,
	})
	require.Equal(t, http.StatusOK, descRec2.Code)

	var descOut2 struct {
		NotebookExecution struct {
			Status string `json:"Status"`
		} `json:"NotebookExecution"`
	}
	require.NoError(t, json.Unmarshal(descRec2.Body.Bytes(), &descOut2))
	assert.Equal(t, "STOPPED", descOut2.NotebookExecution.Status)
}

func TestNotebookExecution_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doEMRRequest(t, h, "DescribeNotebookExecution", map[string]any{
		"NotebookExecutionId": "ex-doesnotexist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestNotebookExecution_ListFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Start two executions with different editor IDs.
	for range 3 {
		doEMRRequest(t, h, "StartNotebookExecution", map[string]any{
			"EditorId":              "e-EDITOR1",
			"NotebookExecutionName": "run1",
			"ExecutionEngine":       map[string]any{"Id": "j-1"},
		})
	}
	doEMRRequest(t, h, "StartNotebookExecution", map[string]any{
		"EditorId":              "e-EDITOR2",
		"NotebookExecutionName": "run2",
		"ExecutionEngine":       map[string]any{"Id": "j-2"},
	})

	// Filter by EditorId.
	rec := doEMRRequest(t, h, "ListNotebookExecutions", map[string]any{
		"EditorId": "e-EDITOR1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		NotebookExecutions []struct {
			NotebookExecutionID string `json:"NotebookExecutionId"`
		} `json:"NotebookExecutions"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Len(t, out.NotebookExecutions, 3)
}

// TestNotebookExecution_ListNoParamsOrTagsLeak asserts the raw JSON body of
// a ListNotebookExecutions item has no "NotebookParams" or "Tags" key --
// types.NotebookExecutionSummary (emr@v1.64.4 types.go:2161) has neither
// member; only the full NotebookExecution (DescribeNotebookExecution) does.
// An SDK client silently drops the unrecognized keys, so only a raw-body
// assertion catches the leak.
func TestNotebookExecution_ListNoParamsOrTagsLeak(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doEMRRequest(t, h, "StartNotebookExecution", map[string]any{
		"EditorId":              "e-ED1",
		"NotebookExecutionName": "leak-run",
		"NotebookParams":        `{"key":"value"}`,
		"ExecutionEngine":       map[string]any{"Id": "j-1"},
		"Tags":                  []any{map[string]any{"Key": "env", "Value": "test"}},
	})

	rec := doEMRRequest(t, h, "ListNotebookExecutions", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw struct {
		NotebookExecutions []map[string]any `json:"NotebookExecutions"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
	require.Len(t, raw.NotebookExecutions, 1)

	item := raw.NotebookExecutions[0]
	_, hasParams := item["NotebookParams"]
	assert.False(
		t,
		hasParams,
		"ListNotebookExecutions item leaked NotebookParams; real NotebookExecutionSummary has no such member",
	)
	_, hasTags := item["Tags"]
	assert.False(
		t,
		hasTags,
		"ListNotebookExecutions item leaked Tags; real NotebookExecutionSummary has no such member",
	)
	assert.Contains(t, item, "Status", "NotebookExecutionSummary must still emit Status")
	assert.Contains(t, item, "NotebookExecutionId", "NotebookExecutionSummary must still emit NotebookExecutionId")
}

func TestNotebookExecution_Persistence(t *testing.T) {
	t.Parallel()

	src := emr.NewInMemoryBackend(testAccountID, testRegion)
	ne, err := src.StartNotebookExecution(context.Background(), "e-ED1", "persist-run", "{}", "j-1", nil)
	require.NoError(t, err)

	snap := src.Snapshot(t.Context())
	require.NotNil(t, snap)

	dst := emr.NewInMemoryBackend("", "")
	require.NoError(t, dst.Restore(t.Context(), snap))

	restored, err := dst.DescribeNotebookExecution(context.Background(), ne.NotebookExecutionID)
	require.NoError(t, err)
	assert.Equal(t, "persist-run", restored.NotebookExecutionName)
	assert.Equal(t, "RUNNING", restored.Status)
}

func TestNotebookExecution_TagsEmptyNotAbsent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "no tags returns Tags:[]",
			body: map[string]any{
				"EditorId":              "e-ED1",
				"NotebookExecutionName": "no-tag-run",
				"ExecutionEngine":       map[string]any{"Id": "j-1"},
			},
		},
		{
			name: "empty tags returns Tags:[]",
			body: map[string]any{
				"EditorId":              "e-ED2",
				"NotebookExecutionName": "empty-tag-run",
				"ExecutionEngine":       map[string]any{"Id": "j-2"},
				"Tags":                  []any{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			startRec := doEMRRequest(t, h, "StartNotebookExecution", tt.body)
			require.Equal(t, http.StatusOK, startRec.Code)

			var start struct {
				NotebookExecutionID string `json:"NotebookExecutionId"`
			}
			require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &start))

			rec := doEMRRequest(t, h, "DescribeNotebookExecution", map[string]any{
				"NotebookExecutionId": start.NotebookExecutionID,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var raw map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

			ne, ok := raw["NotebookExecution"].(map[string]any)
			require.True(t, ok, "NotebookExecution must be an object")

			tags, hasKey := ne["Tags"]
			assert.True(t, hasKey, "DescribeNotebookExecution must include 'Tags' key even when empty")
			assert.IsType(t, []any{}, tags, "'Tags' must be an array, not null or absent")
			assert.Empty(t, tags, "'Tags' must be [] not populated")
		})
	}
}
