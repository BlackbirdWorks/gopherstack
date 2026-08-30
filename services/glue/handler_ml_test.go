package glue_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func TestMLTransform_CRUD(t *testing.T) {
	t.Parallel()

	createBody := map[string]any{
		"Name":        "my-transform",
		"Description": "test transform",
		"Role":        "arn:aws:iam::123456789012:role/GlueRole",
		"InputRecordTables": []map[string]any{
			{"DatabaseName": "db1", "TableName": "t1"},
		},
		"Parameters": map[string]any{
			"TransformType": "FIND_MATCHES",
			"FindMatchesParameters": map[string]any{
				"PrimaryKeyColumnName": "id",
			},
		},
	}

	tests := []struct {
		setup    func(h interface{ Name() string })
		body     func(transformID string) map[string]any
		name     string
		op       string
		wantCode int
	}{
		{
			name:     "create",
			op:       "CreateMLTransform",
			body:     func(_ string) map[string]any { return createBody },
			wantCode: http.StatusOK,
		},
		{
			name:     "get",
			op:       "GetMLTransform",
			body:     func(id string) map[string]any { return map[string]any{"TransformId": id} },
			wantCode: http.StatusOK,
		},
		{
			name:     "get-missing",
			op:       "GetMLTransform",
			body:     func(_ string) map[string]any { return map[string]any{"TransformId": "no-such-id"} },
			wantCode: http.StatusBadRequest,
		},
		{
			name: "update",
			op:   "UpdateMLTransform",
			body: func(id string) map[string]any {
				return map[string]any{"TransformId": id, "Description": "updated"}
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "get-all",
			op:       "GetMLTransforms",
			body:     func(_ string) map[string]any { return map[string]any{} },
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			var transformID string

			createRec := doGlueRequest(t, h, "CreateMLTransform", createBody)
			if createRec.Code == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &out))
				transformID, _ = out["TransformId"].(string)
			}

			rec := doGlueRequest(t, h, tt.op, tt.body(transformID))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestMLTransform_Delete(t *testing.T) {
	t.Parallel()

	t.Run("delete-found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createRec := doGlueRequest(t, h, "CreateMLTransform", map[string]any{
			"Name": "ml-del", "Role": "arn:aws:iam::123:role/R",
		})
		require.Equal(t, http.StatusOK, createRec.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &out))
		transformID := out["TransformId"].(string)

		rec := doGlueRequest(t, h, "DeleteMLTransform", map[string]any{"TransformId": transformID})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("delete-missing", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doGlueRequest(t, h, "DeleteMLTransform", map[string]any{"TransformId": "no-such"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestMLTransform_TaskRunStubs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	t.Run("get-ml-task-run", func(t *testing.T) {
		t.Parallel()
		rec := doGlueRequest(t, h, "GetMLTaskRun", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "SUCCEEDED")
	})

	t.Run("get-ml-task-runs", func(t *testing.T) {
		t.Parallel()
		rec := doGlueRequest(t, h, "GetMLTaskRuns", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "TaskRuns")
	})

	t.Run("start-export-labels-task-run", func(t *testing.T) {
		t.Parallel()
		rec := doGlueRequest(t, h, "StartExportLabelsTaskRun", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "TaskRunId")
	})

	t.Run("start-import-labels-task-run", func(t *testing.T) {
		t.Parallel()
		rec := doGlueRequest(t, h, "StartImportLabelsTaskRun", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("start-ml-evaluation-task-run", func(t *testing.T) {
		t.Parallel()
		rec := doGlueRequest(t, h, "StartMLEvaluationTaskRun", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("start-ml-labeling-set-generation-task-run", func(t *testing.T) {
		t.Parallel()
		rec := doGlueRequest(t, h, "StartMLLabelingSetGenerationTaskRun", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("cancel-ml-task-run-empty-ids-rejected", func(t *testing.T) {
		t.Parallel()
		rec := doGlueRequest(t, h, "CancelMLTaskRun", map[string]any{})
		require.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

// TestMLTransform tests MLTransform CRUD.
func TestMLTransform(t *testing.T) {
	t.Parallel()
	h := newGlueHandler(t)

	// CreateMLTransform
	out := dispatchNewOp(t, h, "CreateMLTransform", map[string]any{
		"Name":        "my-transform",
		"Description": "test transform",
		"Role":        "arn:aws:iam::123456789012:role/GlueRole",
		"InputRecordTables": []any{
			map[string]any{"DatabaseName": "mydb", "TableName": "mytable"},
		},
		"Parameters": map[string]any{"TransformType": "FIND_MATCHES"},
	})
	transformID, _ := out["TransformId"].(string)
	if transformID == "" {
		t.Fatalf("expected TransformId, got: %v", out)
	}

	// GetMLTransform
	out2 := dispatchNewOp(t, h, "GetMLTransform", map[string]any{"TransformId": transformID})
	if out2["Name"] != "my-transform" {
		t.Errorf("Name mismatch: %v", out2["Name"])
	}

	// GetMLTransforms
	out3 := dispatchNewOp(t, h, "GetMLTransforms", map[string]any{})
	transforms, _ := out3["Transforms"].([]any)
	if len(transforms) != 1 {
		t.Errorf("expected 1 transform, got %d", len(transforms))
	}

	// UpdateMLTransform
	dispatchNewOp(t, h, "UpdateMLTransform", map[string]any{
		"TransformId": transformID,
		"Name":        "my-transform",
		"Description": "updated transform",
	})

	// DeleteMLTransform
	dispatchNewOp(t, h, "DeleteMLTransform", map[string]any{"TransformId": transformID})

	// Verify deletion
	dispatchNewOpExpectError(t, h, "GetMLTransform", map[string]any{"TransformId": transformID})
}

// createTestMLTransform is a helper that creates an ML transform and returns its ID.
func createTestMLTransform(t *testing.T, h *glue.Handler, name string) string {
	t.Helper()

	rec := doGlueRequest(t, h, "CreateMLTransform", map[string]any{
		"Name": name,
		"Role": "arn:aws:iam::000000000000:role/GlueRole",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		TransformID string `json:"TransformId"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotEmpty(t, out.TransformID)

	return out.TransformID
}

// TestMLEvaluationTaskRun exercises the ML evaluation task run lifecycle.
func TestMLEvaluationTaskRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setupFn     func(t *testing.T, h *glue.Handler) (transformID string)
		transformID string // override if not using setup
		wantCode    int
	}{
		{
			name: "unknown_transform_returns_400",
			setupFn: func(_ *testing.T, _ *glue.Handler) string {
				return "no-such-transform"
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "known_transform_returns_200",
			setupFn: func(t *testing.T, h *glue.Handler) string {
				t.Helper()

				return createTestMLTransform(t, h, "eval-transform")
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			transformID := tc.setupFn(t, h)

			rec := doGlueRequest(t, h, "StartMLEvaluationTaskRun", map[string]any{
				"TransformId": transformID,
			})
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// TestMLLabelingSetGenerationTaskRun exercises the labeling set generation task run.
func TestMLLabelingSetGenerationTaskRun(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	transformID := createTestMLTransform(t, h, "label-transform")

	rec := doGlueRequest(t, h, "StartMLLabelingSetGenerationTaskRun", map[string]any{
		"TransformId": transformID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		TaskRunID string `json:"TaskRunId"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out.TaskRunID)
}

// TestMLExportImportLabels exercises export/import label task runs.
func TestMLExportImportLabels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		extraInput map[string]any
		name       string
		action     string
	}{
		{
			name:       "export_labels",
			action:     "StartExportLabelsTaskRun",
			extraInput: map[string]any{"OutputS3Path": "s3://bucket/labels/"},
		},
		{
			name:       "import_labels",
			action:     "StartImportLabelsTaskRun",
			extraInput: map[string]any{"InputS3Path": "s3://bucket/import/"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			transformID := createTestMLTransform(t, h, tc.name+"-transform")

			input := map[string]any{"TransformId": transformID}
			maps.Copy(input, tc.extraInput)

			rec := doGlueRequest(t, h, tc.action, input)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				TaskRunID string `json:"TaskRunId"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.NotEmpty(t, out.TaskRunID)
		})
	}
}

// TestGetMLTaskRun exercises GetMLTaskRun with various inputs.
func TestGetMLTaskRun(t *testing.T) {
	t.Parallel()

	t.Run("empty_ids_returns_ok", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doGlueRequest(t, h, "GetMLTaskRun", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			Status string `json:"Status"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Equal(t, "SUCCEEDED", out.Status)
	})

	t.Run("unknown_run_returns_400", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doGlueRequest(t, h, "GetMLTaskRun", map[string]any{
			"TransformId": "t-missing",
			"TaskRunId":   "r-missing",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("lifecycle_start_get_cancel", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		transformID := createTestMLTransform(t, h, "lifecycle-transform")

		startRec := doGlueRequest(t, h, "StartMLEvaluationTaskRun", map[string]any{
			"TransformId": transformID,
		})
		require.Equal(t, http.StatusOK, startRec.Code)

		var startOut struct {
			TaskRunID string `json:"TaskRunId"`
		}
		require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))

		getRec := doGlueRequest(t, h, "GetMLTaskRun", map[string]any{
			"TransformId": transformID,
			"TaskRunId":   startOut.TaskRunID,
		})
		require.Equal(t, http.StatusOK, getRec.Code)

		var getOut struct {
			TransformID string `json:"TransformId"`
			TaskRunID   string `json:"TaskRunId"`
			Status      string `json:"Status"`
		}
		require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
		assert.Equal(t, transformID, getOut.TransformID)
		assert.Equal(t, startOut.TaskRunID, getOut.TaskRunID)
		assert.Equal(t, "RUNNING", getOut.Status)

		cancelRec := doGlueRequest(t, h, "CancelMLTaskRun", map[string]any{
			"TransformId": transformID,
			"TaskRunId":   startOut.TaskRunID,
		})
		require.Equal(t, http.StatusOK, cancelRec.Code)

		getRec2 := doGlueRequest(t, h, "GetMLTaskRun", map[string]any{
			"TransformId": transformID,
			"TaskRunId":   startOut.TaskRunID,
		})
		require.Equal(t, http.StatusOK, getRec2.Code)

		var getOut2 struct {
			Status string `json:"Status"`
		}
		require.NoError(t, json.Unmarshal(getRec2.Body.Bytes(), &getOut2))
		assert.Equal(t, "STOPPED", getOut2.Status)
	})
}

// TestGetMLTaskRuns exercises GetMLTaskRuns listing.
func TestGetMLTaskRuns(t *testing.T) {
	t.Parallel()

	t.Run("empty_transform_id_returns_empty_list", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doGlueRequest(t, h, "GetMLTaskRuns", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			TaskRuns []any `json:"TaskRuns"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Empty(t, out.TaskRuns)
	})

	t.Run("multiple_runs_returned", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		transformID := createTestMLTransform(t, h, "multi-run-transform")

		for range 3 {
			rec := doGlueRequest(t, h, "StartMLEvaluationTaskRun", map[string]any{
				"TransformId": transformID,
			})
			require.Equal(t, http.StatusOK, rec.Code)
		}

		rec := doGlueRequest(t, h, "GetMLTaskRuns", map[string]any{
			"TransformId": transformID,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			TaskRuns []any `json:"TaskRuns"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Len(t, out.TaskRuns, 3)
	})
}

// TestGetMLTaskRuns_SDKPagination_TotalOrderNoTiesLost drives the real
// aws-sdk-go-v2 client. GetMLTaskRunsInput carries real MaxResults/NextToken
// query members (api_op_GetMLTaskRuns.go) that the handler previously never
// declared or read at all, so every call returned the full unpaginated set.
// All runs here start within the same wall-clock second (StartedOn is a
// whole-second epoch value), the same tie-prone-sort precondition already
// fixed for five other glue listings (gopherstack-6nr4) -- the union of
// every page must reproduce the seeded set exactly, with no drops or
// duplicates from ties landing on a page boundary.
func TestGetMLTaskRuns_SDKPagination_TotalOrderNoTiesLost(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestGlueClient(t, h)
	transformID := createTestMLTransform(t, h, "paginated-transform")

	const numRuns = 6

	wantIDs := make(map[string]bool, numRuns)

	for range numRuns {
		rec := doGlueRequest(t, h, "StartMLEvaluationTaskRun", map[string]any{
			"TransformId": transformID,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			TaskRunID string `json:"TaskRunId"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		require.NotEmpty(t, out.TaskRunID)
		wantIDs[out.TaskRunID] = true
	}

	require.Len(t, wantIDs, numRuns, "task run IDs must be unique")

	gotIDs := make(map[string]bool)

	input := &gluesdk.GetMLTaskRunsInput{
		TransformId: aws.String(transformID),
		MaxResults:  aws.Int32(2),
	}

	for pages := 0; ; pages++ {
		require.Less(t, pages, 10, "pagination did not terminate")

		out, err := client.GetMLTaskRuns(t.Context(), input)
		require.NoError(t, err)
		require.LessOrEqual(t, len(out.TaskRuns), 2, "must honor MaxResults")

		for _, r := range out.TaskRuns {
			require.NotNil(t, r.TaskRunId)
			gotIDs[*r.TaskRunId] = true
		}

		if out.NextToken == nil || *out.NextToken == "" {
			break
		}

		input.NextToken = out.NextToken
	}

	assert.Equal(t, wantIDs, gotIDs,
		"paginated union must equal the seeded set exactly despite same-second StartedOn ties")
}

// TestGetMLTaskRuns_SDKFilter_ByStatus proves Filter.Status (real
// TaskRunFilterCriteria.Status) is honored, not silently dropped.
func TestGetMLTaskRuns_SDKFilter_ByStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestGlueClient(t, h)
	transformID := createTestMLTransform(t, h, "filtered-transform")

	rec := doGlueRequest(t, h, "StartMLEvaluationTaskRun", map[string]any{"TransformId": transformID})
	require.Equal(t, http.StatusOK, rec.Code)

	var started struct {
		TaskRunID string `json:"TaskRunId"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &started))

	cancelRec := doGlueRequest(t, h, "CancelMLTaskRun", map[string]any{
		"TransformId": transformID,
		"TaskRunId":   started.TaskRunID,
	})
	require.Equal(t, http.StatusOK, cancelRec.Code)

	rec2 := doGlueRequest(t, h, "StartMLEvaluationTaskRun", map[string]any{"TransformId": transformID})
	require.Equal(t, http.StatusOK, rec2.Code)

	out, err := client.GetMLTaskRuns(t.Context(), &gluesdk.GetMLTaskRunsInput{
		TransformId: aws.String(transformID),
		Filter:      &types.TaskRunFilterCriteria{Status: types.TaskStatusTypeStopped},
	})
	require.NoError(t, err)
	require.Len(t, out.TaskRuns, 1, "Filter.Status must exclude the RUNNING run")
	assert.Equal(t, started.TaskRunID, aws.ToString(out.TaskRuns[0].TaskRunId))
	assert.Equal(t, types.TaskStatusTypeStopped, out.TaskRuns[0].Status)
}

// TestCancelMLTaskRun exercises CancelMLTaskRun error cases.
func TestCancelMLTaskRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "both_ids_empty_returns_400",
			input:    map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "unknown_run_returns_400",
			input:    map[string]any{"TransformId": "t-unknown", "TaskRunId": "r-unknown"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "CancelMLTaskRun", tc.input)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// TestListMLTransforms_Stateful verifies ListMLTransforms returns real IDs.
func TestListMLTransforms_Stateful(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		createCount int
		wantLen     int
	}{
		{name: "empty", createCount: 0, wantLen: 0},
		{name: "one_transform", createCount: 1, wantLen: 1},
		{name: "three_transforms", createCount: 3, wantLen: 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			for i := range tc.createCount {
				createTestMLTransform(t, h, "transform-"+string(rune('a'+i)))
			}

			rec := doGlueRequest(t, h, "ListMLTransforms", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				TransformIDs []string `json:"TransformIds"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.TransformIDs, tc.wantLen)
		})
	}
}
