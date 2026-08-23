package codepipeline_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

func TestHandler_PipelineExecution_TerminalStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run        func(t *testing.T, h *codepipeline.Handler, execID string) *httptest.ResponseRecorder
		name       string
		wantStatus string
		wantType   string
		httpStatus int
	}{
		{
			name: "GetPipelineExecution reaches Succeeded after Start",
			run: func(t *testing.T, h *codepipeline.Handler, execID string) *httptest.ResponseRecorder {
				t.Helper()

				return doRequest(t, h, "GetPipelineExecution", map[string]any{
					"pipelineName":        "term-pipeline",
					"pipelineExecutionId": execID,
				})
			},
			httpStatus: http.StatusOK,
			wantStatus: "Succeeded",
		},
		{
			// StopPipelineExecutionOutput carries no status field on the
			// wire (matching real AWS), so the terminal status is verified
			// via a follow-up GetPipelineExecution instead.
			name: "StopPipelineExecution reaches Stopped",
			run: func(t *testing.T, h *codepipeline.Handler, execID string) *httptest.ResponseRecorder {
				t.Helper()

				stopRec := doRequest(t, h, "StopPipelineExecution", map[string]any{
					"pipelineName":        "term-pipeline",
					"pipelineExecutionId": execID,
					"reason":              "manual stop",
					"abandon":             true,
				})
				require.Equal(t, http.StatusOK, stopRec.Code)

				return doRequest(t, h, "GetPipelineExecution", map[string]any{
					"pipelineName":        "term-pipeline",
					"pipelineExecutionId": execID,
				})
			},
			httpStatus: http.StatusOK,
			wantStatus: "Stopped",
		},
		{
			name: "GetPipelineExecution on unknown ID returns PipelineExecutionNotFoundException",
			run: func(t *testing.T, h *codepipeline.Handler, _ string) *httptest.ResponseRecorder {
				t.Helper()

				return doRequest(t, h, "GetPipelineExecution", map[string]any{
					"pipelineName":        "term-pipeline",
					"pipelineExecutionId": "no-such-execution",
				})
			},
			httpStatus: http.StatusBadRequest,
			wantType:   "PipelineExecutionNotFoundException",
		},
		{
			name: "StopPipelineExecution on unknown ID returns PipelineExecutionNotFoundException",
			run: func(t *testing.T, h *codepipeline.Handler, _ string) *httptest.ResponseRecorder {
				t.Helper()

				return doRequest(t, h, "StopPipelineExecution", map[string]any{
					"pipelineName":        "term-pipeline",
					"pipelineExecutionId": "no-such-execution",
				})
			},
			httpStatus: http.StatusBadRequest,
			wantType:   "PipelineExecutionNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreatePipeline(context.Background(), samplePipeline("term-pipeline"), nil)
			require.NoError(t, err)

			startRec := doRequest(t, h, "StartPipelineExecution", map[string]any{"name": "term-pipeline"})
			require.Equal(t, http.StatusOK, startRec.Code)

			var startOut map[string]any
			require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
			execID, _ := startOut["pipelineExecutionId"].(string)
			require.NotEmpty(t, execID)

			rec := tt.run(t, h, execID)
			require.Equal(t, tt.httpStatus, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			if tt.wantType != "" {
				assert.Equal(t, tt.wantType, out["__type"])
			}

			if tt.wantStatus != "" {
				// GetPipelineExecution nests its result under a
				// "pipelineExecution" envelope; other successful ops in this
				// table (StopPipelineExecution) put "status" at the top level.
				if nested, ok := out["pipelineExecution"].(map[string]any); ok {
					assert.Equal(t, tt.wantStatus, nested["status"])
				} else {
					assert.Equal(t, tt.wantStatus, out["status"])
				}
			}
		})
	}
}

func TestHandler_StartGetStopListPipelineExecution(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create pipeline first
	doRequest(t, h, "CreatePipeline", map[string]any{
		"pipeline": samplePipeline("exec-pipeline"),
	})

	// Start pipeline execution
	rec := doRequest(t, h, "StartPipelineExecution", map[string]any{
		"name": "exec-pipeline",
	})
	require.Equal(t, 200, rec.Code)

	resp := decodeBody(t, rec.Body.Bytes())
	execID, _ := resp["pipelineExecutionId"].(string)
	require.NotEmpty(t, execID)

	// Start with missing name
	rec = doRequest(t, h, "StartPipelineExecution", map[string]any{})
	assert.Equal(t, 400, rec.Code)

	// Get pipeline execution
	rec = doRequest(t, h, "GetPipelineExecution", map[string]any{
		"pipelineName":        "exec-pipeline",
		"pipelineExecutionId": execID,
	})
	require.Equal(t, 200, rec.Code)

	// Get with missing name
	rec = doRequest(t, h, "GetPipelineExecution", map[string]any{})
	assert.Equal(t, 400, rec.Code)

	// Stop pipeline execution
	rec = doRequest(t, h, "StopPipelineExecution", map[string]any{
		"pipelineName":        "exec-pipeline",
		"pipelineExecutionId": execID,
		"reason":              "manual stop",
	})
	require.Equal(t, 200, rec.Code)

	// Stop with missing name
	rec = doRequest(t, h, "StopPipelineExecution", map[string]any{})
	assert.Equal(t, 400, rec.Code)

	// List pipeline executions
	rec = doRequest(t, h, "ListPipelineExecutions", map[string]any{
		"pipelineName": "exec-pipeline",
	})
	require.Equal(t, 200, rec.Code)

	// List with missing name
	rec = doRequest(t, h, "ListPipelineExecutions", map[string]any{})
	assert.Equal(t, 400, rec.Code)
}

// ---- Pipeline State tests ----

func TestHandler_ListPipelineExecutions_StoresAndReturns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codepipeline.Handler) string
		name       string
		wantCount  int
		wantStatus int
	}{
		{
			name: "two starts gives two entries in reverse order",
			setup: func(h *codepipeline.Handler) string {
				_, err := h.Backend.CreatePipeline(context.Background(), samplePipeline("list-execs"), nil)
				require.NoError(t, err)

				rec := doRequest(t, h, "StartPipelineExecution", map[string]any{"name": "list-execs"})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = doRequest(t, h, "StartPipelineExecution", map[string]any{"name": "list-execs"})
				require.Equal(t, http.StatusOK, rec.Code)

				return "list-execs"
			},
			wantCount:  2,
			wantStatus: http.StatusOK,
		},
		{
			name: "empty pipeline returns empty list",
			setup: func(h *codepipeline.Handler) string {
				_, err := h.Backend.CreatePipeline(context.Background(), samplePipeline("empty-execs"), nil)
				require.NoError(t, err)

				return "empty-execs"
			},
			wantCount:  0,
			wantStatus: http.StatusOK,
		},
		{
			name: "non-existent pipeline returns error",
			setup: func(_ *codepipeline.Handler) string {
				return "ghost-pipeline"
			},
			wantCount:  0,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			pipelineName := tt.setup(h)

			rec := doRequest(t, h, "ListPipelineExecutions", map[string]any{"pipelineName": pipelineName})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				summaries, _ := out["pipelineExecutionSummaries"].([]any)
				assert.Len(t, summaries, tt.wantCount)
			}
		})
	}
}

// --------------------------------------------------------------------------
// PipelineSummary includes pipelineType and executionMode
// --------------------------------------------------------------------------

func TestListPipelineExecutions_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	const name = "page-pipeline"
	_, err := h.Backend.CreatePipeline(t.Context(), samplePipeline(name), nil)
	require.NoError(t, err)

	const total = 5
	for range total {
		_, sErr := h.Backend.StartPipelineExecution(t.Context(), name)
		require.NoError(t, sErr)
	}

	type listResp struct {
		NextToken                  string           `json:"nextToken"`
		PipelineExecutionSummaries []map[string]any `json:"pipelineExecutionSummaries"`
	}

	seen := map[string]bool{}
	token := ""
	pages := 0

	for {
		body := map[string]any{"pipelineName": name, "maxResults": 2}
		if token != "" {
			body["nextToken"] = token
		}

		rec := doRequest(t, h, "ListPipelineExecutions", body)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp listResp
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.LessOrEqual(t, len(resp.PipelineExecutionSummaries), 2, "page exceeds maxResults")

		for _, s := range resp.PipelineExecutionSummaries {
			id := s["pipelineExecutionId"].(string)
			assert.False(t, seen[id], "execution %s returned twice", id)
			seen[id] = true
		}

		pages++
		require.Less(t, pages, 10, "pagination did not terminate")

		token = resp.NextToken
		if token == "" {
			break
		}
	}

	assert.Len(t, seen, total, "all executions returned exactly once")
	assert.GreaterOrEqual(t, pages, 3)
}

func TestCPBounds_ListPipelineExecutions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	setupRec := doRequest(t, h, "CreatePipeline", map[string]any{
		"pipeline": samplePipeline("bounds-pipe"),
	})
	require.Equal(t, http.StatusOK, setupRec.Code)

	setupRec = doRequest(t, h, "StartPipelineExecution", map[string]any{"name": "bounds-pipe"})
	require.Equal(t, http.StatusOK, setupRec.Code)

	tests := []struct {
		name       string
		maxResults int32
		wantError  bool
	}{
		{"0 uses cap", 0, false},
		{"1 is valid", 1, false},
		{"100 is valid cap", 100, false},
		{"101 exceeds cap", 101, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, "ListPipelineExecutions", map[string]any{
				"pipelineName": "bounds-pipe",
				"maxResults":   tc.maxResults,
			})

			if tc.wantError {
				assert.NotEqual(t, http.StatusOK, rec.Code)
			} else {
				assert.Equal(t, http.StatusOK, rec.Code)
			}
		})
	}
}

func TestCPPagination_ListPipelineExecutions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreatePipeline", map[string]any{
		"pipeline": samplePipeline("exec-pipe"),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	for range 5 {
		rec = doRequest(t, h, "StartPipelineExecution", map[string]any{"name": "exec-pipe"})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	var nextToken string
	total := 0
	pages := 0

	for {
		body := map[string]any{
			"pipelineName": "exec-pipe",
			"maxResults":   int32(2),
		}
		if nextToken != "" {
			body["nextToken"] = nextToken
		}

		rec = doRequest(t, h, "ListPipelineExecutions", body)
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			NextToken                  string           `json:"nextToken"`
			PipelineExecutionSummaries []map[string]any `json:"pipelineExecutionSummaries"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

		total += len(out.PipelineExecutionSummaries)
		pages++
		nextToken = out.NextToken

		if nextToken == "" {
			break
		}
	}

	assert.Equal(t, 5, total)
	assert.Equal(t, 3, pages)
}

// ---------------------------------------------------------------------------
// ListWebhooks — MaxResults (1-60) + multi-page
// ---------------------------------------------------------------------------

func TestListPipelineExecutions_TriggerObject(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreatePipeline", map[string]any{
		"pipeline": samplePipeline("trigger-pipe"),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	for range 2 {
		rec = doRequest(t, h, "StartPipelineExecution", map[string]any{"name": "trigger-pipe"})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec = doRequest(t, h, "ListPipelineExecutions", map[string]any{"pipelineName": "trigger-pipe"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		PipelineExecutionSummaries []map[string]any `json:"pipelineExecutionSummaries"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.PipelineExecutionSummaries, 2)

	for _, exec := range out.PipelineExecutionSummaries {
		trigger, ok := exec["trigger"].(map[string]any)
		assert.True(t, ok, "trigger must be an object, not a string")
		if ok {
			assert.NotEmpty(t, trigger["triggerType"], "trigger.triggerType must be set")
		}
	}
}

// TestParity_Webhook_Tagging verifies TagResource/UntagResource/ListTagsForResource on webhooks.

func TestListActionExecutions_TracksExecutions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		filterFn    func(execID string) string
		name        string
		wantCount   int
		wantErr     bool
		unknownPipe bool
	}{
		{
			name:      "all_executions",
			filterFn:  func(string) string { return "" },
			wantCount: 2, // two StartPipelineExecution calls, one action each
		},
		{
			name:      "filter_by_execution_id",
			filterFn:  func(execID string) string { return execID },
			wantCount: 1,
		},
		{
			name:      "filter_unknown_execution_id",
			filterFn:  func(string) string { return "does-not-exist" },
			wantCount: 0,
		},
		{
			name:        "unknown_pipeline",
			unknownPipe: true,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")

			if tt.unknownPipe {
				_, err := b.ListActionExecutions(context.Background(), "missing", "")
				require.Error(t, err)

				return
			}

			_, err := b.CreatePipeline(context.Background(), samplePipeline("ae-pipeline"), nil)
			require.NoError(t, err)

			exec1, err := b.StartPipelineExecution(context.Background(), "ae-pipeline")
			require.NoError(t, err)
			_, err = b.StartPipelineExecution(context.Background(), "ae-pipeline")
			require.NoError(t, err)

			items, err := b.ListActionExecutions(
				context.Background(),
				"ae-pipeline",
				tt.filterFn(exec1.PipelineExecutionID),
			)
			require.NoError(t, err)
			assert.Len(t, items, tt.wantCount)

			for _, it := range items {
				assert.Equal(t, "SourceAction", it["actionName"])
				assert.Equal(t, "Source", it["stageName"])
				assert.Equal(t, "Succeeded", it["status"])
				assert.NotEmpty(t, it["actionExecutionId"])
			}
		})
	}
}

// TestListRuleTypes_ReturnsCatalog verifies that ListRuleTypes returns the
// AWS-managed rule type catalog with well-formed identifiers.

func TestCPBounds_ListActionExecutions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	setupRec := doRequest(t, h, "CreatePipeline", map[string]any{
		"pipeline": samplePipeline("ae-bounds-pipe"),
	})
	require.Equal(t, http.StatusOK, setupRec.Code)

	setupRec = doRequest(t, h, "StartPipelineExecution", map[string]any{"name": "ae-bounds-pipe"})
	require.Equal(t, http.StatusOK, setupRec.Code)

	tests := []struct {
		name       string
		maxResults int32
		wantError  bool
	}{
		{"0 uses cap", 0, false},
		{"1 is valid", 1, false},
		{"100 is valid cap", 100, false},
		{"101 exceeds cap", 101, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, "ListActionExecutions", map[string]any{
				"pipelineName": "ae-bounds-pipe",
				"maxResults":   tc.maxResults,
			})

			if tc.wantError {
				assert.NotEqual(t, http.StatusOK, rec.Code)
			} else {
				assert.Equal(t, http.StatusOK, rec.Code)
			}
		})
	}
}

func TestCPPagination_ListActionExecutions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreatePipeline", map[string]any{
		"pipeline": samplePipeline("ae-page-pipe"),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// 5 StartPipelineExecution calls on a 1-action pipeline → 5 action executions.
	for range 5 {
		rec = doRequest(t, h, "StartPipelineExecution", map[string]any{"name": "ae-page-pipe"})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	var nextToken string
	total := 0
	pages := 0

	for {
		body := map[string]any{
			"pipelineName": "ae-page-pipe",
			"maxResults":   int32(2),
		}
		if nextToken != "" {
			body["nextToken"] = nextToken
		}

		rec = doRequest(t, h, "ListActionExecutions", body)
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			NextToken              string           `json:"nextToken"`
			ActionExecutionDetails []map[string]any `json:"actionExecutionDetails"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

		total += len(out.ActionExecutionDetails)
		pages++
		nextToken = out.NextToken

		if nextToken == "" {
			break
		}
	}

	assert.Equal(t, 5, total)
	assert.Equal(t, 3, pages)
}

// ---------------------------------------------------------------------------
// ListActionTypes — no MaxResults input (fixed cap 25) + multi-page
// ---------------------------------------------------------------------------

func TestListDeployActionExecutionTargets_KnownAndUnknown(t *testing.T) {
	t.Parallel()

	b := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.ListDeployActionExecutionTargets(context.Background(), "missing", "exec-1")
	require.Error(t, err)

	_, err = b.CreatePipeline(context.Background(), samplePipeline("dt-pipeline"), nil)
	require.NoError(t, err)

	// A known pipeline with an ActionExecutionId that was never recorded
	// still errors: real ListDeployActionExecutionTargets does not return an
	// empty list for a made-up execution ID (gopherstack-2wvq).
	_, err = b.ListDeployActionExecutionTargets(context.Background(), "dt-pipeline", "exec-1")
	require.Error(t, err)

	_, err = b.StartPipelineExecution(context.Background(), "dt-pipeline")
	require.NoError(t, err)

	execs, err := b.ListActionExecutions(context.Background(), "dt-pipeline", "")
	require.NoError(t, err)
	require.NotEmpty(t, execs)
	actionExecutionID, ok := execs[0]["actionExecutionId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, actionExecutionID)

	items, err := b.ListDeployActionExecutionTargets(context.Background(), "dt-pipeline", actionExecutionID)
	require.NoError(t, err)
	assert.Empty(t, items)

	// ActionExecutionId alone -- no PipelineName -- must also resolve:
	// ListDeployActionExecutionTargetsInput marks only ActionExecutionId
	// required (codepipeline@v1.49.4), PipelineName is an optional filter.
	items, err = b.ListDeployActionExecutionTargets(context.Background(), "", actionExecutionID)
	require.NoError(t, err)
	assert.Empty(t, items)

	_, err = b.ListDeployActionExecutionTargets(context.Background(), "", "no-such-execution")
	require.Error(t, err)
}
