package codepipeline_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Ensure net/http import used via StatusOK references only.

// ---------------------------------------------------------------------------
// ListPipelineExecutions — MaxResults (1-100) + multi-page
// ---------------------------------------------------------------------------

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

func TestCPBounds_ListWebhooks(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Seed one pipeline for the webhook target.
	setupRec := doRequest(t, h, "CreatePipeline", map[string]any{
		"pipeline": samplePipeline("wh-pipe"),
	})
	require.Equal(t, http.StatusOK, setupRec.Code)

	setupRec = doRequest(t, h, "PutWebhook", map[string]any{
		"webhook": map[string]any{
			"name":                        "wh-bounds",
			"targetPipeline":              "wh-pipe",
			"targetAction":                "SourceAction",
			"authentication":              "UNAUTHENTICATED",
			"filters":                     []any{},
			"authenticationConfiguration": map[string]any{},
		},
	})
	require.Equal(t, http.StatusOK, setupRec.Code)

	tests := []struct {
		name       string
		maxResults int32
		wantError  bool
	}{
		{"0 uses cap", 0, false},
		{"1 is valid", 1, false},
		{"60 is valid cap", 60, false},
		{"61 exceeds cap", 61, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, "ListWebhooks", map[string]any{
				"MaxResults": tc.maxResults,
			})

			if tc.wantError {
				assert.NotEqual(t, http.StatusOK, rec.Code)
			} else {
				assert.Equal(t, http.StatusOK, rec.Code)
			}
		})
	}
}

func TestCPPagination_ListWebhooks(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreatePipeline", map[string]any{
		"pipeline": samplePipeline("wh-page-pipe"),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	for i := range 5 {
		rec = doRequest(t, h, "PutWebhook", map[string]any{
			"webhook": map[string]any{
				"name":                        fmt.Sprintf("wh-%02d", i),
				"targetPipeline":              "wh-page-pipe",
				"targetAction":                "SourceAction",
				"authentication":              "UNAUTHENTICATED",
				"filters":                     []any{},
				"authenticationConfiguration": map[string]any{},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	var nextToken string
	total := 0
	pages := 0

	for {
		body := map[string]any{
			"MaxResults": int32(2),
		}
		if nextToken != "" {
			body["NextToken"] = nextToken
		}

		rec = doRequest(t, h, "ListWebhooks", body)
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			NextToken string           `json:"NextToken"`
			Webhooks  []map[string]any `json:"webhooks"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

		total += len(out.Webhooks)
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
// ListActionExecutions — MaxResults (1-100) + multi-page
// ---------------------------------------------------------------------------

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

func TestCPPagination_ListActionTypes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create 5 custom action types to paginate over.
	for i := range 5 {
		rec := doRequest(t, h, "CreateCustomActionType", map[string]any{
			"category":              "Build",
			"provider":              fmt.Sprintf("CustomProvider%02d", i),
			"version":               "1",
			"inputArtifactDetails":  map[string]any{"minimumCount": 0, "maximumCount": 5},
			"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Verify all 5 appear with no NextToken (default cap 25 covers all).
	rec := doRequest(t, h, "ListActionTypes", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		NextToken   string           `json:"nextToken"`
		ActionTypes []map[string]any `json:"actionTypes"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

	assert.Len(t, out.ActionTypes, 5)
	assert.Empty(t, out.NextToken, "5 items fit within the 25-cap default page")
}

func TestCPPagination_ListActionTypes_MultiPage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create 30 custom action types to exceed the fixed cap of 25.
	for i := range 30 {
		rec := doRequest(t, h, "CreateCustomActionType", map[string]any{
			"category":              "Build",
			"provider":              fmt.Sprintf("Provider%03d", i),
			"version":               "1",
			"inputArtifactDetails":  map[string]any{"minimumCount": 0, "maximumCount": 5},
			"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	var nextToken string
	total := 0
	pages := 0

	for {
		body := map[string]any{}
		if nextToken != "" {
			body["nextToken"] = nextToken
		}

		rec := doRequest(t, h, "ListActionTypes", body)
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			NextToken   string           `json:"nextToken"`
			ActionTypes []map[string]any `json:"actionTypes"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

		total += len(out.ActionTypes)
		pages++
		nextToken = out.NextToken

		if nextToken == "" {
			break
		}
	}

	assert.Equal(t, 30, total)
	assert.Equal(t, 2, pages) // 25+5
}

// ---------------------------------------------------------------------------
// ListRuleExecutions — MaxResults (1-100), always empty list for known pipeline
// ---------------------------------------------------------------------------

func TestCPBounds_ListRuleExecutions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	setupRec := doRequest(t, h, "CreatePipeline", map[string]any{
		"pipeline": samplePipeline("re-bounds-pipe"),
	})
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

			rec := doRequest(t, h, "ListRuleExecutions", map[string]any{
				"pipelineName": "re-bounds-pipe",
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
