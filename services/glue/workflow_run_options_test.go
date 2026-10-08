package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStartWorkflowRun_RunProperties(t *testing.T) {
	t.Parallel()

	tests := []struct {
		defaults map[string]string
		request  map[string]string
		want     map[string]any
		name     string
	}{
		{name: "none", want: map[string]any{}},
		{
			name:     "defaults_only",
			defaults: map[string]string{"a": "1"},
			want:     map[string]any{"a": "1"},
		},
		{
			name:     "request_overrides_defaults",
			defaults: map[string]string{"a": "1", "b": "2"},
			request:  map[string]string{"b": "3", "c": "4"},
			want:     map[string]any{"a": "1", "b": "3", "c": "4"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doGlueRequest(t, h, "CreateWorkflow", map[string]any{
				"Name": "wf", "DefaultRunProperties": tt.defaults,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doGlueRequest(t, h, "StartWorkflowRun", map[string]any{"Name": "wf", "RunProperties": tt.request})
			require.Equal(t, http.StatusOK, rec.Code)

			var start map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &start))

			rec = doGlueRequest(t, h, "GetWorkflowRunProperties", map[string]any{
				"Name": "wf", "RunId": start["RunId"],
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var got map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
			assert.Equal(t, tt.want, got["RunProperties"])
		})
	}
}

func TestGetWorkflowRun_IncludeGraph(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		action       string
		includeGraph bool
	}{
		{name: "run_without_graph", action: "GetWorkflowRun"},
		{name: "run_with_graph", action: "GetWorkflowRun", includeGraph: true},
		{name: "runs_without_graph", action: "GetWorkflowRuns"},
		{name: "runs_with_graph", action: "GetWorkflowRuns", includeGraph: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			setupGraphWorkflow(t, h)

			rec := doGlueRequest(t, h, "StartWorkflowRun", map[string]any{"Name": "graphwf"})
			require.Equal(t, http.StatusOK, rec.Code)

			var start map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &start))

			body := map[string]any{"Name": "graphwf", "IncludeGraph": tt.includeGraph}
			if tt.action == "GetWorkflowRun" {
				body["RunId"] = start["RunId"]
			}

			rec = doGlueRequest(t, h, tt.action, body)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			run, _ := out["Run"].(map[string]any)
			if tt.action == "GetWorkflowRuns" {
				runs, _ := out["Runs"].([]any)
				require.Len(t, runs, 1)
				run, _ = runs[0].(map[string]any)
			}

			if !tt.includeGraph {
				assert.NotContains(t, run, "Graph")

				return
			}

			graph, ok := run["Graph"].(map[string]any)
			require.True(t, ok)
			assert.NotEmpty(t, graph["Nodes"])
		})
	}
}
