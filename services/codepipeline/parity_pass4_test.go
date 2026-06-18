package codepipeline_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListPipelineExecutions_Pagination verifies that ListPipelineExecutions
// (previously ignoring its pagination params and omitting NextToken) now honors
// MaxResults and walks pages via NextToken.
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
