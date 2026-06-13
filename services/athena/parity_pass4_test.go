package athena_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListQueryExecutions_Pagination verifies that ListQueryExecutions honors
// MaxResults and walks pages via NextToken without dropping or duplicating IDs.
func TestListQueryExecutions_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	const total = 5
	for range total {
		rec := doRequest(t, h, "StartQueryExecution", `{"QueryString":"SELECT 1"}`)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	type listResp struct {
		NextToken         string   `json:"NextToken"`
		QueryExecutionIDs []string `json:"QueryExecutionIds"`
	}

	seen := map[string]bool{}
	token := ""
	pages := 0

	for {
		body := `{"MaxResults":2}`
		if token != "" {
			body = `{"MaxResults":2,"NextToken":"` + token + `"}`
		}

		rec := doRequest(t, h, "ListQueryExecutions", body)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp listResp
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.LessOrEqual(t, len(resp.QueryExecutionIDs), 2, "page exceeds MaxResults")

		for _, id := range resp.QueryExecutionIDs {
			assert.False(t, seen[id], "id %s returned twice", id)
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
	assert.GreaterOrEqual(t, pages, 3, "MaxResults=2 over 5 items should span >=3 pages")
}

// TestListQueryExecutions_NextTokenOmittedOnLastPage verifies the final page
// carries no NextToken.
func TestListQueryExecutions_NextTokenOmittedOnLastPage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "StartQueryExecution", `{"QueryString":"SELECT 1"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "ListQueryExecutions", `{"MaxResults":50}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	_, hasToken := resp["NextToken"]
	assert.False(t, hasToken, "NextToken must be omitted on the last page")
}
