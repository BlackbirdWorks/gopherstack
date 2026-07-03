package athena_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

// listPage requests a single ListQueryExecutions page and returns the ids and
// next token.
func listPage(t *testing.T, h *athena.Handler, maxResults int, token string) ([]string, string) {
	t.Helper()

	body := `{"MaxResults":` + itoa(maxResults) + `,"NextToken":` + jsonString(token) + `}`
	rec := doRequest(t, h, "ListQueryExecutions", body)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		NextToken         string   `json:"NextToken"`
		QueryExecutionIDs []string `json:"QueryExecutionIds"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp.QueryExecutionIDs, resp.NextToken
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}

	neg := n < 0
	if neg {
		n = -n
	}

	var buf []byte
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}

	if neg {
		buf = append([]byte{'-'}, buf...)
	}

	return string(buf)
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)

	return string(b)
}

func seedExecutions(t *testing.T, h *athena.Handler, n int) {
	t.Helper()

	for range n {
		rec := doRequest(t, h, "StartQueryExecution", `{"QueryString":"SELECT 1","WorkGroup":"primary"}`)
		require.Equal(t, http.StatusOK, rec.Code)
	}
}

// TestPagination_OpaqueToken verifies that the ListQueryExecutions NextToken is
// opaque (not a raw execution ID), that the full page chain enumerates every
// execution exactly once, and that forged/malformed tokens are rejected with
// InvalidRequestException.
func TestPagination_OpaqueToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedExecutions(t, h, 5)

	all, _ := listPage(t, h, 100, "")
	require.Len(t, all, 5)

	idSet := make(map[string]bool, len(all))
	for _, id := range all {
		idSet[id] = true
	}

	// First page of size 2 yields an opaque token.
	page1, token := listPage(t, h, 2, "")
	require.Len(t, page1, 2)
	require.NotEmpty(t, token)

	// The token must not leak a raw execution ID.
	assert.False(t, idSet[token], "pagination token must be opaque, not a raw execution ID")

	// Walk the rest of the chain and confirm no duplicates / full coverage.
	seen := map[string]int{}
	for _, id := range page1 {
		seen[id]++
	}

	next := token
	for next != "" {
		var page []string
		page, next = listPage(t, h, 2, next)

		for _, id := range page {
			seen[id]++
		}
	}

	assert.Len(t, seen, 5, "all executions enumerated across the page chain")
	for id, count := range seen {
		assert.Equal(t, 1, count, "execution %s appeared %d times (expected once)", id, count)
	}
}

// TestPagination_ForgedTokenRejected verifies malformed and tampered tokens are
// rejected rather than silently accepted.
func TestPagination_ForgedTokenRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		token string
	}{
		{name: "not_base64", token: "!!!not-a-token!!!"},
		{name: "base64_without_separator", token: "AAAABBBB"},
		{name: "raw_execution_id_shaped", token: "abcdef0123"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			seedExecutions(t, h, 3)

			rec := doRequest(t, h, "ListQueryExecutions",
				`{"MaxResults":2,"NextToken":`+jsonString(tt.token)+`}`)

			require.Equal(t, http.StatusBadRequest, rec.Code)

			var errResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, "InvalidRequestException", errResp["__type"])
		})
	}
}

// TestPagination_MutationStable verifies that inserting executions between page
// fetches does not skip or duplicate any of the original executions when a
// previously issued token is used to resume.
func TestPagination_MutationStable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedExecutions(t, h, 5)

	original, _ := listPage(t, h, 100, "")
	require.Len(t, original, 5)

	page1, token := listPage(t, h, 2, "")
	require.Len(t, page1, 2)
	require.NotEmpty(t, token)

	// Mutate the collection between fetches.
	seedExecutions(t, h, 3)

	seen := map[string]int{}
	for _, id := range page1 {
		seen[id]++
	}

	next := token
	for next != "" {
		var page []string
		page, next = listPage(t, h, 2, next)

		for _, id := range page {
			seen[id]++
		}
	}

	// Every original execution appears exactly once across the resumed chain.
	for _, id := range original {
		assert.Equal(t, 1, seen[id], "original execution %s must appear exactly once", id)
	}
}
