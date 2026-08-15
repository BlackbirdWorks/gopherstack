package opensearch_test

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

const docBasePath = "/2021-01-01/opensearch/domain"

// decodeBody decodes an HTTP response body into a map.
func decodeBody(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var out map[string]any
	if len(raw) > 0 {
		require.NoError(t, json.Unmarshal(raw, &out))
	}

	return out
}

// setupIndexHandler creates a domain + index over HTTP and returns the handler.
func setupIndexHandler(t *testing.T, domain, index string) *opensearch.Handler {
	t.Helper()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPost, docBasePath, map[string]any{"DomainName": domain})
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = doRequest(t, h, http.MethodPost, docBasePath+"/"+domain+"/index/"+index, map[string]any{})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	return h
}

// TestHTTPDocumentCRUDAndSearch drives the HTTP _doc / _count / _search routes
// end to end so the emulated data-plane is exercised as callers would use it.
func TestHTTPDocumentCRUDAndSearch(t *testing.T) {
	t.Parallel()

	h := setupIndexHandler(t, "dom", "items")
	base := docBasePath + "/dom/index/items"

	// Index a document with an explicit ID.
	resp := doRequest(t, h, http.MethodPost, base+"/_doc/i1",
		map[string]any{"name": "alpha", "kind": "tool"})
	body := decodeBody(t, resp)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "i1", body["_id"])
	assert.Equal(t, "created", body["result"])

	// Index a document with an auto-generated ID.
	resp = doRequest(t, h, http.MethodPost, base+"/_doc",
		map[string]any{"name": "beta", "kind": "tool"})
	body = decodeBody(t, resp)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NotEmpty(t, body["_id"])

	// Count reflects both documents.
	resp = doRequest(t, h, http.MethodGet, base+"/_count", nil)
	body = decodeBody(t, resp)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.InDelta(t, 2, body["count"], 0)

	// GetIndex's only real wire field is IndexSchema (api_op_GetIndex.go) --
	// document count isn't part of the real response shape, so it's already
	// covered by the _count assertion above instead.
	resp = doRequest(t, h, http.MethodGet, base, nil)
	body = decodeBody(t, resp)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, body, "IndexSchema")

	// Fetch a document.
	resp = doRequest(t, h, http.MethodGet, base+"/_doc/i1", nil)
	body = decodeBody(t, resp)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	source, ok := body["_source"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "alpha", source["name"])

	// Search returns real hits, not synthetic values.
	resp = doRequest(t, h, http.MethodPost, base+"/_search", map[string]any{
		"query": map[string]any{"term": map[string]any{"name": "alpha"}},
	})
	body = decodeBody(t, resp)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	hits, ok := body["hits"].(map[string]any)
	require.True(t, ok)
	total, ok := hits["total"].(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, 1, total["value"], 0)
	hitList, ok := hits["hits"].([]any)
	require.True(t, ok)
	require.Len(t, hitList, 1)

	// Delete the document; count drops.
	resp = doRequest(t, h, http.MethodDelete, base+"/_doc/i1", nil)
	body = decodeBody(t, resp)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "deleted", body["result"])

	resp = doRequest(t, h, http.MethodGet, base+"/_count", nil)
	body = decodeBody(t, resp)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.InDelta(t, 1, body["count"], 0)
}

// TestHTTPDocumentErrors covers HTTP error mapping for the document routes.
func TestHTTPDocumentErrors(t *testing.T) {
	t.Parallel()

	h := setupIndexHandler(t, "dom", "items")
	base := docBasePath + "/dom/index/items"

	tests := []struct {
		body     any
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "get_missing_document",
			method:   http.MethodGet,
			path:     base + "/_doc/ghost",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "count_unknown_index",
			method:   http.MethodGet,
			path:     docBasePath + "/dom/index/nope/_count",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "search_unsupported_query",
			method:   http.MethodPost,
			path:     base + "/_search",
			body:     map[string]any{"query": map[string]any{"range": map[string]any{"n": 1}}},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp := doRequest(t, h, tt.method, tt.path, tt.body)
			defer resp.Body.Close()
			assert.Equal(t, tt.wantCode, resp.StatusCode)
		})
	}
}
