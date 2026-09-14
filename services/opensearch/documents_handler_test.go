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

	// Index a document with an explicit ID. Real OpenSearch's Index Document
	// API response (https://docs.opensearch.org/latest/api-reference/
	// document-apis/index-document/) carries _version/_shards/_seq_no/
	// _primary_term alongside _index/_id/result.
	resp := doRequest(t, h, http.MethodPost, base+"/_doc/i1",
		map[string]any{"name": "alpha", "kind": "tool"})
	body := decodeBody(t, resp)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "i1", body["_id"])
	assert.Equal(t, "created", body["result"])
	assert.InDelta(t, 1, body["_version"], 0)
	assert.InDelta(t, 0, body["_seq_no"], 0)
	assert.InDelta(t, 1, body["_primary_term"], 0)
	assert.NotContains(t, body, "created", "real Index Document API has no top-level \"created\" field")
	shards, ok := body["_shards"].(map[string]any)
	require.True(t, ok, "_shards must be present")
	assert.InDelta(t, 1, shards["total"], 0)

	// Index a document with an auto-generated ID.
	resp = doRequest(t, h, http.MethodPost, base+"/_doc",
		map[string]any{"name": "beta", "kind": "tool"})
	body = decodeBody(t, resp)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NotEmpty(t, body["_id"])

	// Count reflects both documents. Real Count API response
	// (https://docs.opensearch.org/latest/api-reference/search-apis/count/)
	// carries _shards alongside count.
	resp = doRequest(t, h, http.MethodGet, base+"/_count", nil)
	body = decodeBody(t, resp)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.InDelta(t, 2, body["count"], 0)
	countShards, ok := body["_shards"].(map[string]any)
	require.True(t, ok, "_shards must be present")
	assert.InDelta(t, 0, countShards["skipped"], 0)

	// GetIndex's only real wire field is IndexSchema (api_op_GetIndex.go) --
	// document count isn't part of the real response shape, so it's already
	// covered by the _count assertion above instead.
	resp = doRequest(t, h, http.MethodGet, base, nil)
	body = decodeBody(t, resp)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, body, "IndexSchema")

	// Fetch a document. Real Get Document API response carries
	// _version/_seq_no/_primary_term alongside found/_source.
	resp = doRequest(t, h, http.MethodGet, base+"/_doc/i1", nil)
	body = decodeBody(t, resp)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	source, ok := body["_source"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "alpha", source["name"])
	assert.InDelta(t, 1, body["_version"], 0)
	assert.InDelta(t, 1, body["_primary_term"], 0)

	// Search returns real hits, not synthetic values. Real Search API
	// response (https://docs.opensearch.org/latest/api-reference/
	// search-apis/search/) carries took/timed_out/_shards at the top level
	// and max_score/_score alongside total/hits.
	resp = doRequest(t, h, http.MethodPost, base+"/_search", map[string]any{
		"query": map[string]any{"term": map[string]any{"name": "alpha"}},
	})
	body = decodeBody(t, resp)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, body, "took")
	assert.Equal(t, false, body["timed_out"])
	assert.Contains(t, body, "_shards")
	hits, ok := body["hits"].(map[string]any)
	require.True(t, ok)
	total, ok := hits["total"].(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, 1, total["value"], 0)
	assert.NotNil(t, hits["max_score"])
	hitList, ok := hits["hits"].([]any)
	require.True(t, ok)
	require.Len(t, hitList, 1)
	firstHit, ok := hitList[0].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, firstHit, "_score")

	// Delete the document; count drops. Real Delete Document API response
	// carries _version/_shards/_seq_no/_primary_term alongside result.
	resp = doRequest(t, h, http.MethodDelete, base+"/_doc/i1", nil)
	body = decodeBody(t, resp)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "deleted", body["result"])
	assert.InDelta(t, 2, body["_version"], 0, "delete bumps the tombstone _version")
	assert.Contains(t, body, "_shards")

	resp = doRequest(t, h, http.MethodGet, base+"/_count", nil)
	body = decodeBody(t, resp)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.InDelta(t, 1, body["count"], 0)
}

// TestHTTPCreateIndex_RealResponseShape proves the data-plane create-index
// route ({domainName}/index/{indexName}, not an AWS SDK op) returns real
// OpenSearch's Create Index API response
// (https://docs.opensearch.org/latest/api-reference/index-apis/create-index/:
// acknowledged/shards_acknowledged/index) and reads the real lowercase
// mappings/settings/aliases request body, instead of the AWS-control-plane-
// shaped PascalCase body and GetIndex-shaped response this route used to
// have by mistake.
func TestHTTPCreateIndex_RealResponseShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPost, docBasePath, map[string]any{"DomainName": "dom"})
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = doRequest(t, h, http.MethodPost, docBasePath+"/dom/index/widgets", map[string]any{
		"mappings": map[string]any{"properties": map[string]any{"name": map[string]any{"type": "text"}}},
		"settings": map[string]any{"number_of_shards": 1},
	})
	body := decodeBody(t, resp)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	assert.Equal(t, true, body["acknowledged"])
	assert.Equal(t, true, body["shards_acknowledged"])
	assert.Equal(t, "widgets", body["index"])
	assert.NotContains(t, body, "IndexName",
		"response must be the real create-index shape, not the invented metadata envelope")

	// The lowercase mappings/settings body must have actually been read, not
	// silently dropped: GetIndex's synthesized IndexSchema echoes them back.
	resp = doRequest(t, h, http.MethodGet, docBasePath+"/dom/index/widgets", nil)
	body = decodeBody(t, resp)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	schema, ok := body["IndexSchema"].(map[string]any)
	require.True(t, ok)
	settings, ok := schema["Settings"].(map[string]any)
	require.True(t, ok, "the lowercase \"settings\" request body must have been read")
	assert.InDelta(t, 1, settings["number_of_shards"], 0)
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
