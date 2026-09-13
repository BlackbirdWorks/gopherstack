package opensearch_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

// doBulkRequest posts a raw NDJSON body (real _bulk request bodies are not
// valid single JSON documents, so doRequest's json.Marshal wrapper can't be
// reused) to path with the given Content-Type, and decodes the response.
func doBulkRequest(t *testing.T, h *opensearch.Handler, path, contentType, ndjson string) (int, map[string]any) {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(ndjson))
	req.Header.Set("Content-Type", contentType)

	rw := httptest.NewRecorder()
	h.ServeHTTP(rw, req)
	resp := rw.Result()
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var out map[string]any
	if len(raw) > 0 {
		require.NoError(t, json.Unmarshal(raw, &out))
	}

	return resp.StatusCode, out
}

// createTestIndex creates domainName/indexName so a bulk action targeting it
// finds a real, existing index -- this backend requires explicit index
// creation before document writes, the same restraint the single-document
// _doc route already has (documents_handler_test.go's setupIndexHandler).
func createTestIndex(t *testing.T, h *opensearch.Handler, domainName, indexName string) {
	t.Helper()

	resp := doRequest(t, h, http.MethodPost,
		"/2021-01-01/opensearch/domain/"+domainName+"/index/"+indexName, map[string]any{})
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// bulkItem extracts the single-action wire item (e.g. body["index"]) from
// one entry of the response's "items" array.
func bulkItem(t *testing.T, items []any, i int, action string) map[string]any {
	t.Helper()

	entry, ok := items[i].(map[string]any)
	require.True(t, ok, "item %d is not an object", i)

	item, ok := entry[action].(map[string]any)
	require.True(t, ok, "item %d has no %q action", i, action)

	return item
}

// TestBulk_DomainWide_ActionsAndEnvelope drives POST {domainName}/_bulk with
// index/create/update/delete actions and asserts the exact documented
// envelope (took/errors/items) and per-item success shape (_index/_id/
// _version/result/_shards/_seq_no/_primary_term/status) --
// https://opensearch.org/docs/latest/api-reference/document-apis/bulk/.
func TestBulk_DomainWide_ActionsAndEnvelope(t *testing.T) {
	t.Parallel()

	h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
	createTestDomain(t, h, "bulk-dom")
	createTestIndex(t, h, "bulk-dom", "items")

	ndjson := `{"index":{"_index":"items","_id":"i1"}}
{"name":"alpha"}
{"create":{"_index":"items","_id":"i2"}}
{"name":"beta"}
{"update":{"_index":"items","_id":"i1"}}
{"doc":{"kind":"tool"}}
{"delete":{"_index":"items","_id":"i2"}}
`

	status, body := doBulkRequest(t, h, "/2021-01-01/opensearch/domain/bulk-dom/_bulk",
		"application/x-ndjson", ndjson)
	require.Equal(t, http.StatusOK, status)

	assert.Equal(t, false, body["errors"])
	assert.Contains(t, body, "took")

	items, ok := body["items"].([]any)
	require.True(t, ok)
	require.Len(t, items, 4)

	idx := bulkItem(t, items, 0, "index")
	assert.Equal(t, "items", idx["_index"])
	assert.Equal(t, "i1", idx["_id"])
	assert.Equal(t, "created", idx["result"])
	assert.InDelta(t, 1, idx["_version"], 0)
	assert.InDelta(t, http.StatusCreated, idx["status"], 0)
	shards, ok := idx["_shards"].(map[string]any)
	require.True(t, ok, "_shards must be present")
	assert.InDelta(t, 1, shards["total"], 0)
	assert.Contains(t, idx, "_seq_no")
	assert.Contains(t, idx, "_primary_term")

	create := bulkItem(t, items, 1, "create")
	assert.Equal(t, "i2", create["_id"])
	assert.Equal(t, "created", create["result"])
	assert.InDelta(t, http.StatusCreated, create["status"], 0)

	update := bulkItem(t, items, 2, "update")
	assert.Equal(t, "i1", update["_id"])
	assert.Equal(t, "updated", update["result"])
	assert.InDelta(t, 2, update["_version"], 0)
	assert.InDelta(t, http.StatusOK, update["status"], 0)

	del := bulkItem(t, items, 3, "delete")
	assert.Equal(t, "i2", del["_id"])
	assert.Equal(t, "deleted", del["result"])
	assert.InDelta(t, http.StatusOK, del["status"], 0)

	// The update must have genuinely merged, not replaced: "name" from the
	// original index survives alongside the new "kind" field.
	resp := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/domain/bulk-dom/index/items/_doc/i1", nil)
	getBody := decodeBody(t, resp)
	resp.Body.Close()
	source, ok := getBody["_source"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "alpha", source["name"])
	assert.Equal(t, "tool", source["kind"])
}

// TestBulk_ContentTypes proves both documented Content-Type values are
// accepted.
func TestBulk_ContentTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contentType string
	}{
		{name: "ndjson", contentType: "application/x-ndjson"},
		{name: "json", contentType: "application/json"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
			createTestDomain(t, h, "ct-dom")
			createTestIndex(t, h, "ct-dom", "items")

			ndjson := "{\"index\":{\"_index\":\"items\",\"_id\":\"i1\"}}\n{\"name\":\"alpha\"}\n"
			status, body := doBulkRequest(t, h, "/2021-01-01/opensearch/domain/ct-dom/_bulk", tt.contentType, ndjson)
			require.Equal(t, http.StatusOK, status)
			assert.Equal(t, false, body["errors"])
		})
	}
}

// TestBulk_QueryParamsAccepted proves ?refresh and ?routing don't cause an
// error (accepted, even though this single-node backend has no
// refresh-interval or shard-routing model to apply them to).
func TestBulk_QueryParamsAccepted(t *testing.T) {
	t.Parallel()

	h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
	createTestDomain(t, h, "qp-dom")
	createTestIndex(t, h, "qp-dom", "items")

	ndjson := "{\"index\":{\"_index\":\"items\",\"_id\":\"i1\"}}\n{\"name\":\"alpha\"}\n"
	status, body := doBulkRequest(
		t, h, "/2021-01-01/opensearch/domain/qp-dom/_bulk?refresh=wait_for&routing=shard-a",
		"application/x-ndjson", ndjson,
	)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, false, body["errors"])
}

// TestBulk_IndexScoped_DefaultsIndexFromPath proves POST
// {domainName}/index/{indexName}/_bulk supplies indexName as the default
// _index for an action line that omits one.
func TestBulk_IndexScoped_DefaultsIndexFromPath(t *testing.T) {
	t.Parallel()

	h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
	createTestDomain(t, h, "scoped-dom")
	createTestIndex(t, h, "scoped-dom", "widgets")

	ndjson := "{\"index\":{\"_id\":\"w1\"}}\n{\"name\":\"gadget\"}\n"
	status, body := doBulkRequest(
		t, h, "/2021-01-01/opensearch/domain/scoped-dom/index/widgets/_bulk", "application/x-ndjson", ndjson,
	)
	require.Equal(t, http.StatusOK, status)

	items, ok := body["items"].([]any)
	require.True(t, ok)
	require.Len(t, items, 1)

	item := bulkItem(t, items, 0, "index")
	assert.Equal(t, "widgets", item["_index"])
}

// TestBulk_ErrorItems covers the documented failure shapes: table-driven
// over independent domains so each case is isolated.
func TestBulk_ErrorItems(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setupNDJSON string
		ndjson      string
		wantAction  string
		wantType    string
		wantStatus  float64
	}{
		{
			name:        "create_on_existing_id_version_conflict",
			setupNDJSON: "{\"index\":{\"_index\":\"items\",\"_id\":\"dup\"}}\n{\"name\":\"a\"}\n",
			ndjson:      "{\"create\":{\"_index\":\"items\",\"_id\":\"dup\"}}\n{\"name\":\"b\"}\n",
			wantAction:  "create",
			wantStatus:  http.StatusConflict,
			wantType:    "version_conflict_engine_exception",
		},
		{
			name:       "delete_index_not_found",
			ndjson:     "{\"delete\":{\"_index\":\"no-such-index\",\"_id\":\"x\"}}\n",
			wantAction: "delete",
			wantStatus: http.StatusNotFound,
			wantType:   "index_not_found_exception",
		},
		{
			name:        "update_missing_document_no_upsert",
			setupNDJSON: "{\"index\":{\"_index\":\"items\",\"_id\":\"seed\"}}\n{\"name\":\"a\"}\n",
			ndjson:      "{\"update\":{\"_index\":\"items\",\"_id\":\"ghost\"}}\n{\"doc\":{\"name\":\"b\"}}\n",
			wantAction:  "update",
			wantStatus:  http.StatusNotFound,
			wantType:    "document_missing_exception",
		},
		{
			name:        "update_with_script_rejected",
			setupNDJSON: "{\"index\":{\"_index\":\"items\",\"_id\":\"seed\"}}\n{\"name\":\"a\"}\n",
			ndjson: "{\"update\":{\"_index\":\"items\",\"_id\":\"seed\"}}\n" +
				"{\"script\":{\"source\":\"ctx._source.name='b'\"}}\n",
			wantAction: "update",
			wantStatus: http.StatusBadRequest,
			wantType:   "illegal_argument_exception",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
			createTestDomain(t, h, "err-dom")
			createTestIndex(t, h, "err-dom", "items")

			if tt.setupNDJSON != "" {
				status, setupBody := doBulkRequest(
					t, h, "/2021-01-01/opensearch/domain/err-dom/_bulk", "application/x-ndjson", tt.setupNDJSON,
				)
				require.Equal(t, http.StatusOK, status)
				require.Equal(t, false, setupBody["errors"])
			}

			status, body := doBulkRequest(
				t, h, "/2021-01-01/opensearch/domain/err-dom/_bulk", "application/x-ndjson", tt.ndjson,
			)
			require.Equal(t, http.StatusOK, status, "the outer HTTP response is 200 even when an item fails")
			assert.Equal(t, true, body["errors"])

			items, ok := body["items"].([]any)
			require.True(t, ok)
			require.Len(t, items, 1)

			item := bulkItem(t, items, 0, tt.wantAction)
			assert.InDelta(t, tt.wantStatus, item["status"], 0)

			errObj, ok := item["error"].(map[string]any)
			require.True(t, ok, "error object must be present")
			assert.Equal(t, tt.wantType, errObj["type"])
			assert.NotEmpty(t, errObj["reason"])
		})
	}
}

// TestBulk_UpdateDocAsUpsert proves update's documented "doc_as_upsert"
// behavior: a missing document is created from doc instead of erroring.
func TestBulk_UpdateDocAsUpsert(t *testing.T) {
	t.Parallel()

	h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
	createTestDomain(t, h, "upsert-dom")
	createTestIndex(t, h, "upsert-dom", "items")

	ndjson := "{\"update\":{\"_index\":\"items\",\"_id\":\"new1\"}}\n" +
		"{\"doc\":{\"name\":\"fresh\"},\"doc_as_upsert\":true}\n"

	status, body := doBulkRequest(
		t, h, "/2021-01-01/opensearch/domain/upsert-dom/_bulk", "application/x-ndjson", ndjson,
	)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, false, body["errors"])

	items, ok := body["items"].([]any)
	require.True(t, ok)
	require.Len(t, items, 1)

	item := bulkItem(t, items, 0, "update")
	assert.Equal(t, "created", item["result"])
	assert.InDelta(t, http.StatusCreated, item["status"], 0)

	getResp := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/domain/upsert-dom/index/items/_doc/new1", nil)
	getBody := decodeBody(t, getResp)
	getResp.Body.Close()
	source, ok := getBody["_source"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "fresh", source["name"])
}

// TestBulk_MalformedBody proves a malformed NDJSON body fails the whole
// request (not silently ignored), matching the sibling _search/_doc routes'
// existing invalid-body handling.
func TestBulk_MalformedBody(t *testing.T) {
	t.Parallel()

	h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
	createTestDomain(t, h, "bad-dom")

	tests := []struct {
		name   string
		ndjson string
	}{
		{name: "invalid_json_line", ndjson: "not-json\n"},
		{name: "multiple_actions_on_one_line", ndjson: "{\"index\":{},\"delete\":{}}\n"},
		{name: "missing_source_line", ndjson: "{\"index\":{\"_index\":\"items\",\"_id\":\"i1\"}}\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, _ := doBulkRequest(
				t, h, "/2021-01-01/opensearch/domain/bad-dom/_bulk", "application/x-ndjson", tt.ndjson,
			)
			assert.Equal(t, http.StatusBadRequest, status)
		})
	}
}
