package cosmosdb_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

func newTestHandler(t *testing.T) *cosmosdb.Handler {
	t.Helper()

	backend := cosmosdb.NewInMemoryBackend()

	return cosmosdb.NewHandler(backend)
}

// doRequest builds an echo context for method/path/headers/body and invokes
// the handler directly, mirroring services/azuretable's doRequest.
func doRequest(
	t *testing.T, h *cosmosdb.Handler, method, path string, headers map[string]string, body []byte,
) *httptest.ResponseRecorder {
	t.Helper()

	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(method, path, bytes.NewReader(body))
	} else {
		req = httptest.NewRequest(method, path, http.NoBody)
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	e := echo.New()
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

func TestHandler_CommonHeaders(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/dbs", nil, nil)

	assert.NotEmpty(t, rec.Header().Get("X-Ms-Version"))
	assert.NotEmpty(t, rec.Header().Get("X-Ms-Request-Id"))
	assert.NotEmpty(t, rec.Header().Get("Date"))
	assert.Equal(t, "1", rec.Header().Get("X-Ms-Request-Charge"))
	assert.NotEmpty(t, rec.Header().Get("X-Ms-Session-Token"))
	assert.NotEmpty(t, rec.Header().Get("X-Ms-Activity-Id"))
}

func TestHandler_InvalidURI(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name string
		path string
	}{
		{name: "root", path: "/"},
		{name: "not dbs", path: "/foo"},
		{name: "too many segments", path: "/dbs/a/colls/b/docs/c/extra"},
		{name: "colls typo", path: "/dbs/a/bogus"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, http.MethodGet, tt.path, nil, nil)
			assert.Equal(t, http.StatusBadRequest, rec.Code, tt.name)
		})
	}
}

func TestHandler_DatabaseLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createBody, err := json.Marshal(map[string]string{"id": "mydb"})
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPost, "/dbs", nil, createBody)
	require.Equal(t, http.StatusCreated, rec.Code)
	assert.NotEmpty(t, rec.Header().Get("ETag"))

	body := decodeBody(t, rec)
	assert.Equal(t, "mydb", body["id"])
	assert.NotEmpty(t, body["_rid"])

	// Duplicate create -> 409.
	rec = doRequest(t, h, http.MethodPost, "/dbs", nil, createBody)
	assert.Equal(t, http.StatusConflict, rec.Code)

	// Get.
	rec = doRequest(t, h, http.MethodGet, "/dbs/mydb", nil, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// Get missing -> 404.
	rec = doRequest(t, h, http.MethodGet, "/dbs/nope", nil, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// List.
	rec = doRequest(t, h, http.MethodGet, "/dbs", nil, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	list := decodeBody(t, rec)
	dbs, ok := list["Databases"].([]any)
	require.True(t, ok)
	assert.Len(t, dbs, 1)
	assert.InDelta(t, 1, list["_count"], 0)

	// Delete.
	rec = doRequest(t, h, http.MethodDelete, "/dbs/mydb", nil, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	rec = doRequest(t, h, http.MethodDelete, "/dbs/mydb", nil, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_CreateDatabase_InvalidInput(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name string
		body string
	}{
		{name: "not json", body: "not json"},
		{name: "empty id", body: `{"id":""}`},
		{name: "missing id", body: `{}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, http.MethodPost, "/dbs", nil, []byte(tt.body))
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// createTestDatabase creates database "mydb" and returns the handler.
func createTestDatabase(t *testing.T, h *cosmosdb.Handler) {
	t.Helper()

	body, err := json.Marshal(map[string]string{"id": "mydb"})
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPost, "/dbs", nil, body)
	require.Equal(t, http.StatusCreated, rec.Code)
}

func createTestContainer(t *testing.T, h *cosmosdb.Handler) {
	t.Helper()

	body, err := json.Marshal(map[string]any{
		"id":           "mycoll",
		"partitionKey": map[string]any{"paths": []string{"/pk"}, "kind": "Hash"},
	})
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPost, "/dbs/mydb/colls", nil, body)
	require.Equal(t, http.StatusCreated, rec.Code)
}

func TestHandler_ContainerLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestDatabase(t, h)

	body, err := json.Marshal(map[string]any{
		"id":           "mycoll",
		"partitionKey": map[string]any{"paths": []string{"/pk"}, "kind": "Hash"},
	})
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPost, "/dbs/mydb/colls", nil, body)
	require.Equal(t, http.StatusCreated, rec.Code)

	respBody := decodeBody(t, rec)
	assert.Equal(t, "mycoll", respBody["id"])
	pkDef, ok := respBody["partitionKey"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Hash", pkDef["kind"])

	// Duplicate -> 409.
	rec = doRequest(t, h, http.MethodPost, "/dbs/mydb/colls", nil, body)
	assert.Equal(t, http.StatusConflict, rec.Code)

	// Against missing database -> 404.
	rec = doRequest(t, h, http.MethodPost, "/dbs/nope/colls", nil, body)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Get.
	rec = doRequest(t, h, http.MethodGet, "/dbs/mydb/colls/mycoll", nil, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List.
	rec = doRequest(t, h, http.MethodGet, "/dbs/mydb/colls", nil, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	list := decodeBody(t, rec)
	colls, ok := list["DocumentCollections"].([]any)
	require.True(t, ok)
	assert.Len(t, colls, 1)

	// Delete.
	rec = doRequest(t, h, http.MethodDelete, "/dbs/mydb/colls/mycoll", nil, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/dbs/mydb/colls/mycoll", nil, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_CreateContainer_InvalidPartitionKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestDatabase(t, h)

	body, err := json.Marshal(map[string]any{
		"id":           "mycoll",
		"partitionKey": map[string]any{"paths": []string{"/a", "/b"}, "kind": "Hash"},
	})
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPost, "/dbs/mydb/colls", nil, body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DocumentLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestDatabase(t, h)
	createTestContainer(t, h)

	docBody, err := json.Marshal(map[string]any{"id": "doc1", "pk": "partA", "value": 42})
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPost, "/dbs/mydb/colls/mycoll/docs", nil, docBody)
	require.Equal(t, http.StatusCreated, rec.Code)
	etag := rec.Header().Get("ETag")
	require.NotEmpty(t, etag)

	created := decodeBody(t, rec)
	assert.Equal(t, "doc1", created["id"])
	assert.NotEmpty(t, created["_rid"])
	assert.NotEmpty(t, created["_self"])
	assert.NotEmpty(t, created["_attachments"])

	// Duplicate create (no upsert) -> 409.
	rec = doRequest(t, h, http.MethodPost, "/dbs/mydb/colls/mycoll/docs", nil, docBody)
	assert.Equal(t, http.StatusConflict, rec.Code)

	// Upsert succeeds.
	rec = doRequest(t, h, http.MethodPost, "/dbs/mydb/colls/mycoll/docs",
		map[string]string{"X-Ms-Documentdb-Is-Upsert": "true"}, docBody)
	assert.Equal(t, http.StatusCreated, rec.Code)

	// Get requires partition key header.
	rec = doRequest(t, h, http.MethodGet, "/dbs/mydb/colls/mycoll/docs/doc1", nil, nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	pkHeader := map[string]string{"X-Ms-Documentdb-Partitionkey": `["partA"]`}

	rec = doRequest(t, h, http.MethodGet, "/dbs/mydb/colls/mycoll/docs/doc1", pkHeader, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	got := decodeBody(t, rec)
	assert.InDelta(t, 42, got["value"], 0.0001)

	// Get missing document -> 404.
	rec = doRequest(t, h, http.MethodGet, "/dbs/mydb/colls/mycoll/docs/nope", pkHeader, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Replace with wrong If-Match -> 412.
	replaceBody, err := json.Marshal(map[string]any{"pk": "partA", "value": 100})
	require.NoError(t, err)

	wrongEtagHeaders := map[string]string{
		"X-Ms-Documentdb-Partitionkey": `["partA"]`, "If-Match": `"bogus"`,
	}
	rec = doRequest(t, h, http.MethodPut, "/dbs/mydb/colls/mycoll/docs/doc1", wrongEtagHeaders, replaceBody)
	assert.Equal(t, http.StatusPreconditionFailed, rec.Code)

	// Unconditional replace succeeds and drops unrelated fields.
	rec = doRequest(t, h, http.MethodPut, "/dbs/mydb/colls/mycoll/docs/doc1", pkHeader, replaceBody)
	require.Equal(t, http.StatusOK, rec.Code)

	replaced := decodeBody(t, rec)
	assert.InDelta(t, 100, replaced["value"], 0.0001)
	newEtag := rec.Header().Get("ETag")
	assert.NotEqual(t, etag, newEtag)

	// Delete with wrong ETag fails.
	rec = doRequest(t, h, http.MethodDelete, "/dbs/mydb/colls/mycoll/docs/doc1", wrongEtagHeaders, nil)
	assert.Equal(t, http.StatusPreconditionFailed, rec.Code)

	// Delete with correct ETag succeeds.
	correctEtagHeaders := map[string]string{
		"X-Ms-Documentdb-Partitionkey": `["partA"]`, "If-Match": newEtag,
	}
	rec = doRequest(t, h, http.MethodDelete, "/dbs/mydb/colls/mycoll/docs/doc1", correctEtagHeaders, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Verify gone.
	rec = doRequest(t, h, http.MethodGet, "/dbs/mydb/colls/mycoll/docs/doc1", pkHeader, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_ReadFeed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestDatabase(t, h)
	createTestContainer(t, h)

	for _, id := range []string{"a", "b"} {
		body, err := json.Marshal(map[string]any{"id": id, "pk": "x"})
		require.NoError(t, err)

		rec := doRequest(t, h, http.MethodPost, "/dbs/mydb/colls/mycoll/docs", nil, body)
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	rec := doRequest(t, h, http.MethodGet, "/dbs/mydb/colls/mycoll/docs", nil, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	feed := decodeBody(t, rec)
	assert.InDelta(t, 2, feed["_count"], 0)
}

func TestHandler_Query(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestDatabase(t, h)
	createTestContainer(t, h)

	for i, name := range []string{"alice", "bob"} {
		body, err := json.Marshal(map[string]any{"id": name, "pk": "x", "n": i})
		require.NoError(t, err)

		rec := doRequest(t, h, http.MethodPost, "/dbs/mydb/colls/mycoll/docs", nil, body)
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	queryBody, err := json.Marshal(map[string]any{"query": "SELECT * FROM c WHERE c.id = 'alice'"})
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPost, "/dbs/mydb/colls/mycoll/docs",
		map[string]string{"X-Ms-Documentdb-Isquery": "True", "Content-Type": "application/query+json"}, queryBody)
	require.Equal(t, http.StatusOK, rec.Code)

	result := decodeBody(t, rec)
	assert.InDelta(t, 1, result["_count"], 0)

	// A malformed query yields 400, never a panic.
	badBody, err := json.Marshal(map[string]any{"query": "NOT VALID SQL AT ALL ((("})
	require.NoError(t, err)

	rec = doRequest(t, h, http.MethodPost, "/dbs/mydb/colls/mycoll/docs",
		map[string]string{"X-Ms-Documentdb-Isquery": "true"}, badBody)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPut, "/dbs", nil, nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_ResetClearsState(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestDatabase(t, h)

	h.Reset()

	rec := doRequest(t, h, http.MethodGet, "/dbs/mydb", nil, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestParseResourcePath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string
		wantDB   string
		wantColl string
		wantDoc  string
		wantKind int
	}{
		{name: "databases", path: "/dbs", wantKind: 1},
		{name: "database item", path: "/dbs/mydb", wantKind: 2, wantDB: "mydb"},
		{name: "containers", path: "/dbs/mydb/colls", wantKind: 3, wantDB: "mydb"},
		{name: "container item", path: "/dbs/mydb/colls/mycoll", wantKind: 4, wantDB: "mydb", wantColl: "mycoll"},
		{name: "documents", path: "/dbs/mydb/colls/mycoll/docs", wantKind: 5, wantDB: "mydb", wantColl: "mycoll"},
		{
			name: "document item", path: "/dbs/mydb/colls/mycoll/docs/doc1", wantKind: 6,
			wantDB: "mydb", wantColl: "mycoll", wantDoc: "doc1",
		},
		{name: "empty", path: "/", wantKind: 0},
		{name: "not dbs", path: "/foo", wantKind: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			kind, db, coll, doc := cosmosdb.ParseResourcePath(tt.path)
			assert.Equal(t, tt.wantKind, kind)
			assert.Equal(t, tt.wantDB, db)
			assert.Equal(t, tt.wantColl, coll)
			assert.Equal(t, tt.wantDoc, doc)
		})
	}
}

func TestIsQueryRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		headers map[string]string
		name    string
		want    bool
	}{
		{name: "isquery true", headers: map[string]string{"X-Ms-Documentdb-Isquery": "True"}, want: true},
		{name: "isquery lowercase", headers: map[string]string{"X-Ms-Documentdb-Isquery": "true"}, want: true},
		{name: "content type", headers: map[string]string{"Content-Type": "application/query+json"}, want: true},
		{name: "neither", headers: map[string]string{}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/dbs/a/colls/b/docs", http.NoBody)
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			assert.Equal(t, tt.want, cosmosdb.IsQueryRequest(req))
		})
	}
}
