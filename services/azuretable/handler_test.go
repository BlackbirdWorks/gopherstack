package azuretable_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuretable"
)

const testAccount = "devstoreaccount1"

func newTestHandler(t *testing.T) *azuretable.Handler {
	t.Helper()

	backend := azuretable.NewInMemoryBackend()

	return azuretable.NewHandler(backend)
}

// doRequest builds an echo context for method/path (with optional body) and
// invokes the handler directly, mirroring services/azurequeue's doRequest.
func doRequest(t *testing.T, h *azuretable.Handler, method, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()

	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(method, path, bytes.NewReader(body))
	} else {
		req = httptest.NewRequest(method, path, http.NoBody)
	}

	e := echo.New()
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func TestHandler_CommonHeaders(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/"+testAccount+"/Tables", nil)

	assert.NotEmpty(t, rec.Header().Get("X-Ms-Version"))
	assert.NotEmpty(t, rec.Header().Get("X-Ms-Request-Id"))
	assert.NotEmpty(t, rec.Header().Get("Date"))
	assert.Equal(t, "3.0;", rec.Header().Get("Dataserviceversion"))
}

func TestHandler_InvalidURI(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name string
		path string
	}{
		{name: "root", path: "/"},
		{name: "account_only", path: "/" + testAccount},
		{name: "account_only_with_slash", path: "/" + testAccount + "/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, http.MethodGet, tt.path, nil)
			assert.Equal(t, http.StatusBadRequest, rec.Code, tt.name)
			assert.Equal(t, "InvalidUri", rec.Header().Get("X-Ms-Error-Code"), tt.name)
		})
	}
}

func TestHandler_Batch_NotImplemented(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/"+testAccount+"/$batch", []byte("--batch"))

	assert.Equal(t, http.StatusNotImplemented, rec.Code)
	assert.Equal(t, "NotImplemented", rec.Header().Get("X-Ms-Error-Code"))
	assert.Contains(t, rec.Body.String(), "PARITY.md")
}

func TestHandler_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{name: "tables_collection_delete", method: http.MethodDelete, path: "/" + testAccount + "/Tables"},
		{name: "tables_item_get", method: http.MethodGet, path: "/" + testAccount + "/Tables('foo')"},
		{name: "entity_collection_put", method: http.MethodPut, path: "/" + testAccount + "/mytable"},
		{
			name: "entity_item_post", method: http.MethodPost,
			path: "/" + testAccount + "/mytable(PartitionKey='p',RowKey='r')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, tt.method, tt.path, nil)
			assert.Equal(t, http.StatusMethodNotAllowed, rec.Code, tt.name)
			assert.Equal(t, "UnsupportedHttpVerb", rec.Header().Get("X-Ms-Error-Code"), tt.name)
		})
	}
}

func TestHandler_EntityItem_InvalidKeyPredicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/"+testAccount+"/mytable(garbage)", nil)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "InvalidInput", rec.Header().Get("X-Ms-Error-Code"))
}

func TestSplitPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		path         string
		wantAccount  string
		wantResource string
	}{
		{name: "empty", path: "", wantAccount: "", wantResource: ""},
		{name: "account_only", path: "/acct", wantAccount: "acct", wantResource: ""},
		{name: "tables", path: "/acct/Tables", wantAccount: "acct", wantResource: "Tables"},
		{
			name: "entity_item", path: "/acct/tbl(PartitionKey='p',RowKey='r')",
			wantAccount: "acct", wantResource: "tbl(PartitionKey='p',RowKey='r')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			account, resource := azuretable.SplitPath(tt.path)
			assert.Equal(t, tt.wantAccount, account, tt.name)
			assert.Equal(t, tt.wantResource, resource, tt.name)
		})
	}
}

func TestParseResource(t *testing.T) {
	t.Parallel()

	const (
		kindInvalid = iota
		kindBatch
		kindTablesCollection
		kindTablesItem
		kindEntityCollection
		kindEntityItem
	)

	tests := []struct {
		name      string
		resource  string
		wantName  string
		wantInner string
		wantKind  int
	}{
		{name: "batch", resource: "$batch", wantKind: kindBatch},
		{name: "tables_collection", resource: "Tables", wantKind: kindTablesCollection, wantName: "Tables"},
		{
			name: "tables_item", resource: "Tables('foo')", wantKind: kindTablesItem,
			wantName: "Tables", wantInner: "'foo'",
		},
		{name: "entity_collection_bare", resource: "mytable", wantKind: kindEntityCollection, wantName: "mytable"},
		{
			name: "entity_collection_empty_parens", resource: "mytable()", wantKind: kindEntityCollection,
			wantName: "mytable",
		},
		{
			name: "entity_item", resource: "mytable(PartitionKey='p',RowKey='r')", wantKind: kindEntityItem,
			wantName: "mytable", wantInner: "PartitionKey='p',RowKey='r'",
		},
		{name: "unclosed_paren", resource: "mytable(foo", wantKind: kindInvalid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			kind, name, inner := azuretable.ParseResource(tt.resource)
			assert.Equal(t, tt.wantKind, kind, tt.name)
			assert.Equal(t, tt.wantName, name, tt.name)
			assert.Equal(t, tt.wantInner, inner, tt.name)
		})
	}
}

func TestParseEntityKeyPredicate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		predicate string
		wantPK    string
		wantRK    string
		wantOK    bool
	}{
		{name: "normal_order", predicate: "PartitionKey='p',RowKey='r'", wantPK: "p", wantRK: "r", wantOK: true},
		{name: "reversed_order", predicate: "RowKey='r',PartitionKey='p'", wantPK: "p", wantRK: "r", wantOK: true},
		{
			name: "escaped_quote", predicate: "PartitionKey='p''q',RowKey='r'",
			wantPK: "p'q", wantRK: "r", wantOK: true,
		},
		{
			name:      "comma_inside_value",
			predicate: "PartitionKey='p,q',RowKey='r'",
			wantPK:    "p,q",
			wantRK:    "r",
			wantOK:    true,
		},
		{name: "empty_keys", predicate: "PartitionKey='',RowKey=''", wantPK: "", wantRK: "", wantOK: true},
		{name: "missing_rowkey", predicate: "PartitionKey='p'", wantOK: false},
		{name: "unknown_key", predicate: "Foo='p',RowKey='r'", wantOK: false},
		{name: "no_equals", predicate: "PartitionKeyp,RowKey='r'", wantOK: false},
		{name: "garbage", predicate: "garbage", wantOK: false},
		{name: "empty", predicate: "", wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pk, rk, ok := azuretable.ParseEntityKeyPredicate(tt.predicate)
			assert.Equal(t, tt.wantOK, ok, tt.name)

			if tt.wantOK {
				assert.Equal(t, tt.wantPK, pk, tt.name)
				assert.Equal(t, tt.wantRK, rk, tt.name)
			}
		})
	}
}

func TestUnquoteODataString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		in     string
		want   string
		wantOK bool
	}{
		{name: "simple", in: "'foo'", want: "foo", wantOK: true},
		{name: "escaped_quote", in: "'foo''bar'", want: "foo'bar", wantOK: true},
		{name: "empty", in: "''", want: "", wantOK: true},
		{name: "too_short", in: "'", wantOK: false},
		{name: "no_quotes", in: "foo", wantOK: false},
		{name: "unescaped_quote_inside", in: "'foo'bar'", wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := azuretable.UnquoteODataString(tt.in)
			assert.Equal(t, tt.wantOK, ok, tt.name)

			if tt.wantOK {
				assert.Equal(t, tt.want, got, tt.name)
			}
		})
	}
}

func TestExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		want   string
	}{
		{name: "list_tables", method: http.MethodGet, path: "/" + testAccount + "/Tables", want: "ListTables"},
		{name: "create_table", method: http.MethodPost, path: "/" + testAccount + "/Tables", want: "CreateTable"},
		{
			name: "delete_table", method: http.MethodDelete, path: "/" + testAccount + "/Tables('foo')",
			want: "DeleteTable",
		},
		{name: "insert_entity", method: http.MethodPost, path: "/" + testAccount + "/mytable", want: "InsertEntity"},
		{name: "query_entities", method: http.MethodGet, path: "/" + testAccount + "/mytable()", want: "QueryEntities"},
		{
			name: "get_entity", method: http.MethodGet,
			path: "/" + testAccount + "/mytable(PartitionKey='p',RowKey='r')", want: "GetEntity",
		},
		{
			name: "replace_entity", method: http.MethodPut,
			path: "/" + testAccount + "/mytable(PartitionKey='p',RowKey='r')", want: "ReplaceEntity",
		},
		{
			name: "merge_entity_patch", method: http.MethodPatch,
			path: "/" + testAccount + "/mytable(PartitionKey='p',RowKey='r')", want: "MergeEntity",
		},
		{
			name: "merge_entity_literal", method: "MERGE",
			path: "/" + testAccount + "/mytable(PartitionKey='p',RowKey='r')", want: "MergeEntity",
		},
		{
			name: "delete_entity", method: http.MethodDelete,
			path: "/" + testAccount + "/mytable(PartitionKey='p',RowKey='r')", want: "DeleteEntity",
		},
		{name: "batch", method: http.MethodPost, path: "/" + testAccount + "/$batch", want: "Batch"},
		{name: "unknown", method: http.MethodOptions, path: "/" + testAccount + "/Tables", want: "Unknown"},
	}

	h := newTestHandler(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, http.NoBody)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.want, h.ExtractOperation(c), tt.name)
		})
	}
}

func TestExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/"+testAccount+"/mytable", http.NoBody)
	c := e.NewContext(req, httptest.NewRecorder())

	assert.Equal(t, "mytable", h.ExtractResource(c))
}

func TestRouteMatcher_AlwaysFalse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	matcher := h.RouteMatcher()

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/"+testAccount+"/Tables", http.NoBody)
	c := e.NewContext(req, httptest.NewRecorder())

	assert.False(t, matcher(c))
}

func TestMatchPriority_Lowest(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	assert.Equal(t, 0, h.MatchPriority())
}

func TestGetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	assert.NotEmpty(t, h.GetSupportedOperations())
}

func TestName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	assert.Equal(t, "AzureTable", h.Name())
}

func TestCheckAuth_StructurallyValidHeaderAccepted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/"+testAccount+"/Tables", http.NoBody)
	req.Header.Set("Authorization", "SharedKey devstoreaccount1:c2lnbmF0dXJl")

	e := echo.New()
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestCheckAuth_MalformedHeaderStillAccepted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/"+testAccount+"/Tables", http.NoBody)
	req.Header.Set("Authorization", "not-a-real-header")

	e := echo.New()
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestReset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := []byte(`{"TableName":"foo"}`)
	rec := doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	h.Reset()

	rec = doRequest(t, h, http.MethodGet, "/"+testAccount+"/Tables", nil)
	assert.JSONEq(t, `{"value":[]}`, rec.Body.String())
}
