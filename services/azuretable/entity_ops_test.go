package azuretable_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuretable"
)

func TestInsertEntity(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"mytable"}`))

		body := []byte(`{"PartitionKey":"p","RowKey":"r","Name":"hi"}`)
		rec := doRequest(t, h, http.MethodPost, "/"+testAccount+"/mytable", body)

		require.Equal(t, http.StatusCreated, rec.Code)
		assert.NotEmpty(t, rec.Header().Get("ETag"))

		var got map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
		assert.Equal(t, "p", got["PartitionKey"])
		assert.Equal(t, "r", got["RowKey"])
		assert.Equal(t, "hi", got["Name"])
		assert.NotEmpty(t, got["Timestamp"])
	})

	t.Run("prefer_return_no_content", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"mytable"}`))

		req := httptest.NewRequest(
			http.MethodPost, "/"+testAccount+"/mytable", strings.NewReader(`{"PartitionKey":"p","RowKey":"r"}`),
		)
		req.Header.Set("Prefer", "return-no-content")

		e := echo.New()
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		require.NoError(t, h.Handler()(c))

		assert.Equal(t, http.StatusNoContent, rec.Code)
		assert.Equal(t, "return-no-content", rec.Header().Get("Preference-Applied"))
		assert.NotEmpty(t, rec.Header().Get("ETag"))
	})

	t.Run("table_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(
			t,
			h,
			http.MethodPost,
			"/"+testAccount+"/nosuchtable",
			[]byte(`{"PartitionKey":"p","RowKey":"r"}`),
		)
		assert.Equal(t, http.StatusNotFound, rec.Code)
		assert.Equal(t, "TableNotFound", rec.Header().Get("X-Ms-Error-Code"))
	})

	t.Run("duplicate_conflict", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"mytable"}`))
		body := []byte(`{"PartitionKey":"p","RowKey":"r"}`)
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/mytable", body)

		rec := doRequest(t, h, http.MethodPost, "/"+testAccount+"/mytable", body)
		assert.Equal(t, http.StatusConflict, rec.Code)
		assert.Equal(t, "EntityAlreadyExists", rec.Header().Get("X-Ms-Error-Code"))
	})

	t.Run("missing_keys_rejected", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"mytable"}`))

		rec := doRequest(t, h, http.MethodPost, "/"+testAccount+"/mytable", []byte(`{"RowKey":"r"}`))
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Equal(t, "InvalidInput", rec.Header().Get("X-Ms-Error-Code"))
	})

	t.Run("empty_keys_accepted", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"mytable"}`))

		rec := doRequest(t, h, http.MethodPost, "/"+testAccount+"/mytable", []byte(`{"PartitionKey":"","RowKey":""}`))
		assert.Equal(t, http.StatusCreated, rec.Code)
	})

	t.Run("malformed_body", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"mytable"}`))

		rec := doRequest(t, h, http.MethodPost, "/"+testAccount+"/mytable", []byte(`not json`))
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

// TestEntity_EDMTypeRoundTrip covers a round trip per supported EDM type:
// insert an entity carrying one property of that type, then GET it back and
// assert both the value and (where applicable) the "@odata.type" annotation
// survive.
func TestEntity_EDMTypeRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		insertBody string
		wantValue  any
		wantAnn    string
		annPresent bool
	}{
		{name: "string", insertBody: `"Prop":"hello"`, wantValue: "hello"},
		{name: "int32", insertBody: `"Prop":42`, wantValue: float64(42)},
		{
			name: "int64", insertBody: `"Prop":"9223372036854775807","Prop@odata.type":"Edm.Int64"`,
			wantValue: "9223372036854775807", wantAnn: "Edm.Int64", annPresent: true,
		},
		{name: "double_fractional", insertBody: `"Prop":3.14`, wantValue: 3.14},
		{name: "boolean", insertBody: `"Prop":true`, wantValue: true},
		{
			name: "datetime", insertBody: `"Prop":"2024-01-02T03:04:05.1234567Z","Prop@odata.type":"Edm.DateTime"`,
			wantValue: "2024-01-02T03:04:05.1234567Z", wantAnn: "Edm.DateTime", annPresent: true,
		},
		{
			name: "guid", insertBody: `"Prop":"550e8400-e29b-41d4-a716-446655440000","Prop@odata.type":"Edm.Guid"`,
			wantValue: "550e8400-e29b-41d4-a716-446655440000", wantAnn: "Edm.Guid", annPresent: true,
		},
		{
			name: "binary",
			insertBody: `"Prop":"` + base64.StdEncoding.EncodeToString([]byte("hi there")) +
				`","Prop@odata.type":"Edm.Binary"`,
			wantValue: base64.StdEncoding.EncodeToString([]byte("hi there")), wantAnn: "Edm.Binary", annPresent: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"mytable"}`))

			insertBody := []byte(`{"PartitionKey":"p","RowKey":"r",` + tt.insertBody + `}`)
			rec := doRequest(t, h, http.MethodPost, "/"+testAccount+"/mytable", insertBody)
			require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

			rec = doRequest(t, h, http.MethodGet,
				"/"+testAccount+"/mytable(PartitionKey='p',RowKey='r')", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var got map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
			assert.Equal(t, tt.wantValue, got["Prop"], tt.name)

			if tt.annPresent {
				assert.Equal(t, tt.wantAnn, got["Prop@odata.type"], tt.name)
			} else {
				assert.NotContains(t, got, "Prop@odata.type", tt.name)
			}
		})
	}
}

func TestGetEntity(t *testing.T) {
	t.Parallel()

	t.Run("not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"mytable"}`))

		rec := doRequest(t, h, http.MethodGet, "/"+testAccount+"/mytable(PartitionKey='p',RowKey='r')", nil)
		assert.Equal(t, http.StatusNotFound, rec.Code)
		assert.Equal(t, "ResourceNotFound", rec.Header().Get("X-Ms-Error-Code"))
	})

	t.Run("table_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, http.MethodGet, "/"+testAccount+"/nosuch(PartitionKey='p',RowKey='r')", nil)
		assert.Equal(t, http.StatusNotFound, rec.Code)
		assert.Equal(t, "TableNotFound", rec.Header().Get("X-Ms-Error-Code"))
	})

	t.Run("select_projection", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"mytable"}`))
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/mytable",
			[]byte(`{"PartitionKey":"p","RowKey":"r","A":"x","B":"y"}`))

		rec := doRequest(t, h, http.MethodGet,
			"/"+testAccount+"/mytable(PartitionKey='p',RowKey='r')?$select=A", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var got map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
		assert.Equal(t, "x", got["A"])
		assert.NotContains(t, got, "B")
		assert.Contains(t, got, "PartitionKey")
		assert.Contains(t, got, "RowKey")
	})
}

func TestQueryEntities(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T) *azuretable.Handler {
		t.Helper()

		h := newTestHandler(t)
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"mytable"}`))
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/mytable",
			[]byte(`{"PartitionKey":"p1","RowKey":"r1","Age":10}`))
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/mytable",
			[]byte(`{"PartitionKey":"p2","RowKey":"r1","Age":20}`))

		return h
	}

	t.Run("no_filter_returns_all_sorted", func(t *testing.T) {
		t.Parallel()

		h := setup(t)
		rec := doRequest(t, h, http.MethodGet, "/"+testAccount+"/mytable()", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var body struct {
			Value []map[string]any `json:"value"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		require.Len(t, body.Value, 2)
		assert.Equal(t, "p1", body.Value[0]["PartitionKey"])
		assert.Equal(t, "p2", body.Value[1]["PartitionKey"])
	})

	t.Run("filter_eq", func(t *testing.T) {
		t.Parallel()

		h := setup(t)
		rec := doRequest(t, h, http.MethodGet,
			"/"+testAccount+"/mytable()?$filter=Age%20eq%2020", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var body struct {
			Value []map[string]any `json:"value"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		require.Len(t, body.Value, 1)
		assert.Equal(t, "p2", body.Value[0]["PartitionKey"])
	})

	t.Run("filter_parse_error", func(t *testing.T) {
		t.Parallel()

		h := setup(t)
		rec := doRequest(t, h, http.MethodGet,
			"/"+testAccount+"/mytable()?$filter=Age%20eq", nil)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Equal(t, "InvalidInput", rec.Header().Get("X-Ms-Error-Code"))
	})

	t.Run("top_caps_results", func(t *testing.T) {
		t.Parallel()

		h := setup(t)
		rec := doRequest(t, h, http.MethodGet, "/"+testAccount+"/mytable()?$top=1", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var body struct {
			Value []map[string]any `json:"value"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Len(t, body.Value, 1)
	})

	t.Run("invalid_top_rejected", func(t *testing.T) {
		t.Parallel()

		h := setup(t)
		rec := doRequest(t, h, http.MethodGet, "/"+testAccount+"/mytable()?$top=-5", nil)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("table_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, http.MethodGet, "/"+testAccount+"/nosuch()", nil)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

func TestReplaceEntity(t *testing.T) {
	t.Parallel()

	t.Run("upsert_creates_when_absent", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"mytable"}`))

		rec := doRequest(t, h, http.MethodPut,
			"/"+testAccount+"/mytable(PartitionKey='p',RowKey='r')", []byte(`{"A":"x"}`))
		assert.Equal(t, http.StatusNoContent, rec.Code)
		assert.NotEmpty(t, rec.Header().Get("ETag"))
	})

	t.Run("if_match_star_requires_existing", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"mytable"}`))

		req := httptest.NewRequest(http.MethodPut,
			"/"+testAccount+"/mytable(PartitionKey='p',RowKey='r')", strings.NewReader(`{"A":"x"}`))
		req.Header.Set("If-Match", "*")

		e := echo.New()
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		require.NoError(t, h.Handler()(c))
		assert.Equal(t, http.StatusNotFound, rec.Code)
		assert.Equal(t, "ResourceNotFound", rec.Header().Get("X-Ms-Error-Code"))
	})

	t.Run("etag_mismatch_412", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"mytable"}`))
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/mytable",
			[]byte(`{"PartitionKey":"p","RowKey":"r"}`))

		req := httptest.NewRequest(http.MethodPut,
			"/"+testAccount+"/mytable(PartitionKey='p',RowKey='r')", strings.NewReader(`{"A":"x"}`))
		req.Header.Set("If-Match", `W/"datetime'bogus'"`)

		e := echo.New()
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		require.NoError(t, h.Handler()(c))
		assert.Equal(t, http.StatusPreconditionFailed, rec.Code)
		assert.Equal(t, "UpdateConditionNotSatisfied", rec.Header().Get("X-Ms-Error-Code"))
	})

	t.Run("replace_drops_unlisted_properties", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"mytable"}`))
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/mytable",
			[]byte(`{"PartitionKey":"p","RowKey":"r","A":"x","B":"y"}`))

		rec := doRequest(t, h, http.MethodPut,
			"/"+testAccount+"/mytable(PartitionKey='p',RowKey='r')", []byte(`{"A":"z"}`))
		require.Equal(t, http.StatusNoContent, rec.Code)

		rec = doRequest(t, h, http.MethodGet, "/"+testAccount+"/mytable(PartitionKey='p',RowKey='r')", nil)
		var got map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
		assert.Equal(t, "z", got["A"])
		assert.NotContains(t, got, "B")
	})
}

func TestMergeEntity(t *testing.T) {
	t.Parallel()

	t.Run("merge_keeps_unlisted_properties", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"mytable"}`))
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/mytable",
			[]byte(`{"PartitionKey":"p","RowKey":"r","A":"x","B":"y"}`))

		req := httptest.NewRequest(http.MethodPatch,
			"/"+testAccount+"/mytable(PartitionKey='p',RowKey='r')", strings.NewReader(`{"A":"z"}`))
		e := echo.New()
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		require.NoError(t, h.Handler()(c))
		require.Equal(t, http.StatusNoContent, rec.Code)

		rec2 := doRequest(t, h, http.MethodGet, "/"+testAccount+"/mytable(PartitionKey='p',RowKey='r')", nil)
		var got map[string]any
		require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &got))
		assert.Equal(t, "z", got["A"])
		assert.Equal(t, "y", got["B"])
	})

	t.Run("literal_merge_method", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"mytable"}`))
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/mytable",
			[]byte(`{"PartitionKey":"p","RowKey":"r","A":"x"}`))

		req := httptest.NewRequest("MERGE",
			"/"+testAccount+"/mytable(PartitionKey='p',RowKey='r')", strings.NewReader(`{"A":"z"}`))
		e := echo.New()
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		require.NoError(t, h.Handler()(c))
		assert.Equal(t, http.StatusNoContent, rec.Code)
	})

	t.Run("upsert_creates_when_absent", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"mytable"}`))

		req := httptest.NewRequest(http.MethodPatch,
			"/"+testAccount+"/mytable(PartitionKey='p',RowKey='r')", strings.NewReader(`{"A":"x"}`))
		e := echo.New()
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		require.NoError(t, h.Handler()(c))
		assert.Equal(t, http.StatusNoContent, rec.Code)
	})
}

func TestDeleteEntity(t *testing.T) {
	t.Parallel()

	t.Run("missing_if_match_rejected", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"mytable"}`))
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/mytable",
			[]byte(`{"PartitionKey":"p","RowKey":"r"}`))

		rec := doRequest(t, h, http.MethodDelete, "/"+testAccount+"/mytable(PartitionKey='p',RowKey='r')", nil)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Equal(t, "InvalidInput", rec.Header().Get("X-Ms-Error-Code"))
	})

	t.Run("if_match_star_succeeds", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"mytable"}`))
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/mytable",
			[]byte(`{"PartitionKey":"p","RowKey":"r"}`))

		req := httptest.NewRequest(http.MethodDelete,
			"/"+testAccount+"/mytable(PartitionKey='p',RowKey='r')", http.NoBody)
		req.Header.Set("If-Match", "*")
		e := echo.New()
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		require.NoError(t, h.Handler()(c))
		assert.Equal(t, http.StatusNoContent, rec.Code)

		rec2 := doRequest(t, h, http.MethodGet, "/"+testAccount+"/mytable(PartitionKey='p',RowKey='r')", nil)
		assert.Equal(t, http.StatusNotFound, rec2.Code)
	})

	t.Run("etag_mismatch_412", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"mytable"}`))
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/mytable",
			[]byte(`{"PartitionKey":"p","RowKey":"r"}`))

		req := httptest.NewRequest(http.MethodDelete,
			"/"+testAccount+"/mytable(PartitionKey='p',RowKey='r')", http.NoBody)
		req.Header.Set("If-Match", `W/"datetime'bogus'"`)
		e := echo.New()
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		require.NoError(t, h.Handler()(c))
		assert.Equal(t, http.StatusPreconditionFailed, rec.Code)
	})

	t.Run("not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"mytable"}`))

		req := httptest.NewRequest(http.MethodDelete,
			"/"+testAccount+"/mytable(PartitionKey='p',RowKey='r')", http.NoBody)
		req.Header.Set("If-Match", "*")
		e := echo.New()
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		require.NoError(t, h.Handler()(c))
		assert.Equal(t, http.StatusNotFound, rec.Code)
		assert.Equal(t, "ResourceNotFound", rec.Header().Get("X-Ms-Error-Code"))
	})
}
