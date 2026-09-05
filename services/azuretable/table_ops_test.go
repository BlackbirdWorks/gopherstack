package azuretable_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateTable(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"mytable"}`))

		require.Equal(t, http.StatusCreated, rec.Code)

		var body map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Equal(t, "mytable", body["TableName"])
		assert.Contains(t, body, "odata.metadata")
	})

	t.Run("prefer_return_no_content", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		req := httptest.NewRequest(
			http.MethodPost,
			"/"+testAccount+"/Tables",
			strings.NewReader(`{"TableName":"mytable"}`),
		)
		req.Header.Set("Prefer", "return-no-content")

		e := echo.New()
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		require.NoError(t, h.Handler()(c))

		assert.Equal(t, http.StatusNoContent, rec.Code)
		assert.Equal(t, "return-no-content", rec.Header().Get("Preference-Applied"))
	})

	t.Run("duplicate_conflict", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"dup"}`))
		require.Equal(t, http.StatusCreated, rec.Code)

		rec = doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"dup"}`))
		assert.Equal(t, http.StatusConflict, rec.Code)
		assert.Equal(t, "TableAlreadyExists", rec.Header().Get("X-Ms-Error-Code"))
	})

	t.Run("empty_name_rejected", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":""}`))
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Equal(t, "InvalidInput", rec.Header().Get("X-Ms-Error-Code"))
	})

	t.Run("malformed_body", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`not json`))
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Equal(t, "InvalidInput", rec.Header().Get("X-Ms-Error-Code"))
	})
}

func TestListTables(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"b"}`))
	doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"a"}`))

	rec := doRequest(t, h, http.MethodGet, "/"+testAccount+"/Tables", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var body struct {
		Value []map[string]any `json:"value"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Len(t, body.Value, 2)
	assert.Equal(t, "a", body.Value[0]["TableName"])
	assert.Equal(t, "b", body.Value[1]["TableName"])
}

func TestListTables_NoMetadataLevel(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"a"}`))

	req := httptest.NewRequest(http.MethodGet, "/"+testAccount+"/Tables", http.NoBody)
	req.Header.Set("Accept", "application/json;odata=nometadata")

	e := echo.New()
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	var body struct {
		Value []map[string]any `json:"value"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Len(t, body.Value, 1)
	assert.NotContains(t, body.Value[0], "odata.metadata")
	assert.Contains(t, rec.Header().Get("Content-Type"), "odata=nometadata")
}

func TestListTables_FullMetadataLevel(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"a"}`))

	req := httptest.NewRequest(http.MethodGet, "/"+testAccount+"/Tables", http.NoBody)
	req.Header.Set("Accept", "application/json;odata=fullmetadata")

	e := echo.New()
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	var body struct {
		Value []map[string]any `json:"value"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Len(t, body.Value, 1)
	assert.Contains(t, body.Value[0], "odata.type")
	assert.Contains(t, body.Value[0], "odata.id")
	assert.Contains(t, body.Value[0], "odata.editLink")
}

func TestDeleteTable(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, http.MethodPost, "/"+testAccount+"/Tables", []byte(`{"TableName":"gone"}`))

		rec := doRequest(t, h, http.MethodDelete, "/"+testAccount+"/Tables('gone')", nil)
		assert.Equal(t, http.StatusNoContent, rec.Code)

		rec = doRequest(t, h, http.MethodGet, "/"+testAccount+"/Tables", nil)
		assert.JSONEq(t, `{"value":[]}`, rec.Body.String())
	})

	t.Run("not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, http.MethodDelete, "/"+testAccount+"/Tables('nope')", nil)
		assert.Equal(t, http.StatusNotFound, rec.Code)
		assert.Equal(t, "TableNotFound", rec.Header().Get("X-Ms-Error-Code"))
	})

	t.Run("invalid_literal", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, http.MethodDelete, "/"+testAccount+"/Tables(nope)", nil)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Equal(t, "InvalidInput", rec.Header().Get("X-Ms-Error-Code"))
	})
}
