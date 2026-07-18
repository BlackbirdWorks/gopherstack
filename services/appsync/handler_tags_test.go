package appsync_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestHandler_TagOperations(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	// Tag the resource.
	tagBody := map[string]any{"tags": map[string]any{"env": "prod", "team": "platform"}}
	rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID+"/tags", tagBody)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// List tags.
	rec2 := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/tags", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&resp))
	tags := resp["tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "platform", tags["team"])

	// Untag one key.
	rec3, err := http.NewRequest(http.MethodDelete, "/v1/apis/"+api.APIID+"/tags?tagKeys=env", nil)
	require.NoError(t, err)

	_ = rec3
	// Use doRequest helper style with query param.
	e := echo.New()
	req := httptest.NewRequest(http.MethodDelete, "/v1/apis/"+api.APIID+"/tags?tagKeys=env", nil)
	rr := httptest.NewRecorder()
	ctx := e.NewContext(req, rr)

	require.NoError(t, h.Handler()(ctx))
	assert.Equal(t, http.StatusNoContent, rr.Code)

	// List tags again - should only have "team".
	rec4 := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/tags", nil)
	require.Equal(t, http.StatusOK, rec4.Code)

	var resp4 map[string]any
	require.NoError(t, json.NewDecoder(rec4.Body).Decode(&resp4))
	tags4 := resp4["tags"].(map[string]any)
	assert.NotContains(t, tags4, "env")
	assert.Equal(t, "platform", tags4["team"])
}

func TestHandler_TagResource_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodPost, "/v1/apis/nonexistent/tags", map[string]any{"tags": map[string]any{}})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_UntagResource_MissingQueryParam(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodDelete, "/v1/apis/"+api.APIID+"/tags", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_TagOps_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPut, "/v1/apis/"+api.APIID+"/tags", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_Tags_CRUD(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TaggedAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	// Tag the resource.
	rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID+"/tags", map[string]any{
		"tags": map[string]string{"env": "prod"},
	})
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// List tags.
	rec = doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/tags", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Untag.
	rec = doRequest(t, h, http.MethodDelete, "/v1/apis/"+api.APIID+"/tags?tagKeys=env", nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestHandler_Tags_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodGet, "/v1/apis/nonexistent/tags", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
