package resourcegroups_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/resourcegroups"
)

func newTestResourceGroupsHandler(t *testing.T) *resourcegroups.Handler {
	t.Helper()

	return resourcegroups.NewHandler(resourcegroups.NewInMemoryBackend("000000000000", "us-east-1"), slog.Default())
}

func doResourceGroupsRequest(t *testing.T, h *resourcegroups.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	} else {
		bodyBytes = []byte("{}")
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "ResourceGroups."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestResourceGroups_Handler_CreateGroup(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)

	rec := doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
		"Name":        "my-group",
		"Description": "test group",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp, "Group")
}

func TestResourceGroups_Handler_ListGroups(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "g1"})
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "g2"})

	rec := doResourceGroupsRequest(t, h, "ListGroups", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp, "GroupIdentifiers")
}

func TestResourceGroups_Handler_GetGroup(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "my-group"})

	rec := doResourceGroupsRequest(t, h, "GetGroup", map[string]any{"GroupName": "my-group"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp, "Group")
}

func TestResourceGroups_Handler_GetGroup_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)

	rec := doResourceGroupsRequest(t, h, "GetGroup", map[string]any{"GroupName": "nonexistent"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestResourceGroups_Handler_DeleteGroup(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "my-group"})

	rec := doResourceGroupsRequest(t, h, "DeleteGroup", map[string]any{"GroupName": "my-group"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestResourceGroups_Handler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)

	rec := doResourceGroupsRequest(t, h, "UnknownAction", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestResourceGroups_Handler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	matcher := h.RouteMatcher()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "ResourceGroups.ListGroups")
	c := e.NewContext(req, httptest.NewRecorder())

	assert.True(t, matcher(c))
}

func TestResourceGroups_Provider(t *testing.T) {
	t.Parallel()

	p := &resourcegroups.Provider{}
	assert.Equal(t, "ResourceGroups", p.Name())
}
