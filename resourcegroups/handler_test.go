package resourcegroups_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/resourcegroups"
)

func newTestResourceGroupsHandler(t *testing.T) *resourcegroups.Handler {
	t.Helper()

	return resourcegroups.NewHandler(resourcegroups.NewInMemoryBackend("000000000000", "us-east-1"), slog.Default())
}

func doResourceGroupsRequest(
	t *testing.T,
	h *resourcegroups.Handler,
	action string,
	body any,
) *httptest.ResponseRecorder {
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

func TestResourceGroups_Handler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "CreateGroup",
			run: func(t *testing.T) {
				h := newTestResourceGroupsHandler(t)

				rec := doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
					"Name":        "my-group",
					"Description": "test group",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "Group")
			},
		},
		{
			name: "ListGroups",
			run: func(t *testing.T) {
				h := newTestResourceGroupsHandler(t)
				doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "g1"})
				doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "g2"})

				rec := doResourceGroupsRequest(t, h, "ListGroups", nil)
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "GroupIdentifiers")
			},
		},
		{
			name: "GetGroup",
			run: func(t *testing.T) {
				h := newTestResourceGroupsHandler(t)
				doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "my-group"})

				rec := doResourceGroupsRequest(t, h, "GetGroup", map[string]any{"GroupName": "my-group"})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "Group")
			},
		},
		{
			name: "GetGroup_NotFound",
			run: func(t *testing.T) {
				h := newTestResourceGroupsHandler(t)

				rec := doResourceGroupsRequest(t, h, "GetGroup", map[string]any{"GroupName": "nonexistent"})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "DeleteGroup",
			run: func(t *testing.T) {
				h := newTestResourceGroupsHandler(t)
				doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "my-group"})

				rec := doResourceGroupsRequest(t, h, "DeleteGroup", map[string]any{"GroupName": "my-group"})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "UnknownAction",
			run: func(t *testing.T) {
				h := newTestResourceGroupsHandler(t)

				rec := doResourceGroupsRequest(t, h, "UnknownAction", nil)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "RouteMatcher",
			run: func(t *testing.T) {
				h := newTestResourceGroupsHandler(t)
				matcher := h.RouteMatcher()

				e := echo.New()
				req := httptest.NewRequest(http.MethodPost, "/", nil)
				req.Header.Set("X-Amz-Target", "ResourceGroups.ListGroups")
				c := e.NewContext(req, httptest.NewRecorder())

				assert.True(t, matcher(c))
			},
		},
		{
			name: "Provider_Name",
			run: func(t *testing.T) {
				p := &resourcegroups.Provider{}
				assert.Equal(t, "ResourceGroups", p.Name())
			},
		},
		{
			name: "Handler_Name",
			run: func(t *testing.T) {
				h := newTestResourceGroupsHandler(t)
				assert.Equal(t, "ResourceGroups", h.Name())
			},
		},
		{
			name: "GetSupportedOperations",
			run: func(t *testing.T) {
				h := newTestResourceGroupsHandler(t)
				ops := h.GetSupportedOperations()
				assert.Contains(t, ops, "CreateGroup")
				assert.Contains(t, ops, "DeleteGroup")
				assert.Contains(t, ops, "ListGroups")
				assert.Contains(t, ops, "GetGroup")
			},
		},
		{
			name: "MatchPriority",
			run: func(t *testing.T) {
				h := newTestResourceGroupsHandler(t)
				assert.Equal(t, 100, h.MatchPriority())
			},
		},
		{
			name: "ExtractOperation",
			run: func(t *testing.T) {
				h := newTestResourceGroupsHandler(t)
				e := echo.New()

				req := httptest.NewRequest(http.MethodPost, "/", nil)
				req.Header.Set("X-Amz-Target", "ResourceGroups.CreateGroup")
				c := e.NewContext(req, httptest.NewRecorder())
				assert.Equal(t, "CreateGroup", h.ExtractOperation(c))

				// No target → "Unknown"
				req2 := httptest.NewRequest(http.MethodPost, "/", nil)
				c2 := e.NewContext(req2, httptest.NewRecorder())
				assert.Equal(t, "Unknown", h.ExtractOperation(c2))
			},
		},
		{
			name: "ExtractResource",
			run: func(t *testing.T) {
				h := newTestResourceGroupsHandler(t)
				e := echo.New()

				// Name field (CreateGroup)
				req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"Name":"my-group"}`))
				c := e.NewContext(req, httptest.NewRecorder())
				assert.Equal(t, "my-group", h.ExtractResource(c))

				// GroupName field (GetGroup/DeleteGroup)
				req2 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"GroupName":"other-group"}`))
				c2 := e.NewContext(req2, httptest.NewRecorder())
				assert.Equal(t, "other-group", h.ExtractResource(c2))
			},
		},
		{
			name: "CreateGroup_Duplicate",
			run: func(t *testing.T) {
				h := newTestResourceGroupsHandler(t)
				doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "my-group"})

				rec := doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "my-group"})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "DeleteGroup_NotFound",
			run: func(t *testing.T) {
				h := newTestResourceGroupsHandler(t)

				rec := doResourceGroupsRequest(t, h, "DeleteGroup", map[string]any{"GroupName": "nonexistent"})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "RouteMatcher_NoMatch",
			run: func(t *testing.T) {
				h := newTestResourceGroupsHandler(t)
				matcher := h.RouteMatcher()

				e := echo.New()
				req := httptest.NewRequest(http.MethodPost, "/", nil)
				req.Header.Set("X-Amz-Target", "Kinesis_20131202.CreateStream")
				c := e.NewContext(req, httptest.NewRecorder())

				assert.False(t, matcher(c))
			},
		},
		{
			name: "Provider_Init",
			run: func(t *testing.T) {
				p := &resourcegroups.Provider{}
				ctx := &service.AppContext{Logger: slog.Default()}
				svc, err := p.Init(ctx)
				require.NoError(t, err)
				assert.NotNil(t, svc)
				assert.Equal(t, "ResourceGroups", svc.Name())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}
