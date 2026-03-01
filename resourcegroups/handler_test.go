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

func TestResourceGroupsHandler_CreateGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setup        func(t *testing.T, h *resourcegroups.Handler)
		groupName    string
		description  string
		wantCode     int
		wantContains []string
	}{
		{
			name:         "success",
			groupName:    "my-group",
			description:  "test group",
			wantCode:     http.StatusOK,
			wantContains: []string{"Group"},
		},
		{
			name:      "duplicate",
			groupName: "my-group",
			setup: func(t *testing.T, h *resourcegroups.Handler) {
				doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "my-group"})
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestResourceGroupsHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}
			body := map[string]any{"Name": tt.groupName}
			if tt.description != "" {
				body["Description"] = tt.description
			}
			rec := doResourceGroupsRequest(t, h, "CreateGroup", body)
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestResourceGroupsHandler_ListGroups(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "g1"})
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "g2"})

	rec := doResourceGroupsRequest(t, h, "ListGroups", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GroupIdentifiers")
}

func TestResourceGroupsHandler_GetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setup        func(t *testing.T, h *resourcegroups.Handler)
		groupName    string
		wantCode     int
		wantContains []string
	}{
		{
			name:      "success",
			groupName: "my-group",
			setup: func(t *testing.T, h *resourcegroups.Handler) {
				doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "my-group"})
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"Group"},
		},
		{
			name:      "not_found",
			groupName: "nonexistent",
			wantCode:  http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestResourceGroupsHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rec := doResourceGroupsRequest(t, h, "GetGroup", map[string]any{"GroupName": tt.groupName})
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestResourceGroupsHandler_DeleteGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(t *testing.T, h *resourcegroups.Handler)
		groupName string
		wantCode  int
	}{
		{
			name:      "success",
			groupName: "my-group",
			setup: func(t *testing.T, h *resourcegroups.Handler) {
				doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "my-group"})
			},
			wantCode: http.StatusOK,
		},
		{
			name:      "not_found",
			groupName: "nonexistent",
			wantCode:  http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestResourceGroupsHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rec := doResourceGroupsRequest(t, h, "DeleteGroup", map[string]any{"GroupName": tt.groupName})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestResourceGroupsHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	rec := doResourceGroupsRequest(t, h, "UnknownAction", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestResourceGroupsHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{
			name:   "match",
			target: "ResourceGroups.ListGroups",
			want:   true,
		},
		{
			name:   "no_match",
			target: "Kinesis_20131202.CreateStream",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestResourceGroupsHandler(t)
			matcher := h.RouteMatcher()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

func TestResourceGroupsHandler_ProviderName(t *testing.T) {
	t.Parallel()

	p := &resourcegroups.Provider{}
	assert.Equal(t, "ResourceGroups", p.Name())
}

func TestResourceGroupsHandler_HandlerName(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	assert.Equal(t, "ResourceGroups", h.Name())
}

func TestResourceGroupsHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateGroup")
	assert.Contains(t, ops, "DeleteGroup")
	assert.Contains(t, ops, "ListGroups")
	assert.Contains(t, ops, "GetGroup")
}

func TestResourceGroupsHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	assert.Equal(t, 100, h.MatchPriority())
}

func TestResourceGroupsHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   string
	}{
		{
			name:   "with_target",
			target: "ResourceGroups.CreateGroup",
			want:   "CreateGroup",
		},
		{
			name: "no_target",
			want: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestResourceGroupsHandler(t)
			e := echo.New()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

func TestResourceGroupsHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "name_field",
			body: `{"Name":"my-group"}`,
			want: "my-group",
		},
		{
			name: "group_name_field",
			body: `{"GroupName":"other-group"}`,
			want: "other-group",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestResourceGroupsHandler(t)
			e := echo.New()

			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

func TestResourceGroupsHandler_ProviderInit(t *testing.T) {
	t.Parallel()

	p := &resourcegroups.Provider{}
	ctx := &service.AppContext{Logger: slog.Default()}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
	assert.Equal(t, "ResourceGroups", svc.Name())
}
