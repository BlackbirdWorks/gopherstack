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
	"github.com/blackbirdworks/gopherstack/services/resourcegroups"
)

func newTestResourceGroupsHandler(t *testing.T) *resourcegroups.Handler {
	t.Helper()

	return resourcegroups.NewHandler(resourcegroups.NewInMemoryBackend("000000000000", "us-east-1"))
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
		setup        func(t *testing.T, h *resourcegroups.Handler)
		name         string
		groupName    string
		description  string
		wantContains []string
		wantCode     int
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
				t.Helper()
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
		setup        func(t *testing.T, h *resourcegroups.Handler)
		name         string
		groupName    string
		wantContains []string
		wantCode     int
	}{
		{
			name:      "success",
			groupName: "my-group",
			setup: func(t *testing.T, h *resourcegroups.Handler) {
				t.Helper()
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
				t.Helper()
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

// doResourceTagsRequest makes a request to the /resources/{arn}/tags endpoint.
func doResourceTagsRequest(
	t *testing.T,
	h *resourcegroups.Handler,
	method, resourceARN string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyReader *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)
		bodyReader = bytes.NewReader(b)
	} else {
		bodyReader = bytes.NewReader(nil)
	}

	e := echo.New()
	path := "/resources/" + resourceARN + "/tags"
	req := httptest.NewRequest(method, path, bodyReader)
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.Request().RequestURI = path

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestResourceGroupsHandler_GetTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *resourcegroups.Handler) string
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(h *resourcegroups.Handler) string {
				rec := doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
					"Name": "tagged-group",
					"Tags": map[string]string{"env": "test"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var out struct {
					Group struct {
						GroupArn string `json:"GroupArn"`
					} `json:"Group"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				return out.Group.GroupArn
			},
			wantCode: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(_ *resourcegroups.Handler) string {
				return "arn:aws:resource-groups:us-east-1:000000000000:group/nonexistent"
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestResourceGroupsHandler(t)
			arn := tt.setup(h)
			rec := doResourceTagsRequest(t, h, http.MethodGet, arn, nil)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestResourceGroupsHandler_Tag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "success",
			tags:     map[string]string{"team": "platform"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			tags:     map[string]string{"k": "v"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestResourceGroupsHandler(t)
			var groupARN string
			if tt.wantCode == http.StatusOK {
				rec := doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "my-group"})
				require.Equal(t, http.StatusOK, rec.Code)
				var out struct {
					Group struct {
						GroupArn string `json:"GroupArn"`
					} `json:"Group"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				groupARN = out.Group.GroupArn
			} else {
				groupARN = "arn:aws:resource-groups:us-east-1:000000000000:group/nonexistent"
			}
			rec := doResourceTagsRequest(t, h, http.MethodPut, groupARN, map[string]any{"Tags": tt.tags})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestResourceGroupsHandler_Untag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		keys     []string
		wantCode int
	}{
		{
			name:     "success",
			keys:     []string{"env"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			keys:     []string{"env"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestResourceGroupsHandler(t)
			var groupARN string
			if tt.wantCode == http.StatusOK {
				rec := doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
					"Name": "my-group",
					"Tags": map[string]string{"env": "test"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var out struct {
					Group struct {
						GroupArn string `json:"GroupArn"`
					} `json:"Group"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				groupARN = out.Group.GroupArn
			} else {
				groupARN = "arn:aws:resource-groups:us-east-1:000000000000:group/nonexistent"
			}
			rec := doResourceTagsRequest(t, h, http.MethodPatch, groupARN, map[string]any{"Keys": tt.keys})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestResourceGroupsHandler_ResourceTagsMethodNotAllowed(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	rec := doResourceTagsRequest(t, h, http.MethodDelete,
		"arn:aws:resource-groups:us-east-1:000000000000:group/my-group", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestResourceGroupsHandler_RouteMatcherTagsPath(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	matcher := h.RouteMatcher()
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet,
		"/resources/arn:aws:resource-groups:us-east-1:000000000000:group/my-group/tags", nil)
	req.RequestURI = "/resources/arn:aws:resource-groups:us-east-1:000000000000:group/my-group/tags"
	c := e.NewContext(req, httptest.NewRecorder())

	assert.True(t, matcher(c))
}

// doResourceGroupsRESTRequest makes requests via REST path.
func doResourceGroupsRESTRequest(
	t *testing.T,
	h *resourcegroups.Handler,
	path string,
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
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestResourceGroupsHandler_RESTCreateGroup(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	rec := doResourceGroupsRESTRequest(t, h, "/groups", map[string]any{"Name": "rest-group"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestResourceGroupsHandler_RESTGetGroup(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRESTRequest(t, h, "/groups", map[string]any{"Name": "rest-group"})
	rec := doResourceGroupsRESTRequest(t, h, "/get-group", map[string]any{"GroupName": "rest-group"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestResourceGroupsHandler_RESTDeleteGroup(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRESTRequest(t, h, "/groups", map[string]any{"Name": "rest-group"})
	rec := doResourceGroupsRESTRequest(t, h, "/delete-group", map[string]any{"GroupName": "rest-group"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestResourceGroupsHandler_RESTListGroups(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	rec := doResourceGroupsRESTRequest(t, h, "/groups-list", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestResourceGroupsHandler_RESTMethodNotAllowed(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/groups", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestResourceGroupsHandler_GetGroupQuery(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
		"Name": "qgroup",
		"ResourceQuery": map[string]any{
			"Type":  "TAG_FILTERS_1_0",
			"Query": `{"ResourceTypeFilters":["AWS::AllSupported"]}`,
		},
	})

	rec := doResourceGroupsRequest(t, h, "GetGroupQuery", map[string]any{"GroupName": "qgroup"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestResourceGroupsHandler_GetGroupConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "cfggroup"})

	rec := doResourceGroupsRequest(t, h, "GetGroupConfiguration", map[string]any{"GroupName": "cfggroup"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestResourceGroupsHandler_RESTGetGroupQuery(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRESTRequest(t, h, "/groups", map[string]any{"Name": "qgroup2"})
	rec := doResourceGroupsRESTRequest(t, h, "/get-group-query", map[string]any{"GroupName": "qgroup2"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestResourceGroupsHandler_RESTGetGroupConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRESTRequest(t, h, "/groups", map[string]any{"Name": "cfggroup2"})
	rec := doResourceGroupsRESTRequest(t, h, "/get-group-configuration", map[string]any{"GroupName": "cfggroup2"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestResourceGroupsHandler_RESTExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		method string
		want   string
	}{
		{name: "create_group", path: "/groups", method: http.MethodPost, want: "CreateGroup"},
		{name: "get_group", path: "/get-group", method: http.MethodPost, want: "GetGroup"},
		{
			name:   "tags_get",
			path:   "/resources/arn:aws:rg:us-east-1:123:group/g/tags",
			method: http.MethodGet,
			want:   "GetTags",
		},
		{
			name:   "tags_put",
			path:   "/resources/arn:aws:rg:us-east-1:123:group/g/tags",
			method: http.MethodPut,
			want:   "Tag",
		},
		{
			name:   "tags_patch",
			path:   "/resources/arn:aws:rg:us-east-1:123:group/g/tags",
			method: http.MethodPatch,
			want:   "Untag",
		},
		{
			name:   "tags_unknown_method",
			path:   "/resources/arn:aws:rg:us-east-1:123:group/g/tags",
			method: http.MethodDelete,
			want:   "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestResourceGroupsHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

func TestResourceGroupsHandler_SnapshotRestore(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "persist-group"})

	snap := h.Snapshot()
	require.NotNil(t, snap)

	h2 := newTestResourceGroupsHandler(t)
	require.NoError(t, h2.Restore(snap))

	rec := doResourceGroupsRequest(t, h2, "GetGroup", map[string]any{"GroupName": "persist-group"})
	assert.Equal(t, http.StatusOK, rec.Code)
}
