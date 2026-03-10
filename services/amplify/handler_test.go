package amplify_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/amplify"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestHandler() (*amplify.Handler, *amplify.InMemoryBackend) {
	b := newTestBackend()
	h := amplify.NewHandler(b)

	return h, b
}

func doRequest(
	t *testing.T,
	handler *amplify.Handler,
	method, path string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var buf *bytes.Buffer

	if body != nil {
		b, _ := json.Marshal(body)
		buf = bytes.NewBuffer(b)
	} else {
		buf = bytes.NewBuffer(nil)
	}

	req := httptest.NewRequest(method, path, buf)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := handler.Handler()(c)
	require.NoError(t, err)

	return rec
}

// ---- Service metadata tests ----

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	assert.Equal(t, "Amplify", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateApp")
	assert.Contains(t, ops, "GetApp")
	assert.Contains(t, ops, "ListApps")
	assert.Contains(t, ops, "DeleteApp")
	assert.Contains(t, ops, "CreateBranch")
	assert.Contains(t, ops, "GetBranch")
	assert.Contains(t, ops, "ListBranches")
	assert.Contains(t, ops, "DeleteBranch")
	assert.Contains(t, ops, "TagResource")
	assert.Contains(t, ops, "UntagResource")
	assert.Contains(t, ops, "ListTagsForResource")
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	assert.Greater(t, h.MatchPriority(), 0)
}

func TestHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	assert.Equal(t, "amplify", h.ChaosServiceName())
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	e := echo.New()

	tests := []struct {
		name    string
		path    string
		wantHit bool
	}{
		{name: "matches_apps", path: "/apps", wantHit: true},
		{name: "matches_apps_with_id", path: "/apps/abc123", wantHit: true},
		{name: "matches_tags", path: "/tags/arn%3Aaws%3Aamplify", wantHit: true},
		{name: "does_not_match_other", path: "/v1/apis", wantHit: false},
		{name: "does_not_match_random", path: "/buckets", wantHit: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			matcher := h.RouteMatcher()
			assert.Equal(t, tt.wantHit, matcher(c))
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	e := echo.New()

	tests := []struct {
		name      string
		method    string
		path      string
		wantOp    string
	}{
		{method: http.MethodPost, path: "/apps", wantOp: "CreateApp", name: "create_app"},
		{method: http.MethodGet, path: "/apps", wantOp: "ListApps", name: "list_apps"},
		{method: http.MethodGet, path: "/apps/abc123", wantOp: "GetApp", name: "get_app"},
		{method: http.MethodDelete, path: "/apps/abc123", wantOp: "DeleteApp", name: "delete_app"},
		{method: http.MethodPost, path: "/apps/abc123/branches", wantOp: "CreateBranch", name: "create_branch"},
		{method: http.MethodGet, path: "/apps/abc123/branches", wantOp: "ListBranches", name: "list_branches"},
		{method: http.MethodGet, path: "/apps/abc123/branches/main", wantOp: "GetBranch", name: "get_branch"},
		{method: http.MethodDelete, path: "/apps/abc123/branches/main", wantOp: "DeleteBranch", name: "delete_branch"},
		{method: http.MethodGet, path: "/tags/somearn", wantOp: "ListTagsForResource", name: "list_tags"},
		{method: http.MethodPost, path: "/tags/somearn", wantOp: "TagResource", name: "tag_resource"},
		{method: http.MethodDelete, path: "/tags/somearn", wantOp: "UntagResource", name: "untag_resource"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	e := echo.New()

	tests := []struct {
		name     string
		path     string
		wantRes  string
	}{
		{name: "extracts_app_id", path: "/apps/abc123", wantRes: "abc123"},
		{name: "extracts_app_id_from_branches", path: "/apps/abc123/branches/main", wantRes: "abc123"},
		{name: "no_resource_for_tags", path: "/tags/somearn", wantRes: ""},
		{name: "no_resource_for_apps_root", path: "/apps", wantRes: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantRes, h.ExtractResource(c))
		})
	}
}

// ---- App handler tests ----

func TestHandler_CreateApp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       any
		wantStatus int
		wantName   string
	}{
		{
			name:       "creates_app",
			body:       map[string]any{"name": "MyApp", "platform": "WEB"},
			wantStatus: http.StatusCreated,
			wantName:   "MyApp",
		},
		{
			name:       "missing_name_returns_400",
			body:       map[string]any{"platform": "WEB"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_json_returns_400",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			rec := doRequest(t, h, http.MethodPost, "/apps", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantName != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				app := resp["app"].(map[string]any)
				assert.Equal(t, tt.wantName, app["name"])
			}
		})
	}
}

func TestHandler_GetApp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*amplify.InMemoryBackend) string
		wantStatus int
	}{
		{
			name: "returns_existing_app",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)

				return app.AppID
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "returns_404_for_missing_app",
			setup: func(_ *amplify.InMemoryBackend) string {
				return "nonexistent"
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			appID := tt.setup(b)
			rec := doRequest(t, h, http.MethodGet, "/apps/"+appID, nil)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListApps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*amplify.InMemoryBackend)
		wantStatus int
		wantCount  int
	}{
		{
			name:       "returns_empty_list",
			setup:      func(_ *amplify.InMemoryBackend) {},
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name: "returns_all_apps",
			setup: func(b *amplify.InMemoryBackend) {
				_, _ = b.CreateApp("App1", "", "", "", nil)
				_, _ = b.CreateApp("App2", "", "", "", nil)
			},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			tt.setup(b)
			rec := doRequest(t, h, http.MethodGet, "/apps", nil)

			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			apps := resp["apps"].([]any)
			assert.Len(t, apps, tt.wantCount)
		})
	}
}

func TestHandler_DeleteApp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*amplify.InMemoryBackend) string
		wantStatus int
	}{
		{
			name: "deletes_existing_app",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)

				return app.AppID
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name: "returns_404_for_missing_app",
			setup: func(_ *amplify.InMemoryBackend) string {
				return "nonexistent"
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			appID := tt.setup(b)
			rec := doRequest(t, h, http.MethodDelete, "/apps/"+appID, nil)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ---- Branch handler tests ----

func TestHandler_CreateBranch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*amplify.InMemoryBackend) string
		body       any
		wantStatus int
	}{
		{
			name: "creates_branch",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)

				return app.AppID
			},
			body:       map[string]any{"branchName": "main", "stage": "PRODUCTION", "enableAutoBuild": true},
			wantStatus: http.StatusCreated,
		},
		{
			name: "missing_branch_name_returns_400",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)

				return app.AppID
			},
			body:       map[string]any{"stage": "PRODUCTION"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "duplicate_branch_returns_400",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)
				_, _ = b.CreateBranch(app.AppID, "main", "", "", false, nil)

				return app.AppID
			},
			body:       map[string]any{"branchName": "main"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid_json_returns_400",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)

				return app.AppID
			},
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			appID := tt.setup(b)
			rec := doRequest(t, h, http.MethodPost, "/apps/"+appID+"/branches", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetBranch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*amplify.InMemoryBackend) (string, string)
		wantStatus int
	}{
		{
			name: "returns_existing_branch",
			setup: func(b *amplify.InMemoryBackend) (string, string) {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)
				_, _ = b.CreateBranch(app.AppID, "main", "", "", false, nil)

				return app.AppID, "main"
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "returns_404_for_missing_branch",
			setup: func(b *amplify.InMemoryBackend) (string, string) {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)

				return app.AppID, "nonexistent"
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			appID, branchName := tt.setup(b)
			rec := doRequest(t, h, http.MethodGet, "/apps/"+appID+"/branches/"+branchName, nil)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListBranches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*amplify.InMemoryBackend) string
		wantStatus int
	}{
		{
			name: "returns_branches",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)
				_, _ = b.CreateBranch(app.AppID, "main", "", "", false, nil)

				return app.AppID
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "returns_404_for_missing_app",
			setup: func(_ *amplify.InMemoryBackend) string {
				return "nonexistent"
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			appID := tt.setup(b)
			rec := doRequest(t, h, http.MethodGet, "/apps/"+appID+"/branches", nil)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteBranch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*amplify.InMemoryBackend) (string, string)
		wantStatus int
	}{
		{
			name: "deletes_existing_branch",
			setup: func(b *amplify.InMemoryBackend) (string, string) {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)
				_, _ = b.CreateBranch(app.AppID, "main", "", "", false, nil)

				return app.AppID, "main"
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name: "returns_404_for_missing_branch",
			setup: func(b *amplify.InMemoryBackend) (string, string) {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)

				return app.AppID, "nonexistent"
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			appID, branchName := tt.setup(b)
			rec := doRequest(t, h, http.MethodDelete, "/apps/"+appID+"/branches/"+branchName, nil)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ---- Tagging handler tests ----

func TestHandler_TagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*amplify.InMemoryBackend) string
		body       any
		wantStatus int
	}{
		{
			name: "tags_existing_app",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)

				return app.ARN
			},
			body:       map[string]any{"tags": map[string]string{"env": "test"}},
			wantStatus: http.StatusOK,
		},
		{
			name: "returns_404_for_missing_resource",
			setup: func(_ *amplify.InMemoryBackend) string {
				return "arn:aws:amplify:us-east-1:000000000000:apps/nonexistent"
			},
			body:       map[string]any{"tags": map[string]string{"env": "test"}},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "invalid_json_returns_400",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)

				return app.ARN
			},
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			resourceARN := tt.setup(b)
			encodedARN := encodeARN(resourceARN)
			rec := doRequest(t, h, http.MethodPost, "/tags/"+encodedARN, tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*amplify.InMemoryBackend) string
		wantStatus int
	}{
		{
			name: "returns_tags_for_app",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", map[string]string{"env": "test"})

				return app.ARN
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "returns_404_for_missing_resource",
			setup: func(_ *amplify.InMemoryBackend) string {
				return "arn:aws:amplify:us-east-1:000000000000:apps/nonexistent"
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			resourceARN := tt.setup(b)
			encodedARN := encodeARN(resourceARN)
			rec := doRequest(t, h, http.MethodGet, "/tags/"+encodedARN, nil)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*amplify.InMemoryBackend) string
		tagKeys    string
		wantStatus int
	}{
		{
			name: "removes_tags",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", map[string]string{"env": "test"})

				return app.ARN
			},
			tagKeys:    "env",
			wantStatus: http.StatusOK,
		},
		{
			name: "returns_404_for_missing_resource",
			setup: func(_ *amplify.InMemoryBackend) string {
				return "arn:aws:amplify:us-east-1:000000000000:apps/nonexistent"
			},
			tagKeys:    "env",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			resourceARN := tt.setup(b)
			encodedARN := encodeARN(resourceARN)
			path := "/tags/" + encodedARN + "?tagKeys=" + tt.tagKeys
			rec := doRequest(t, h, http.MethodDelete, path, nil)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ---- Edge case tests ----

func TestHandler_UnknownPath(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	rec := doRequest(t, h, http.MethodGet, "/unknown/path", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{name: "apps_patch", method: http.MethodPatch, path: "/apps"},
		{name: "app_id_post", method: http.MethodPost, path: "/apps/abc123"},
		{name: "branches_patch", method: http.MethodPatch, path: "/apps/abc123/branches"},
		{name: "branch_patch", method: http.MethodPatch, path: "/apps/abc123/branches/main"},
		{name: "tags_patch", method: http.MethodPatch, path: "/tags/somearn"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			rec := doRequest(t, h, tt.method, tt.path, nil)
			assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
		})
	}
}

func TestHandler_InvalidSubPath(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	rec := doRequest(t, h, http.MethodGet, "/apps/abc123/invalid", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_InvalidBranchSubPath(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	rec := doRequest(t, h, http.MethodGet, "/apps/abc123/invalid/main", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// encodeARN URL-encodes the colons and slashes in an ARN for use as a path segment.
func encodeARN(arn string) string {
	var buf bytes.Buffer

	for _, c := range arn {
		switch c {
		case ':':
			buf.WriteString("%3A")
		case '/':
			buf.WriteString("%2F")
		default:
			buf.WriteByte(byte(c))
		}
	}

	return buf.String()
}
