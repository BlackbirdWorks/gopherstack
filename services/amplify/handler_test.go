package amplify_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/amplify"
)

// ---- Core test helpers, shared by every handler_<family>_test.go file ----

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
		b, err := json.Marshal(body)
		require.NoError(t, err)
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

// malformedJSON is an intentionally malformed JSON payload used in tests.
const malformedJSON = `{"key": invalid}`

// doRawRequest sends raw bytes as the request body (e.g. for malformed JSON).
func doRawRequest(
	t *testing.T,
	handler *amplify.Handler,
	method, path string,
	raw []byte,
) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, path, bytes.NewBuffer(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := handler.Handler()(c)
	require.NoError(t, err)

	return rec
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

// seedApp creates an app and fails the test if it errors.
func seedApp(t *testing.T, b *amplify.InMemoryBackend, name string) *amplify.App {
	t.Helper()

	app, err := b.CreateApp(name, "", "", "WEB", nil)
	require.NoError(t, err)

	return app
}

// seedMainBranch creates a "main" branch and fails the test if it errors.
func seedMainBranch(t *testing.T, b *amplify.InMemoryBackend, appID string) *amplify.Branch {
	t.Helper()

	br, err := b.CreateBranch(appID, "main", "", "", false, nil)
	require.NoError(t, err)

	return br
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
	assert.Positive(t, h.MatchPriority())
}

func TestHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	assert.Equal(t, "amplify", h.ChaosServiceName())
}

func TestHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	ops := h.ChaosOperations()
	assert.NotEmpty(t, ops)
	assert.Contains(t, ops, "CreateApp")
}

func TestHandler_ChaosRegions(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	regions := h.ChaosRegions()
	assert.NotEmpty(t, regions)
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

// TestHandler_ExtractOperation covers operation-name extraction for every
// route family: apps, branches, tags, webhooks, backend environments,
// domain associations, jobs, deployments, access logs, and artifacts.
func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	e := echo.New()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{method: http.MethodPost, path: "/apps", wantOp: "CreateApp", name: "create_app"},
		{method: http.MethodGet, path: "/apps", wantOp: "ListApps", name: "list_apps"},
		{method: http.MethodGet, path: "/apps/abc123", wantOp: "GetApp", name: "get_app"},
		{method: http.MethodDelete, path: "/apps/abc123", wantOp: "DeleteApp", name: "delete_app"},
		{method: http.MethodPost, path: "/apps/abc", wantOp: "UpdateApp", name: "update_app"},
		{method: http.MethodPost, path: "/apps/abc123/branches", wantOp: "CreateBranch", name: "create_branch"},
		{method: http.MethodGet, path: "/apps/abc123/branches", wantOp: "ListBranches", name: "list_branches"},
		{method: http.MethodGet, path: "/apps/abc123/branches/main", wantOp: "GetBranch", name: "get_branch"},
		{method: http.MethodDelete, path: "/apps/abc123/branches/main", wantOp: "DeleteBranch", name: "delete_branch"},
		{method: http.MethodPost, path: "/apps/abc/branches/main", wantOp: "UpdateBranch", name: "update_branch"},
		{method: http.MethodGet, path: "/tags/somearn", wantOp: "ListTagsForResource", name: "list_tags"},
		{method: http.MethodPost, path: "/tags/somearn", wantOp: "TagResource", name: "tag_resource"},
		{method: http.MethodDelete, path: "/tags/somearn", wantOp: "UntagResource", name: "untag_resource"},
		{name: "create_webhook", method: http.MethodPost, path: "/apps/abc/webhooks", wantOp: "CreateWebhook"},
		{name: "list_webhooks", method: http.MethodGet, path: "/apps/abc/webhooks", wantOp: "ListWebhooks"},
		{name: "get_webhook", method: http.MethodGet, path: "/webhooks/wh1", wantOp: "GetWebhook"},
		{name: "update_webhook", method: http.MethodPost, path: "/webhooks/wh1", wantOp: "UpdateWebhook"},
		{name: "delete_webhook", method: http.MethodDelete, path: "/webhooks/wh1", wantOp: "DeleteWebhook"},
		{name: "create_be", method: http.MethodPost,
			path: "/apps/abc/backendenvironments", wantOp: "CreateBackendEnvironment"},
		{name: "list_be", method: http.MethodGet,
			path: "/apps/abc/backendenvironments", wantOp: "ListBackendEnvironments"},
		{name: "get_be", method: http.MethodGet,
			path: "/apps/abc/backendenvironments/prod", wantOp: "GetBackendEnvironment"},
		{name: "delete_be", method: http.MethodDelete,
			path: "/apps/abc/backendenvironments/prod", wantOp: "DeleteBackendEnvironment"},
		{name: "create_domain", method: http.MethodPost,
			path: "/apps/abc/domains", wantOp: "CreateDomainAssociation"},
		{name: "list_domains", method: http.MethodGet,
			path: "/apps/abc/domains", wantOp: "ListDomainAssociations"},
		{name: "get_domain", method: http.MethodGet,
			path: "/apps/abc/domains/example.com", wantOp: "GetDomainAssociation"},
		{name: "delete_domain", method: http.MethodDelete,
			path: "/apps/abc/domains/example.com", wantOp: "DeleteDomainAssociation"},
		{name: "update_domain", method: http.MethodPost,
			path: "/apps/abc/domains/example.com", wantOp: "UpdateDomainAssociation"},
		{name: "start_job", method: http.MethodPost,
			path: "/apps/abc/branches/main/jobs", wantOp: "StartJob"},
		{name: "list_jobs", method: http.MethodGet,
			path: "/apps/abc/branches/main/jobs", wantOp: "ListJobs"},
		{name: "get_job", method: http.MethodGet,
			path: "/apps/abc/branches/main/jobs/j1", wantOp: "GetJob"},
		{name: "delete_job", method: http.MethodDelete,
			path: "/apps/abc/branches/main/jobs/j1", wantOp: "DeleteJob"},
		{name: "stop_job", method: http.MethodDelete,
			path: "/apps/abc/branches/main/jobs/j1/stop", wantOp: "StopJob"},
		{name: "create_deployment", method: http.MethodPost,
			path: "/apps/abc/branches/main/deployments", wantOp: "CreateDeployment"},
		{name: "start_deployment", method: http.MethodPost,
			path: "/apps/abc/branches/main/deployments/start", wantOp: "StartDeployment"},
		{name: "generate_access_logs", method: http.MethodPost,
			path: "/apps/abc/accesslogs", wantOp: "GenerateAccessLogs"},
		{name: "get_artifact_url", method: http.MethodGet, path: "/artifacts/art1", wantOp: "GetArtifactUrl"},
		{name: "list_artifacts", method: http.MethodGet,
			path: "/apps/abc/branches/main/jobs/j1/artifacts", wantOp: "ListArtifacts"},
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
		name    string
		path    string
		wantRes string
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
		{name: "app_id_put", method: http.MethodPut, path: "/apps/abc123"},
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

// TestHandler_ErrorResponseShape verifies that error responses carry both the
// "X-Amzn-Errortype" header and a "__type" body field alongside "message".
// aws-sdk-go-v2's generated restjson1 deserializer (see
// awsRestjson1_deserializeOpErrorGetApp and friends) selects a typed
// exception (types.NotFoundException, types.BadRequestException, ...) by
// reading one of those two -- never from the HTTP status alone -- so a
// response missing both deserializes client-side as a generic UnknownError
// with an empty code, which breaks any caller that type-switches on a
// specific Amplify exception.
func TestHandler_ErrorResponseShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantType   string
		wantStatus int
	}{
		{
			name:       "get_app_not_found",
			method:     http.MethodGet,
			path:       "/apps/does-not-exist",
			wantStatus: http.StatusNotFound,
			wantType:   "NotFoundException",
		},
		{
			name:       "create_app_missing_name",
			method:     http.MethodPost,
			path:       "/apps",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
			wantType:   "BadRequestException",
		},
		{
			name:       "unknown_path",
			method:     http.MethodGet,
			path:       "/bogus",
			wantStatus: http.StatusNotFound,
			wantType:   "NotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			rec := doRequest(t, h, tt.method, tt.path, tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Equal(t, tt.wantType, rec.Header().Get("X-Amzn-Errortype"))

			var payload map[string]any

			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &payload))
			assert.Equal(t, tt.wantType, payload["__type"])
			assert.NotEmpty(t, payload["message"])
		})
	}
}
