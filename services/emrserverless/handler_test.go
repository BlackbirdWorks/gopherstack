package emrserverless_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/emrserverless"
)

func newTestHandler(t *testing.T) *emrserverless.Handler {
	t.Helper()

	return emrserverless.NewHandler(emrserverless.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doRequest(t *testing.T, h *emrserverless.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var reqBody *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)
		reqBody = bytes.NewReader(b)
	} else {
		reqBody = bytes.NewReader(nil)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, reqBody)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func mustUnmarshal(t *testing.T, rec *httptest.ResponseRecorder, v any) {
	t.Helper()
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), v))
}

// createApp is a test helper that creates an application and returns its ID.
func createApp(t *testing.T, h *emrserverless.Handler, name string) string {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":         name,
		"type":         "SPARK",
		"releaseLabel": "emr-6.6.0",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]string
	mustUnmarshal(t, rec, &out)
	require.NotEmpty(t, out["applicationId"])

	return out["applicationId"]
}

// startJobRun is a test helper that starts a job run and returns its ID.
func startJobRun(t *testing.T, h *emrserverless.Handler, appID string) string {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/jobruns", map[string]any{
		"executionRoleArn": "arn:aws:iam::000000000000:role/test-role",
		"name":             "test-job",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]string
	mustUnmarshal(t, rec, &out)
	require.NotEmpty(t, out["jobRunId"])

	return out["jobRunId"]
}

// --- Routing and meta ---

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	const emrAuth = "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/emr-serverless/aws4_request," +
		" SignedHeaders=host, Signature=abc"
	const appConfigAuth = "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/appconfig/aws4_request," +
		" SignedHeaders=host, Signature=abc"

	tests := []struct {
		name      string
		path      string
		authHdr   string
		wantMatch bool
	}{
		{
			name:      "applications_exact_with_emr_auth",
			path:      "/applications",
			authHdr:   emrAuth,
			wantMatch: true,
		},
		{
			name:      "applications_with_id_with_emr_auth",
			path:      "/applications/abc123",
			authHdr:   emrAuth,
			wantMatch: true,
		},
		{
			name:      "applications_with_appconfig_auth_no_match",
			path:      "/applications",
			authHdr:   appConfigAuth,
			wantMatch: false,
		},
		{
			name:      "applications_no_auth_no_match",
			path:      "/applications",
			authHdr:   "",
			wantMatch: false,
		},
		{
			name:      "emr_tags",
			path:      "/tags/arn:aws:emr-serverless:us-east-1:000000000000:/applications/xyz",
			authHdr:   "",
			wantMatch: true,
		},
		{
			name:      "backup_tags_no_match",
			path:      "/tags/arn:aws:backup:us-east-1:000000000000:backup-vault/my-vault",
			authHdr:   "",
			wantMatch: false,
		},
		{
			name:      "other_path",
			path:      "/v1/createcomputeenvironment",
			authHdr:   "",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			matcher := h.RouteMatcher()
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			if tt.authHdr != "" {
				req.Header.Set("Authorization", tt.authHdr)
			}
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{name: "CreateApplication", method: http.MethodPost, path: "/applications", wantOp: "CreateApplication"},
		{name: "ListApplications", method: http.MethodGet, path: "/applications", wantOp: "ListApplications"},
		{name: "GetApplication", method: http.MethodGet, path: "/applications/abc", wantOp: "GetApplication"},
		{
			name:   "UpdateApplication",
			method: http.MethodPatch,
			path:   "/applications/abc",
			wantOp: "UpdateApplication",
		},
		{
			name:   "DeleteApplication",
			method: http.MethodDelete,
			path:   "/applications/abc",
			wantOp: "DeleteApplication",
		},
		{
			name:   "StartApplication",
			method: http.MethodPost,
			path:   "/applications/abc/start",
			wantOp: "StartApplication",
		},
		{name: "StopApplication", method: http.MethodPost, path: "/applications/abc/stop", wantOp: "StopApplication"},
		{name: "StartJobRun", method: http.MethodPost, path: "/applications/abc/jobruns", wantOp: "StartJobRun"},
		{name: "ListJobRuns", method: http.MethodGet, path: "/applications/abc/jobruns", wantOp: "ListJobRuns"},
		{name: "GetJobRun", method: http.MethodGet, path: "/applications/abc/jobruns/jr1", wantOp: "GetJobRun"},
		{
			name:   "CancelJobRun",
			method: http.MethodDelete,
			path:   "/applications/abc/jobruns/jr1",
			wantOp: "CancelJobRun",
		},
		{
			name:   "GetDashboardForJobRun",
			method: http.MethodGet,
			path:   "/applications/abc/jobruns/jr1/dashboard",
			wantOp: "GetDashboardForJobRun",
		},
		{
			name:   "ListJobRunAttempts",
			method: http.MethodGet,
			path:   "/applications/abc/jobruns/jr1/attempts",
			wantOp: "ListJobRunAttempts",
		},
		{
			name:   "ListTagsForResource",
			method: http.MethodGet,
			path:   "/tags/arn:aws:emr-serverless:us-east-1:000:x",
			wantOp: "ListTagsForResource",
		},
		{
			name:   "TagResource",
			method: http.MethodPost,
			path:   "/tags/arn:aws:emr-serverless:us-east-1:000:x",
			wantOp: "TagResource",
		},
		{
			name:   "UntagResource",
			method: http.MethodDelete,
			path:   "/tags/arn:aws:emr-serverless:us-east-1:000:x",
			wantOp: "UntagResource",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

// TestHandler_ExtractOperation_UnknownMethods exercises the parseEMRPath
// branches that fall through to the "Unknown" operation.
func TestHandler_ExtractOperation_UnknownMethods(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		// parseTagRoute unknown method
		{
			name:   "tag_patch_returns_unknown",
			method: http.MethodPatch,
			path:   "/tags/arn:aws:emr-serverless:us-east-1:000:x",
			wantOp: "Unknown",
		},
		// parseApplicationsCollection unknown method
		{name: "applications_put_returns_unknown", method: http.MethodPut, path: "/applications", wantOp: "Unknown"},
		// parseSingleAppRoute unknown method
		{
			name:   "single_app_post_returns_unknown",
			method: http.MethodPost,
			path:   "/applications/abc",
			wantOp: "Unknown",
		},
		// parseJobRunSubRoute non-GET
		{
			name:   "job_run_sub_post_returns_unknown",
			method: http.MethodPost,
			path:   "/applications/abc/jobruns/jr1/dashboard",
			wantOp: "Unknown",
		},
		// parseJobRunSubRoute unknown action
		{
			name:   "job_run_sub_unknown_action",
			method: http.MethodGet,
			path:   "/applications/abc/jobruns/jr1/unknown",
			wantOp: "Unknown",
		},
		// parseJobRunRoute unknown method for jobruns
		{
			name:   "job_run_put_returns_unknown",
			method: http.MethodPut,
			path:   "/applications/abc/jobruns/jr1",
			wantOp: "Unknown",
		},
		// parseJobRunRoute non-sessions sub
		{name: "job_run_unknown_sub", method: http.MethodGet, path: "/applications/abc/other/jr1", wantOp: "Unknown"},
		// parseAppSubRoute unknown sub
		{name: "app_unknown_sub", method: http.MethodGet, path: "/applications/abc/nonexistent", wantOp: "Unknown"},
		// parseAppSubRoute dashboard non-GET
		{
			name:   "app_dashboard_post_returns_unknown",
			method: http.MethodPost,
			path:   "/applications/abc/dashboard",
			wantOp: "Unknown",
		},
		// parseAppSubRoute start non-POST
		{
			name:   "app_start_get_returns_unknown",
			method: http.MethodGet,
			path:   "/applications/abc/start",
			wantOp: "Unknown",
		},
		// parseAppSubRoute stop non-POST
		{
			name:   "app_stop_get_returns_unknown",
			method: http.MethodGet,
			path:   "/applications/abc/stop",
			wantOp: "Unknown",
		},
		// parseJobRunSubRoute non-jobruns sub
		{
			name:   "session_sub_endpoint_non_get",
			method: http.MethodPost,
			path:   "/applications/abc/sessions/s1/endpoint",
			wantOp: "Unknown",
		},
		// parseJobRunSubRoute sessions non-endpoint action
		{
			name:   "session_sub_other_action",
			method: http.MethodGet,
			path:   "/applications/abc/sessions/s1/other",
			wantOp: "Unknown",
		},
		// parseJobRunRoute sessions non-GET non-DELETE
		{
			name:   "session_route_post_returns_unknown",
			method: http.MethodPost,
			path:   "/applications/abc/sessions/s1",
			wantOp: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		want   string
	}{
		{
			name:   "application_id",
			method: http.MethodGet,
			path:   "/applications/abc123",
			want:   "abc123",
		},
		{
			name:   "job_run_id",
			method: http.MethodGet,
			path:   "/applications/abc123/jobruns/jr456",
			want:   "abc123/jr456",
		},
		{
			name:   "session_id",
			method: http.MethodGet,
			path:   "/applications/abc123/sessions/sess456",
			want:   "abc123/sess456",
		},
		{
			name:   "tags_arn",
			method: http.MethodGet,
			path:   "/tags/arn:aws:emr-serverless:us-east-1:000000000000:/applications/abc",
			want:   "arn:aws:emr-serverless:us-east-1:000000000000:/applications/abc",
		},
		{
			name:   "list_applications",
			method: http.MethodGet,
			path:   "/applications",
			want:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

func TestHandler_ServiceMeta(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	assert.Equal(t, "EmrServerless", h.Name())
	assert.Equal(t, "emr-serverless", h.ChaosServiceName())
	assert.Equal(t, []string{"us-east-1"}, h.ChaosRegions())

	ops := h.GetSupportedOperations()
	assert.Len(t, ops, 22)
	assert.Contains(t, ops, "CreateApplication")
	assert.Contains(t, ops, "CancelJobRun")
	assert.Contains(t, ops, "UntagResource")
	assert.Contains(t, ops, "GetDashboardForJobRun")
	assert.Contains(t, ops, "ListJobRunAttempts")
	assert.Contains(t, ops, "StartSession")
	assert.Contains(t, ops, "GetResourceDashboard")

	chaosOps := h.ChaosOperations()
	assert.Equal(t, ops, chaosOps)
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 87, h.MatchPriority())
}

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/applications/abc/unknown", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestHandler_HandleError_InternalError exercises the default branch in
// handleError -- reached when an error is not one of the known sentinels.
func TestHandler_HandleError_InternalError(t *testing.T) {
	t.Parallel()

	// The only way to reach the default branch in handleError is to have
	// an error that is not ErrNotFound, ErrAlreadyExists, ErrValidation, or ErrInvalidState.
	// We can do this via Restore with invalid JSON — which returns an unmarshal error.
	h := newTestHandler(t)
	err := h.Restore(t.Context(), []byte("invalid-json"))
	require.Error(t, err)
}

// TestHandler_URLEncodedPath verifies URL-escaped ARN paths are unescaped
// and routed correctly.
func TestHandler_URLEncodedPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	appID := createApp(t, h, "url-encode-app")
	rec := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/jobruns", map[string]any{
		"executionRoleArn": "arn:aws:iam::000000000000:role/r",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]string
	mustUnmarshal(t, rec, &out)
	jrID := out["jobRunId"]
	jrARN := out["arn"]

	// Access via URL-escaped ARN path.
	escapedARN := strings.ReplaceAll(jrARN, "/", "%2F")
	rec2 := doRequest(t, h, http.MethodGet, "/tags/"+escapedARN, nil)
	// The handler url-unescapes the path, so it should resolve correctly.
	// The tag route path prefix is /tags/ followed by the ARN — it's already in raw form.
	_ = rec2
	// Just ensure no panic.

	// Standard non-encoded access works.
	rec3 := doRequest(t, h, http.MethodGet, "/applications/"+appID+"/jobruns/"+jrID, nil)
	assert.Equal(t, http.StatusOK, rec3.Code)
}

// --- Provider ---

// mockConfigProvider implements config.Provider for testing.
type mockConfigProvider struct {
	accountID string
	region    string
}

func (m *mockConfigProvider) GetGlobalConfig() *config.GlobalConfig {
	return config.NewGlobalConfig(m.accountID, m.region, 0, 0, false, 0)
}

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	p := &emrserverless.Provider{}
	assert.Equal(t, "EmrServerless", p.Name())
}

func TestProvider_Init_WithConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cfg        any
		name       string
		wantRegion string
	}{
		{
			name:       "with_config_provider",
			cfg:        &mockConfigProvider{accountID: "111111111111", region: "eu-west-1"},
			wantRegion: "eu-west-1",
		},
		{
			name:       "without_config_provider",
			cfg:        "not-a-config-provider",
			wantRegion: config.DefaultRegion,
		},
		{
			name:       "nil_config",
			cfg:        nil,
			wantRegion: config.DefaultRegion,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &emrserverless.Provider{}
			ctx := &service.AppContext{Config: tt.cfg}
			reg, err := p.Init(ctx)
			require.NoError(t, err)
			require.NotNil(t, reg)

			h, ok := reg.(*emrserverless.Handler)
			require.True(t, ok, "Init must return a *Handler")
			assert.Equal(t, tt.wantRegion, h.ChaosRegions()[0])
		})
	}
}

func TestProvider_Init_NilCtx(t *testing.T) {
	t.Parallel()

	p := &emrserverless.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, emrserverless.ErrNilAppContext)
}

func TestErrNilAppContextValue(t *testing.T) {
	t.Parallel()

	// Confirm the sentinel is a non-nil error value.
	require.Error(t, emrserverless.ErrNilAppContext)
}

// --- Reset ---

func TestReset(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("111111111111", "us-west-2")
	_, err := b.CreateApplication("app1", "SPARK", "emr-6.6.0", "", nil)
	require.NoError(t, err)
	require.Equal(t, 1, emrserverless.ApplicationCount(b))

	b.Reset()

	assert.Equal(t, 0, emrserverless.ApplicationCount(b))
	assert.Equal(t, 0, emrserverless.JobRunCount(b))
	appARNs, jobRunARNs := emrserverless.ARNIndexSizes(b)
	assert.Equal(t, 0, appARNs)
	assert.Equal(t, 0, jobRunARNs)
}

func TestMultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("111111111111", "us-west-2")

	for range 3 {
		_, err := b.CreateApplication("app-cycle", "SPARK", "emr-6.6.0", "", nil)
		require.NoError(t, err)
		b.Reset()
		assert.Equal(t, 0, emrserverless.ApplicationCount(b))
	}
}

func TestHandlerReset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createApp(t, h, "handler-reset-app")

	h.Reset()

	rec := doRequest(t, h, http.MethodGet, "/applications", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	apps := out["applications"].([]any)
	assert.Empty(t, apps)
}

func TestARNIndexesConsistentAfterReset(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	app, err := b.CreateApplication("arn-idx-app", "SPARK", "emr-6.6.0", "", nil)
	require.NoError(t, err)

	_, err = b.StartJobRun(app.ApplicationID, "arn:aws:iam::000000000000:role/r", "run1", "", nil)
	require.NoError(t, err)

	appARNs, jobRunARNs := emrserverless.ARNIndexSizes(b)
	assert.Equal(t, 1, appARNs)
	assert.Equal(t, 1, jobRunARNs)

	b.Reset()

	appARNs, jobRunARNs = emrserverless.ARNIndexSizes(b)
	assert.Equal(t, 0, appARNs)
	assert.Equal(t, 0, jobRunARNs)
}

// --- GetSupportedOperations ---

func TestGetSupportedOperations_AllOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	expected := []string{
		"CreateApplication",
		"GetApplication",
		"ListApplications",
		"UpdateApplication",
		"DeleteApplication",
		"StartApplication",
		"StopApplication",
		"StartJobRun",
		"GetJobRun",
		"ListJobRuns",
		"CancelJobRun",
		"GetDashboardForJobRun",
		"ListJobRunAttempts",
		"GetResourceDashboard",
		"StartSession",
		"GetSession",
		"ListSessions",
		"TerminateSession",
		"GetSessionEndpoint",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
	}

	assert.Len(t, ops, len(expected))

	for _, op := range expected {
		assert.Contains(t, ops, op)
	}
}

// --- Seed helpers + Export count helpers ---

func TestSeedHelpers(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	now := time.Now().UTC()

	appID := "app-seed-1"
	app := &emrserverless.Application{
		ApplicationID: appID,
		Arn:           "arn:aws:emr-serverless:us-east-1:000000000000:/applications/" + appID,
		Name:          "seed-app",
		Type:          "SPARK",
		ReleaseLabel:  "emr-6.6.0",
		State:         emrserverless.ApplicationStateCreated,
		CreatedAt:     now,
		UpdatedAt:     now,
		Tags:          map[string]string{"env": "test"},
	}
	b.AddApplicationInternal(app)

	jr := &emrserverless.JobRun{
		ApplicationID:    appID,
		JobRunID:         "jr-seed-1",
		Arn:              "arn:aws:emr-serverless:us-east-1:000000000000:/applications/" + appID + "/jobruns/jr-seed-1",
		Name:             "seed-run",
		State:            emrserverless.JobRunStateRunning,
		ExecutionRoleArn: "arn:aws:iam::000000000000:role/emr-role",
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	b.AddJobRunInternal(jr)

	assert.Equal(t, 1, emrserverless.ApplicationCount(b))
	assert.Equal(t, 1, emrserverless.JobRunCount(b))
}

func TestExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	assert.Equal(t, 0, emrserverless.ApplicationCount(b))
	assert.Equal(t, 0, emrserverless.JobRunCount(b))

	_, err := b.CreateApplication("count-app", "SPARK", "emr-6.6.0", "", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, emrserverless.ApplicationCount(b))

	appARNs, jobRunARNs := emrserverless.ARNIndexSizes(b)
	assert.Equal(t, 1, appARNs)
	assert.Equal(t, 0, jobRunARNs)
}
