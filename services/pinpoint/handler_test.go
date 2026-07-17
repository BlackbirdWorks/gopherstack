package pinpoint_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pinpoint"
)

const (
	testRegion    = "us-east-1"
	testAccountID = "123456789012"
)

func newHandlerForTest(t *testing.T) *pinpoint.Handler {
	t.Helper()

	b := pinpoint.NewInMemoryBackend(testRegion, testAccountID)
	h := pinpoint.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	return h
}

func doPinpointRequest(t *testing.T, h *pinpoint.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error

		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(
		"Authorization",
		"AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/mobiletargeting/aws4_request",
	)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// doRawPinpointRequest issues a request with a raw (already-encoded) body,
// bypassing doPinpointRequest's automatic JSON marshalling.
func doRawPinpointRequest(
	t *testing.T,
	h *pinpoint.Handler,
	method, path string,
	rawBody []byte,
) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()

	req := httptest.NewRequest(method, path, bytes.NewReader(rawBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(
		"Authorization",
		"AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/mobiletargeting/aws4_request",
	)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// createTestApp is a helper that creates a Pinpoint application and returns its ID.
func createTestApp(t *testing.T, h *pinpoint.Handler, name string) string {
	t.Helper()

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": name})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

	id, _ := resp["Id"].(string)
	require.NotEmpty(t, id)

	return id
}

// pinpointJSON decodes an HTTP response body as JSON into a generic map.
func pinpointJSON(t *testing.T, body []byte) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(body, &m))

	return m
}

func TestHandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	assert.Equal(t, 122, pinpoint.HandlerOpsLen(h))
}

// ──────────────────────────────────────────────────
// Method-not-allowed paths
// ──────────────────────────────────────────────────

func TestMethodNotAllowedOnApps(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodDelete, "/v1/apps", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestMethodNotAllowedOnRecommenders(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodDelete, "/v1/recommenders", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

// ──────────────────────────────────────────────────
// Helpers for raw requests (no auto-JSON body)
// ──────────────────────────────────────────────────

func TestHandler_ServiceInterface(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "Name",
			run: func(t *testing.T) {
				t.Helper()
				assert.Equal(t, "Pinpoint", h.Name())
			},
		},
		{
			name: "ChaosServiceName",
			run: func(t *testing.T) {
				t.Helper()
				assert.Equal(t, "mobiletargeting", h.ChaosServiceName())
			},
		},
		{
			name: "ChaosOperations",
			run: func(t *testing.T) {
				t.Helper()
				assert.NotEmpty(t, h.ChaosOperations())
			},
		},
		{
			name: "ChaosRegions",
			run: func(t *testing.T) {
				t.Helper()
				assert.Equal(t, []string{testRegion}, h.ChaosRegions())
			},
		},
		{
			name: "RouteMatcher",
			run: func(t *testing.T) {
				t.Helper()
				assert.NotNil(t, h.RouteMatcher())
			},
		},
		{
			name: "MatchPriority",
			run: func(t *testing.T) {
				t.Helper()
				assert.Equal(t, 87, h.MatchPriority())
			},
		},
		{
			name: "GetSupportedOperations",
			run: func(t *testing.T) {
				t.Helper()
				ops := h.GetSupportedOperations()
				assert.Contains(t, ops, "CreateCampaign")
				assert.Contains(t, ops, "CreateEmailTemplate")
				assert.Contains(t, ops, "CreateExportJob")
				assert.Contains(t, ops, "CreateImportJob")
				assert.Contains(t, ops, "CreateInAppTemplate")
				assert.Contains(t, ops, "CreateJourney")
				assert.Contains(t, ops, "CreatePushTemplate")
				assert.Contains(t, ops, "CreateRecommenderConfiguration")
				assert.Contains(t, ops, "CreateSegment")
				assert.Contains(t, ops, "CreateSmsTemplate")
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

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	tests := []struct {
		method string
		path   string
		name   string
		wantOp string
	}{
		{name: "CreateApp", method: http.MethodPost, path: "/v1/apps", wantOp: "CreateApp"},
		{name: "GetApps", method: http.MethodGet, path: "/v1/apps", wantOp: "GetApps"},
		{name: "GetApps_trailing_slash", method: http.MethodGet, path: "/v1/apps/", wantOp: "GetApps"},
		{name: "GetApp", method: http.MethodGet, path: "/v1/apps/abc123", wantOp: "GetApp"},
		{name: "DeleteApp", method: http.MethodDelete, path: "/v1/apps/abc123", wantOp: "DeleteApp"},
		{
			name:   "ListTagsForResource",
			method: http.MethodGet,
			path:   "/v1/tags/arn%3Aaws",
			wantOp: "ListTagsForResource",
		},
		{name: "TagResource", method: http.MethodPost, path: "/v1/tags/arn%3Aaws", wantOp: "TagResource"},
		{name: "UntagResource", method: http.MethodDelete, path: "/v1/tags/arn%3Aaws", wantOp: "UntagResource"},
		{name: "TagResource_unknown_method", method: http.MethodPut, path: "/v1/tags/arn%3Aaws", wantOp: "Unknown"},
		{
			name:   "CreateRecommenderConfiguration",
			method: http.MethodPost,
			path:   "/v1/recommenders",
			wantOp: "CreateRecommenderConfiguration",
		},
		{
			name:   "CreateRecommenderConfiguration_slash",
			method: http.MethodPost,
			path:   "/v1/recommenders/",
			wantOp: "CreateRecommenderConfiguration",
		},
		{
			name:   "recommenders_list_method",
			method: http.MethodGet,
			path:   "/v1/recommenders",
			wantOp: "GetRecommenderConfigurations",
		},
		{
			name:   "CreateEmailTemplate",
			method: http.MethodPost,
			path:   "/v1/templates/my-tpl/email",
			wantOp: "CreateEmailTemplate",
		},
		{
			name:   "CreateInAppTemplate",
			method: http.MethodPost,
			path:   "/v1/templates/my-tpl/inapp",
			wantOp: "CreateInAppTemplate",
		},
		{
			name:   "CreatePushTemplate",
			method: http.MethodPost,
			path:   "/v1/templates/my-tpl/push",
			wantOp: "CreatePushTemplate",
		},
		{
			name:   "CreateSmsTemplate",
			method: http.MethodPost,
			path:   "/v1/templates/my-tpl/sms",
			wantOp: "CreateSmsTemplate",
		},
		{
			name:   "template_get_method",
			method: http.MethodGet,
			path:   "/v1/templates/my-tpl/email",
			wantOp: "GetEmailTemplate",
		},
		{name: "unknown_path", method: http.MethodGet, path: "/v1/unknown", wantOp: "Unknown"},
		{
			name:   "apps_sub_path_campaigns",
			method: http.MethodPost,
			path:   "/v1/apps/abc/campaigns",
			wantOp: "CreateCampaign",
		},
		{
			name:   "apps_sub_path_journeys",
			method: http.MethodPost,
			path:   "/v1/apps/abc/journeys",
			wantOp: "CreateJourney",
		},
		{
			name:   "apps_sub_path_segments",
			method: http.MethodPost,
			path:   "/v1/apps/abc/segments",
			wantOp: "CreateSegment",
		},
		{
			name:   "apps_sub_path_export_jobs",
			method: http.MethodPost,
			path:   "/v1/apps/abc/jobs/export",
			wantOp: "CreateExportJob",
		},
		{
			name:   "apps_sub_path_import_jobs",
			method: http.MethodPost,
			path:   "/v1/apps/abc/jobs/import",
			wantOp: "CreateImportJob",
		},
		{
			name:   "apps_sub_path_settings_get",
			method: http.MethodGet,
			path:   "/v1/apps/abc/settings",
			wantOp: "GetApplicationSettings",
		},
		{
			name:   "apps_sub_path_settings_put",
			method: http.MethodPut,
			path:   "/v1/apps/abc/settings",
			wantOp: "UpdateApplicationSettings",
		},
		{name: "apps_sub_path_unknown", method: http.MethodGet, path: "/v1/apps/abc/unknown", wantOp: "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			op := h.ExtractOperation(c)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	tests := []struct {
		path         string
		name         string
		wantResource string
	}{
		{name: "apps_root", path: "/v1/apps", wantResource: ""},
		{name: "app_id", path: "/v1/apps/abc123", wantResource: "abc123"},
		{name: "app_sub_path", path: "/v1/apps/abc/campaigns", wantResource: "abc/campaigns"},
		{
			name:         "tags_arn",
			path:         "/v1/tags/arn%3Aaws%3Amobiletargeting%3Aus-east-1%3A123",
			wantResource: "arn:aws:mobiletargeting:us-east-1:123",
		},
		{name: "recommenders", path: "/v1/recommenders", wantResource: ""},
		{name: "template_path", path: "/v1/templates/my-tpl/email", wantResource: "my-tpl/email"},
		{name: "unknown_path", path: "/v1/unknown", wantResource: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			resource := h.ExtractResource(c)
			assert.Equal(t, tt.wantResource, resource)
		})
	}
}

func TestHandler_ErrorPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		method     string
		path       string
		name       string
		wantStatus int
	}{
		{
			name:       "ServeHTTP_unknown_path",
			method:     http.MethodGet,
			path:       "/v1/unknown/path",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "dispatchApps_method_not_allowed",
			method:     http.MethodPut,
			path:       "/v1/apps",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "dispatchApp_method_not_allowed",
			method:     http.MethodPost,
			path:       "/v1/apps/abc123",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "dispatchAppSubPath_settings_method_not_allowed",
			method:     http.MethodDelete,
			path:       "/v1/apps/abc/settings",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "dispatchAppSubPath_unknown_sub_path",
			method:     http.MethodGet,
			path:       "/v1/apps/abc/unknown-resource",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "createCampaign_bad_json",
			method:     http.MethodPost,
			path:       "/v1/apps/abc/campaigns",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "createExportJob_bad_json",
			method:     http.MethodPost,
			path:       "/v1/apps/abc/jobs/export",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "createImportJob_bad_json",
			method:     http.MethodPost,
			path:       "/v1/apps/abc/jobs/import",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "createJourney_bad_json",
			method:     http.MethodPost,
			path:       "/v1/apps/abc/journeys",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "createSegment_bad_json",
			method:     http.MethodPost,
			path:       "/v1/apps/abc/segments",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "dispatchRecommenders_method_not_allowed",
			method:     http.MethodDelete,
			path:       "/v1/recommenders",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "createRecommenderConfiguration_bad_json",
			method:     http.MethodPost,
			path:       "/v1/recommenders",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "dispatchTemplates_method_not_allowed",
			method:     http.MethodPatch,
			path:       "/v1/templates/my-tpl/email",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "dispatchTemplates_unsupported_type",
			method:     http.MethodPost,
			path:       "/v1/templates/my-tpl/unknown-type",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "createEmailTemplate_bad_json",
			method:     http.MethodPost,
			path:       "/v1/templates/my-tpl/email",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "createInAppTemplate_bad_json",
			method:     http.MethodPost,
			path:       "/v1/templates/my-tpl/inapp",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "createPushTemplate_bad_json",
			method:     http.MethodPost,
			path:       "/v1/templates/my-tpl/push",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "createSmsTemplate_bad_json",
			method:     http.MethodPost,
			path:       "/v1/templates/my-tpl/sms",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "dispatchTemplates_list",
			method:     http.MethodGet,
			path:       "/v1/templates/",
			wantStatus: http.StatusOK,
		},
		{
			name:       "handleTagResource_bad_json",
			method:     http.MethodPost,
			path:       "/v1/tags/arn%3Aaws%3Amobiletargeting%3Aus-east-1%3A123456789012%3Aapps%2Fabc",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "handleUntagResource_resource_not_found",
			method:     http.MethodDelete,
			path:       "/v1/tags/arn%3Aaws%3Amobiletargeting%3Aus-east-1%3A123456789012%3Aapps%2Fabc?tagKeys=foo",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "handleListTagsForResource_not_found",
			method:     http.MethodGet,
			path:       "/v1/tags/arn%3Aaws%3Amobiletargeting%3Aus-east-1%3A123456789012%3Aapps%2Fnot-found",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			var bodyBytes []byte
			if s, ok := tt.body.(string); ok {
				bodyBytes = []byte(s)
			} else if tt.body != nil {
				var err error
				bodyBytes, err = json.Marshal(tt.body)
				require.NoError(t, err)
			}

			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
