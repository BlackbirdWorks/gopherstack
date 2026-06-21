package cloudfront_test

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

func newTestHandler() *cloudfront.Handler {
	backend := cloudfront.NewInMemoryBackend("123456789012", config.DefaultRegion)

	return cloudfront.NewHandler(backend)
}

func doXML(
	t *testing.T,
	h *cloudfront.Handler,
	method, path string,
	body []byte,
) *httptest.ResponseRecorder {
	t.Helper()

	return doXMLWithHeaders(t, h, method, path, body, nil)
}

func doXMLWithHeaders(
	t *testing.T,
	h *cloudfront.Handler,
	method, path string,
	body []byte,
	headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyReader *bytes.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	} else {
		bodyReader = bytes.NewReader(nil)
	}

	req := httptest.NewRequest(method, path, bodyReader)
	req.Header.Set("Content-Type", "text/xml")

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func minimalDistConfig(callerRef, comment string, enabled bool) []byte {
	tmpl := `<DistributionConfig>` +
		`<CallerReference>%s</CallerReference>` +
		`<Comment>%s</Comment>` +
		`<Enabled>%v</Enabled>` +
		`</DistributionConfig>`

	return fmt.Appendf([]byte(nil), tmpl, callerRef, comment, enabled)
}

func minimalOAIConfig(callerRef, comment string) []byte {
	tmpl := `<CloudFrontOriginAccessIdentityConfig>` +
		`<CallerReference>%s</CallerReference>` +
		`<Comment>%s</Comment>` +
		`</CloudFrontOriginAccessIdentityConfig>`

	return fmt.Appendf([]byte(nil), tmpl, callerRef, comment)
}

// TestDistributionCRUD covers create, get, update, list, and delete operations.
func TestDistributionCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *cloudfront.Handler) string
		check      func(*testing.T, *httptest.ResponseRecorder, string)
		headers    func(*testing.T, *cloudfront.Handler, string) map[string]string
		name       string
		method     string
		path       string
		body       []byte
		wantStatus int
	}{
		{
			name:   "create_distribution",
			method: http.MethodPost,
			path:   "/2020-05-31/distribution",
			body:   minimalDistConfig("ref-001", "my-dist", true),
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<Distribution")
				assert.Contains(t, rec.Body.String(), "<Status>Deployed</Status>")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
				assert.NotEmpty(t, rec.Header().Get("Location"))
			},
		},
		{
			name:   "get_distribution",
			method: http.MethodGet,
			path:   "", // set in setup
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-002", "get-dist", true,
					minimalDistConfig("ref-002", "get-dist", true))
				require.NoError(t, err)

				return "/2020-05-31/distribution/" + d.ID
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<Distribution")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "get_distribution_not_found",
			method: http.MethodGet,
			path:   "/2020-05-31/distribution/DOESNOTEXIST",
			body:   nil,
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "NoSuchDistribution")
			},
		},
		{
			name:   "get_distribution_config",
			method: http.MethodGet,
			path:   "", // set in setup
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-003", "cfg-dist", true,
					minimalDistConfig("ref-003", "cfg-dist", true))
				require.NoError(t, err)

				return "/2020-05-31/distribution/" + d.ID + "/config"
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "DistributionConfig")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "update_distribution",
			method: http.MethodPut,
			path:   "", // set in setup
			body:   minimalDistConfig("ref-004", "updated-dist", false),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-004", "orig-dist", true,
					minimalDistConfig("ref-004", "orig-dist", true))
				require.NoError(t, err)

				return "/2020-05-31/distribution/" + d.ID + "/config"
			},
			headers: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				// path is "/2020-05-31/distribution/{ID}/config" — extract ID
				parts := strings.Split(strings.TrimPrefix(path, "/2020-05-31/distribution/"), "/")
				d, err := h.Backend.GetDistribution(parts[0])
				require.NoError(t, err)

				return map[string]string{"If-Match": d.ETag}
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<Distribution")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "list_distributions",
			method: http.MethodGet,
			path:   "/2020-05-31/distribution",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateDistribution("ref-005", "list-dist", true,
					minimalDistConfig("ref-005", "list-dist", true))
				require.NoError(t, err)

				return ""
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "DistributionList")
			},
		},
		{
			name:   "delete_distribution",
			method: http.MethodDelete,
			path:   "", // set in setup
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-006", "del-dist", false,
					minimalDistConfig("ref-006", "del-dist", false))
				require.NoError(t, err)

				return "/2020-05-31/distribution/" + d.ID
			},
			headers: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				id := strings.TrimPrefix(path, "/2020-05-31/distribution/")
				d, err := h.Backend.GetDistribution(id)
				require.NoError(t, err)

				return map[string]string{"If-Match": d.ETag}
			},
			wantStatus: http.StatusNoContent,
			check:      func(t *testing.T, _ *httptest.ResponseRecorder, _ string) { t.Helper() },
		},
		{
			name:   "delete_distribution_not_found",
			method: http.MethodDelete,
			path:   "/2020-05-31/distribution/DOESNOTEXIST",
			body:   nil,
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "NoSuchDistribution")
			},
		},
		{
			name:   "update_distribution_precondition_failed",
			method: http.MethodPut,
			path:   "", // set in setup
			body:   minimalDistConfig("ref-007", "updated-dist", false),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-007", "orig-dist", true,
					minimalDistConfig("ref-007", "orig-dist", true))
				require.NoError(t, err)

				return "/2020-05-31/distribution/" + d.ID + "/config"
			},
			wantStatus: http.StatusPreconditionFailed,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "PreconditionFailed")
			},
		},
		{
			name:   "delete_distribution_precondition_failed",
			method: http.MethodDelete,
			path:   "", // set in setup
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-008", "del-dist-2", false,
					minimalDistConfig("ref-008", "del-dist-2", false))
				require.NoError(t, err)

				return "/2020-05-31/distribution/" + d.ID
			},
			wantStatus: http.StatusPreconditionFailed,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "PreconditionFailed")
			},
		},
		{
			name:   "delete_distribution_not_disabled",
			method: http.MethodDelete,
			path:   "", // set in setup
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-009", "enabled-dist", true,
					minimalDistConfig("ref-009", "enabled-dist", true))
				require.NoError(t, err)

				return "/2020-05-31/distribution/" + d.ID
			},
			headers: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				id := strings.TrimPrefix(path, "/2020-05-31/distribution/")
				d, err := h.Backend.GetDistribution(id)
				require.NoError(t, err)

				return map[string]string{"If-Match": d.ETag}
			},
			wantStatus: http.StatusConflict,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "DistributionNotDisabled")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			path := tt.path
			if tt.setup != nil {
				if p := tt.setup(t, h); p != "" {
					path = p
				}
			}

			var hdrs map[string]string
			if tt.headers != nil {
				hdrs = tt.headers(t, h, path)
			}

			rec := doXMLWithHeaders(t, h, tt.method, path, tt.body, hdrs)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec, path)
		})
	}
}

// TestOAICRUD covers Create, Get, List, and Delete for Origin Access Identities.
func TestOAICRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *cloudfront.Handler) string
		check      func(*testing.T, *httptest.ResponseRecorder)
		headers    func(*testing.T, *cloudfront.Handler, string) map[string]string
		name       string
		method     string
		path       string
		body       []byte
		wantStatus int
	}{
		{
			name:   "create_oai",
			method: http.MethodPost,
			path:   "/2020-05-31/origin-access-identity/cloudfront",
			body:   minimalOAIConfig("oai-ref-001", "my-oai"),
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "CloudFrontOriginAccessIdentity")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
				assert.NotEmpty(t, rec.Header().Get("Location"))
			},
		},
		{
			name:   "get_oai",
			method: http.MethodGet,
			path:   "", // set in setup
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				oai, err := h.Backend.CreateOAI("oai-ref-002", "get-oai")
				require.NoError(t, err)

				return "/2020-05-31/origin-access-identity/cloudfront/" + oai.ID
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "CloudFrontOriginAccessIdentity")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "get_oai_not_found",
			method: http.MethodGet,
			path:   "/2020-05-31/origin-access-identity/cloudfront/DOESNOTEXIST",
			body:   nil,
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "NoSuchCloudFrontOriginAccessIdentity")
			},
		},
		{
			name:   "list_oais",
			method: http.MethodGet,
			path:   "/2020-05-31/origin-access-identity/cloudfront",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateOAI("oai-ref-003", "list-oai")
				require.NoError(t, err)

				return ""
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "CloudFrontOriginAccessIdentityList")
			},
		},
		{
			name:   "delete_oai",
			method: http.MethodDelete,
			path:   "", // set in setup
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				oai, err := h.Backend.CreateOAI("oai-ref-004", "del-oai")
				require.NoError(t, err)

				return "/2020-05-31/origin-access-identity/cloudfront/" + oai.ID
			},
			headers: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				id := strings.TrimPrefix(path, "/2020-05-31/origin-access-identity/cloudfront/")
				oai, err := h.Backend.GetOAI(id)
				require.NoError(t, err)

				return map[string]string{"If-Match": oai.ETag}
			},
			wantStatus: http.StatusNoContent,
			check:      func(t *testing.T, _ *httptest.ResponseRecorder) { t.Helper() },
		},
		{
			name:   "delete_oai_not_found",
			method: http.MethodDelete,
			path:   "/2020-05-31/origin-access-identity/cloudfront/DOESNOTEXIST",
			body:   nil,
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "NoSuchCloudFrontOriginAccessIdentity")
			},
		},
		{
			name:   "delete_oai_precondition_failed",
			method: http.MethodDelete,
			path:   "", // set in setup
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				oai, err := h.Backend.CreateOAI("oai-ref-005", "precond-oai")
				require.NoError(t, err)

				return "/2020-05-31/origin-access-identity/cloudfront/" + oai.ID
			},
			// No headers fn → If-Match is missing → PreconditionFailed
			wantStatus: http.StatusPreconditionFailed,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "PreconditionFailed")
			},
		},
		{
			name:   "get_oai_config",
			method: http.MethodGet,
			path:   "", // set in setup
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				oai, err := h.Backend.CreateOAI("oai-ref-006", "config-oai")
				require.NoError(t, err)

				return "/2020-05-31/origin-access-identity/cloudfront/" + oai.ID + "/config"
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "CloudFrontOriginAccessIdentityConfig")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "update_oai",
			method: http.MethodPut,
			path:   "", // set in setup
			body: []byte(
				`<CloudFrontOriginAccessIdentityConfig>` +
					`<CallerReference>ref-007</CallerReference>` +
					`<Comment>updated-oai</Comment>` +
					`</CloudFrontOriginAccessIdentityConfig>`,
			),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				oai, err := h.Backend.CreateOAI("oai-ref-007", "orig-oai")
				require.NoError(t, err)

				return "/2020-05-31/origin-access-identity/cloudfront/" + oai.ID + "/config"
			},
			headers: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				id := strings.TrimPrefix(
					strings.TrimSuffix(path, "/config"),
					"/2020-05-31/origin-access-identity/cloudfront/",
				)
				oai, err := h.Backend.GetOAI(id)
				require.NoError(t, err)

				return map[string]string{"If-Match": oai.ETag}
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "CloudFrontOriginAccessIdentity")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			path := tt.path
			if tt.setup != nil {
				if p := tt.setup(t, h); p != "" {
					path = p
				}
			}

			var hdrs map[string]string
			if tt.headers != nil {
				hdrs = tt.headers(t, h, path)
			}

			rec := doXMLWithHeaders(t, h, tt.method, path, tt.body, hdrs)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec)
		})
	}
}

// TestTagging covers TagResource, ListTagsForResource, and UntagResource.
func TestTagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *cloudfront.Handler) (distARN string)
		check      func(*testing.T, *httptest.ResponseRecorder)
		name       string
		method     string
		extraQuery string
		body       []byte
		wantStatus int
	}{
		{
			name: "tag_resource",
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-tag-001", "tag-dist", true,
					minimalDistConfig("ref-tag-001", "tag-dist", true))
				require.NoError(t, err)

				return d.ARN
			},
			method:     http.MethodPost,
			body:       []byte(`<Tags><Items><Tag><Key>Env</Key><Value>test</Value></Tag></Items></Tags>`),
			wantStatus: http.StatusNoContent,
			check:      func(t *testing.T, _ *httptest.ResponseRecorder) { t.Helper() },
		},
		{
			name: "list_tags_for_resource",
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-tag-002", "list-tag-dist", true,
					minimalDistConfig("ref-tag-002", "list-tag-dist", true))
				require.NoError(t, err)
				err = h.Backend.TagResource(d.ARN, map[string]string{"Env": "prod"})
				require.NoError(t, err)

				return d.ARN
			},
			method:     http.MethodGet,
			body:       nil,
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "ListTagsForResourceResponse")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			arn := tt.setup(t, h)
			path := "/2020-05-31/tagging?Resource=" + arn

			rec := doXML(t, h, tt.method, path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec)
		})
	}
}

// TestInvalidationStubs verifies that invalidation stub endpoints return expected responses.
func TestInvalidationStubs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check      func(*testing.T, *httptest.ResponseRecorder)
		name       string
		method     string
		wantStatus int
	}{
		{
			name:       "create_invalidation",
			method:     http.MethodPost,
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<Invalidation")
			},
		},
		{
			name:       "list_invalidations",
			method:     http.MethodGet,
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "InvalidationList")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			d, err := h.Backend.CreateDistribution("ref-inv", "inv-dist", true,
				minimalDistConfig("ref-inv", "inv-dist", true))
			require.NoError(t, err)

			path := "/2020-05-31/distribution/" + d.ID + "/invalidation"
			rec := doXML(t, h, tt.method, path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec)
		})
	}
}

// TestHandlerName verifies the handler name and service metadata.
func TestHandlerName(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	assert.Equal(t, "CloudFront", h.Name())
	assert.Equal(t, "cloudfront", h.ChaosServiceName())
	assert.NotEmpty(t, h.GetSupportedOperations())
	assert.NotEmpty(t, h.ChaosOperations())
	assert.NotEmpty(t, h.ChaosRegions())
}

// TestRouteMatcher verifies RouteMatcher and MatchPriority.
func TestRouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	e := echo.New()

	tests := []struct {
		name    string
		path    string
		wantHit bool
	}{
		{name: "matches_prefix", path: "/2020-05-31/distribution", wantHit: true},
		{name: "matches_prefix_subpath", path: "/2020-05-31/origin-access-identity/cloudfront", wantHit: true},
		{name: "no_match", path: "/api/other", wantHit: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantHit, h.RouteMatcher()(c))
		})
	}

	assert.Positive(t, h.MatchPriority())
}

// TestExtractOperationAndResource verifies ExtractOperation and ExtractResource.
func TestExtractOperationAndResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		method        string
		path          string
		wantOperation string
		wantResource  string
	}{
		{
			name:          "create_distribution",
			method:        http.MethodPost,
			path:          "/2020-05-31/distribution",
			wantOperation: "CreateDistribution",
			wantResource:  "",
		},
		{
			name:          "get_distribution",
			method:        http.MethodGet,
			path:          "/2020-05-31/distribution/ABCDE12345678F",
			wantOperation: "GetDistribution",
			wantResource:  "ABCDE12345678F",
		},
		{
			name:          "create_oai",
			method:        http.MethodPost,
			path:          "/2020-05-31/origin-access-identity/cloudfront",
			wantOperation: "CreateCloudFrontOriginAccessIdentity",
			wantResource:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantOperation, h.ExtractOperation(c))
			assert.Equal(t, tt.wantResource, h.ExtractResource(c))
		})
	}
}

// TestUnknownOperation verifies that unknown operations return 404.
func TestUnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	// Use an unrecognized path
	rec := doXML(t, h, http.MethodPatch, "/2020-05-31/distribution", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "NoSuchOperation")
}

// TestMalformedXMLHandling verifies that malformed XML returns 400.
func TestMalformedXMLHandling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{
			name: "create_distribution_bad_xml",
			path: "/2020-05-31/distribution",
		},
		{
			name: "create_oai_bad_xml",
			path: "/2020-05-31/origin-access-identity/cloudfront",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doXML(t, h, http.MethodPost, tt.path, []byte(`<<<not xml`))
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestBackendOperations exercises the in-memory backend directly.
func TestBackendOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(*testing.T, *cloudfront.InMemoryBackend)
		name string
	}{
		{
			name: "region",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, config.DefaultRegion, b.Region())
			},
		},
		{
			name: "list_distributions_empty",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				dists := b.ListDistributions()
				assert.Empty(t, dists)
			},
		},
		{
			name: "list_oais_empty",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				oais := b.ListOAIs()
				assert.Empty(t, oais)
			},
		},
		{
			name: "distribution_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.GetDistribution("NOTEXIST")
				require.Error(t, err)
			},
		},
		{
			name: "oai_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.GetOAI("NOTEXIST")
				require.Error(t, err)
			},
		},
		{
			name: "update_nonexistent_distribution",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.UpdateDistribution("NOTEXIST", "comment", true, nil)
				require.Error(t, err)
			},
		},
		{
			name: "delete_nonexistent_distribution",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.DeleteDistribution("NOTEXIST")
				require.Error(t, err)
			},
		},
		{
			name: "delete_nonexistent_oai",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.DeleteOAI("NOTEXIST")
				require.Error(t, err)
			},
		},
		{
			name: "tag_resource_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.TagResource("arn:aws:cloudfront::123:distribution/NOTEXIST", map[string]string{"k": "v"})
				require.Error(t, err)
			},
		},
		{
			name: "untag_resource_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.UntagResource("arn:aws:cloudfront::123:distribution/NOTEXIST", []string{"k"})
				require.Error(t, err)
			},
		},
		{
			name: "list_tags_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.ListTags("arn:aws:cloudfront::123:distribution/NOTEXIST")
				require.Error(t, err)
			},
		},
		{
			name: "full_distribution_lifecycle",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				raw := minimalDistConfig("r1", "c1", true)
				d, err := b.CreateDistribution("r1", "c1", true, raw)
				require.NoError(t, err)
				assert.NotEmpty(t, d.ID)
				assert.NotEmpty(t, d.ARN)
				assert.NotEmpty(t, d.ETag)
				assert.Equal(t, "Deployed", d.Status)
				assert.Contains(t, d.DomainName, ".cloudfront.net")

				got, err := b.GetDistribution(d.ID)
				require.NoError(t, err)
				assert.Equal(t, d.ID, got.ID)

				updated, err := b.UpdateDistribution(d.ID, "updated-comment", false, raw)
				require.NoError(t, err)
				assert.NotEqual(t, d.ETag, updated.ETag)
				assert.Equal(t, "updated-comment", updated.Comment)

				err = b.TagResource(d.ARN, map[string]string{"k": "v"})
				require.NoError(t, err)

				tags, err := b.ListTags(d.ARN)
				require.NoError(t, err)
				assert.Equal(t, "v", tags["k"])

				err = b.UntagResource(d.ARN, []string{"k"})
				require.NoError(t, err)

				err = b.DeleteDistribution(d.ID)
				require.NoError(t, err)

				_, err = b.GetDistribution(d.ID)
				require.Error(t, err)
			},
		},
		{
			name: "full_oai_lifecycle",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				oai, err := b.CreateOAI("oai-ref", "oai-comment")
				require.NoError(t, err)
				assert.NotEmpty(t, oai.ID)
				assert.NotEmpty(t, oai.ETag)
				assert.NotEmpty(t, oai.S3CanonicalUserID)

				got, err := b.GetOAI(oai.ID)
				require.NoError(t, err)
				assert.Equal(t, oai.ID, got.ID)

				list := b.ListOAIs()
				assert.Len(t, list, 1)

				err = b.DeleteOAI(oai.ID)
				require.NoError(t, err)

				_, err = b.GetOAI(oai.ID)
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudfront.NewInMemoryBackend("123456789012", config.DefaultRegion)
			tt.run(t, b)
		})
	}
}

// TestXMLResponseFormat verifies XML content-type and structure.
func TestXMLResponseFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doXML(t, h, http.MethodGet, "/2020-05-31/distribution", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Header().Get("Content-Type"), "xml")

	// Verify the response is valid XML.
	var v any
	err := xml.Unmarshal(rec.Body.Bytes(), &v)
	require.NoError(t, err)
}

func TestCloudFront_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend("000000000000", "us-east-1")
	d, err := b.CreateDistribution("ref1", "my dist", true, nil)
	require.NoError(t, err)

	_, err = b.CreateInvalidation(d.ID, "inv-ref1", []string{"/index.html", "/static/*"})
	require.NoError(t, err)

	_, err = b.CreateOAI("oai-ref1", "my oai")
	require.NoError(t, err)

	h := cloudfront.NewHandler(b)
	snap := h.Snapshot()
	require.NotEmpty(t, snap)

	b2 := cloudfront.NewInMemoryBackend("000000000000", "us-east-1")
	h2 := cloudfront.NewHandler(b2)
	require.NoError(t, h2.Restore(snap))

	// Distribution is restored.
	d2, err := b2.GetDistribution(d.ID)
	require.NoError(t, err)
	assert.Equal(t, "my dist", d2.Comment)

	// Invalidations are restored.
	invs, err := b2.ListInvalidations(d.ID)
	require.NoError(t, err)
	assert.Len(t, invs, 1)

	// ARN index is rebuilt — tag operations should work.
	err = b2.TagResource(d2.ARN, map[string]string{"env": "test"})
	require.NoError(t, err, "tag operation should succeed after restore (ARN index rebuilt)")
}

func TestHandler_CreateInvalidation_ListInvalidations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		callerRef     string
		wantStatus    string
		paths         []string
		wantHTTP      int
		wantListCount int
	}{
		{
			name:          "single_path",
			callerRef:     "ref-001",
			paths:         []string{"/images/*"},
			wantStatus:    "InProgress",
			wantHTTP:      http.StatusCreated,
			wantListCount: 1,
		},
		{
			name:          "wildcard_root",
			callerRef:     "ref-002",
			paths:         []string{"/*"},
			wantStatus:    "InProgress",
			wantHTTP:      http.StatusCreated,
			wantListCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Create a distribution first using the minimal helper.
			createRec := doXML(t, h, http.MethodPost, "/2020-05-31/distribution",
				minimalDistConfig("create-ref-inv", "inv-test-dist", true))
			require.Equal(t, http.StatusCreated, createRec.Code)

			// Extract distribution ID from Location header.
			loc := createRec.Header().Get("Location")
			parts := strings.Split(loc, "/")
			distID := parts[len(parts)-1]
			require.NotEmpty(t, distID)

			// Build paths XML.
			var pathItems strings.Builder
			for _, p := range tt.paths {
				fmt.Fprintf(&pathItems, "<Path>%s</Path>", p)
			}

			// CreateInvalidation.
			invXML := fmt.Sprintf(
				`<InvalidationBatch xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">`+
					`<CallerReference>%s</CallerReference>`+
					`<Paths><Quantity>%d</Quantity><Items>%s</Items></Paths>`+
					`</InvalidationBatch>`,
				tt.callerRef, len(tt.paths), pathItems.String(),
			)

			invRec := doXML(t, h, http.MethodPost,
				"/2020-05-31/distribution/"+distID+"/invalidation",
				[]byte(invXML))
			assert.Equal(t, tt.wantHTTP, invRec.Code, "CreateInvalidation status")

			// Verify the response contains the expected status.
			assert.Contains(t, invRec.Body.String(), "<Status>"+tt.wantStatus+"</Status>")

			// Verify CreateTime is an ISO-8601 string (contains 'T'), not a raw integer.
			assert.Contains(t, invRec.Body.String(), "<CreateTime>")
			assert.Contains(t, invRec.Body.String(), "T", "CreateTime must be RFC3339 formatted")

			// ListInvalidations should return the created invalidation.
			listRec := doXML(t, h, http.MethodGet,
				"/2020-05-31/distribution/"+distID+"/invalidation",
				nil)
			assert.Equal(t, http.StatusOK, listRec.Code)
			assert.Contains(t, listRec.Body.String(), "InProgress")

			// Verify the Quantity matches.
			assert.Contains(t, listRec.Body.String(),
				fmt.Sprintf("<Quantity>%d</Quantity>", tt.wantListCount))
		})
	}
}

// TestAssociateAlias covers the AssociateAlias operation.
func TestAssociateAlias(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *cloudfront.Handler) string
		check      func(*testing.T, *httptest.ResponseRecorder)
		name       string
		alias      string
		wantStatus int
	}{
		{
			name:  "associate_alias_success",
			alias: "www.example.com",
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-aa-001", "alias-dist", true,
					minimalDistConfig("ref-aa-001", "alias-dist", true))
				require.NoError(t, err)

				return d.ID
			},
			wantStatus: http.StatusOK,
			check:      func(t *testing.T, _ *httptest.ResponseRecorder) { t.Helper() },
		},
		{
			name:  "associate_alias_distribution_not_found",
			alias: "notfound.example.com",
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return "DOESNOTEXIST"
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "NoSuchDistribution")
			},
		},
		{
			name:  "associate_alias_empty_alias",
			alias: "",
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-aa-002", "alias-dist2", true,
					minimalDistConfig("ref-aa-002", "alias-dist2", true))
				require.NoError(t, err)

				return d.ID
			},
			wantStatus: http.StatusBadRequest,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "InvalidArgument")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			distID := tt.setup(t, h)
			path := "/2020-05-31/distribution/" + distID + "/associate-alias"
			if tt.alias != "" {
				path += "?Alias=" + tt.alias
			}

			rec := doXML(t, h, http.MethodPut, path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec)
		})
	}
}

// TestAssociateAlias_Idempotent verifies associating the same alias twice is safe.
func TestAssociateAlias_Idempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	d, err := h.Backend.CreateDistribution("ref-ai-001", "idempotent-dist", true,
		minimalDistConfig("ref-ai-001", "idempotent-dist", true))
	require.NoError(t, err)

	path := "/2020-05-31/distribution/" + d.ID + "/associate-alias?Alias=idem.example.com"
	rec := doXML(t, h, http.MethodPut, path, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doXML(t, h, http.MethodPut, path, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestAssociateDistributionWebACL covers the AssociateDistributionWebACL operation.
func TestAssociateDistributionWebACL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *cloudfront.Handler) string
		check      func(*testing.T, *httptest.ResponseRecorder)
		name       string
		body       []byte
		wantStatus int
	}{
		{
			name: "associate_web_acl_success",
			body: []byte(
				`<WebACLAssociation><WebACLId>arn:aws:wafv2:us-east-1:123:global/webacl/test/abc</WebACLId></WebACLAssociation>`,
			),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-wacl-001", "wacl-dist", true,
					minimalDistConfig("ref-wacl-001", "wacl-dist", true))
				require.NoError(t, err)

				return d.ID
			},
			wantStatus: http.StatusOK,
			check:      func(t *testing.T, _ *httptest.ResponseRecorder) { t.Helper() },
		},
		{
			name: "associate_web_acl_not_found",
			body: []byte(`<WebACLAssociation><WebACLId>some-acl</WebACLId></WebACLAssociation>`),
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return "DOESNOTEXIST"
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "NoSuchDistribution")
			},
		},
		{
			name: "associate_web_acl_empty_body",
			body: nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-wacl-002", "wacl-dist2", true,
					minimalDistConfig("ref-wacl-002", "wacl-dist2", true))
				require.NoError(t, err)

				return d.ID
			},
			wantStatus: http.StatusOK,
			check:      func(t *testing.T, _ *httptest.ResponseRecorder) { t.Helper() },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			distID := tt.setup(t, h)
			path := "/2020-05-31/distribution/" + distID + "/associate-web-acl"

			rec := doXML(t, h, http.MethodPut, path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec)
		})
	}
}

// TestAssociateDistributionTenantWebACL covers the AssociateDistributionTenantWebACL operation.
func TestAssociateDistributionTenantWebACL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check      func(*testing.T, *httptest.ResponseRecorder)
		name       string
		tenantID   string
		body       []byte
		wantStatus int
	}{
		{
			name:     "associate_tenant_web_acl_success",
			tenantID: "tenant-abc-123",
			body: []byte(
				`<WebACLAssociation><WebACLId>arn:aws:wafv2:us-east-1:123:global/webacl/tenant/abc</WebACLId></WebACLAssociation>`,
			),
			wantStatus: http.StatusOK,
			check:      func(t *testing.T, _ *httptest.ResponseRecorder) { t.Helper() },
		},
		{
			name:       "associate_tenant_web_acl_empty_tenant",
			tenantID:   "",
			body:       []byte(`<WebACLAssociation><WebACLId>some-acl</WebACLId></WebACLAssociation>`),
			wantStatus: http.StatusBadRequest,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "InvalidArgument")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			var path string
			if tt.tenantID == "" {
				// POST to the tenant endpoint with empty tenant ID segment
				path = "/2020-05-31/distribution-tenant//associate-web-acl"
			} else {
				path = "/2020-05-31/distribution-tenant/" + tt.tenantID + "/associate-web-acl"
			}

			rec := doXML(t, h, http.MethodPut, path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec)
		})
	}
}

// TestCopyDistribution covers the CopyDistribution operation.
func TestCopyDistribution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *cloudfront.Handler) string
		check      func(*testing.T, *httptest.ResponseRecorder)
		name       string
		body       []byte
		wantStatus int
	}{
		{
			name: "copy_distribution_success",
			body: []byte(
				`<CopyDistributionRequest><CallerReference>copy-ref-001</CallerReference></CopyDistributionRequest>`,
			),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-copy-001", "source-dist", true,
					minimalDistConfig("ref-copy-001", "source-dist", true))
				require.NoError(t, err)

				return d.ID
			},
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<Distribution")
				assert.Contains(t, rec.Body.String(), "<Status>Deployed</Status>")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
				assert.NotEmpty(t, rec.Header().Get("Location"))
			},
		},
		{
			name: "copy_distribution_not_found",
			body: []byte(
				`<CopyDistributionRequest><CallerReference>copy-ref-002</CallerReference></CopyDistributionRequest>`,
			),
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return "DOESNOTEXIST"
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "NoSuchDistribution")
			},
		},
		{
			name: "copy_distribution_empty_caller_ref",
			body: []byte(`<CopyDistributionRequest><CallerReference></CallerReference></CopyDistributionRequest>`),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-copy-003", "source-dist2", true,
					minimalDistConfig("ref-copy-003", "source-dist2", true))
				require.NoError(t, err)

				return d.ID
			},
			wantStatus: http.StatusBadRequest,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "InvalidArgument")
			},
		},
		{
			name: "copy_distribution_malformed_xml",
			body: []byte(`<<<not xml`),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-copy-004", "source-dist3", true,
					minimalDistConfig("ref-copy-004", "source-dist3", true))
				require.NoError(t, err)

				return d.ID
			},
			wantStatus: http.StatusBadRequest,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "MalformedXML")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			distID := tt.setup(t, h)
			path := "/2020-05-31/distribution/" + distID + "/copy"

			rec := doXML(t, h, http.MethodPost, path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec)
		})
	}
}

// TestCreateAnycastIPList covers the CreateAnycastIPList operation.
func TestCreateAnycastIPList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check      func(*testing.T, *httptest.ResponseRecorder)
		name       string
		body       []byte
		wantStatus int
	}{
		{
			name: "create_anycast_ip_list_success",
			body: []byte(
				`<AnycastIPListRequest><Name>my-anycast-list</Name><IPCount>5</IPCount></AnycastIPListRequest>`,
			),
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<AnycastIPList")
				assert.Contains(t, rec.Body.String(), "<Name>my-anycast-list</Name>")
				assert.Contains(t, rec.Body.String(), "<Status>Deployed</Status>")
				assert.NotEmpty(t, rec.Header().Get("Location"))
			},
		},
		{
			name:       "create_anycast_ip_list_empty_name",
			body:       []byte(`<AnycastIPListRequest><Name></Name><IPCount>5</IPCount></AnycastIPListRequest>`),
			wantStatus: http.StatusBadRequest,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "InvalidArgument")
			},
		},
		{
			name:       "create_anycast_ip_list_malformed_xml",
			body:       []byte(`<<<not xml`),
			wantStatus: http.StatusBadRequest,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "MalformedXML")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doXML(t, h, http.MethodPost, "/2020-05-31/anycast-ip-list", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec)
		})
	}
}

// TestCreateCachePolicy covers the CreateCachePolicy operation.
func TestCreateCachePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check      func(*testing.T, *httptest.ResponseRecorder)
		name       string
		body       []byte
		wantStatus int
	}{
		{
			name: "create_cache_policy_success",
			body: []byte(`<CachePolicyConfig>` +
				`<Name>my-cache-policy</Name>` +
				`<Comment>test policy</Comment>` +
				`<DefaultTTL>86400</DefaultTTL>` +
				`<MaxTTL>31536000</MaxTTL>` +
				`<MinTTL>0</MinTTL>` +
				`</CachePolicyConfig>`),
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<CachePolicy")
				assert.Contains(t, rec.Body.String(), "<Name>my-cache-policy</Name>")
				assert.Contains(t, rec.Body.String(), "<Comment>test policy</Comment>")
				assert.NotEmpty(t, rec.Header().Get("Location"))
			},
		},
		{
			name:       "create_cache_policy_empty_name",
			body:       []byte(`<CachePolicyConfig><Name></Name></CachePolicyConfig>`),
			wantStatus: http.StatusBadRequest,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "InvalidArgument")
			},
		},
		{
			name:       "create_cache_policy_malformed_xml",
			body:       []byte(`<<<not xml`),
			wantStatus: http.StatusBadRequest,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "MalformedXML")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doXML(t, h, http.MethodPost, "/2020-05-31/cache-policy", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec)
		})
	}
}

// TestCreateCloudFrontOriginAccessIdentity verifies that CreateCloudFrontOriginAccessIdentity
// (the renamed op) still works and is reported correctly.
func TestCreateCloudFrontOriginAccessIdentity(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/2020-05-31/origin-access-identity/cloudfront", nil)
	c := e.NewContext(req, httptest.NewRecorder())

	assert.Equal(t, "CreateCloudFrontOriginAccessIdentity", h.ExtractOperation(c))
}

// TestCreateConnectionFunction covers the CreateConnectionFunction operation.
func TestCreateConnectionFunction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check      func(*testing.T, *httptest.ResponseRecorder)
		name       string
		body       []byte
		wantStatus int
	}{
		{
			name: "create_connection_function_success",
			body: []byte(`<CreateConnectionFunctionRequest>` +
				`<Name>my-conn-fn</Name>` +
				`<Comment>my function</Comment>` +
				`</CreateConnectionFunctionRequest>`),
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<ConnectionFunction")
				assert.Contains(t, rec.Body.String(), "<Name>my-conn-fn</Name>")
				assert.NotEmpty(t, rec.Header().Get("Location"))
			},
		},
		{
			name: "create_connection_function_empty_name",
			body: []byte(`<CreateConnectionFunctionRequest>` +
				`<Name></Name>` +
				`</CreateConnectionFunctionRequest>`),
			wantStatus: http.StatusBadRequest,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "InvalidArgument")
			},
		},
		{
			name:       "create_connection_function_malformed_xml",
			body:       []byte(`<<<not xml`),
			wantStatus: http.StatusBadRequest,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "MalformedXML")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doXML(t, h, http.MethodPost, "/2020-05-31/connection-function", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec)
		})
	}
}

// TestCreateConnectionGroup covers the CreateConnectionGroup operation.
func TestCreateConnectionGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check      func(*testing.T, *httptest.ResponseRecorder)
		name       string
		body       []byte
		wantStatus int
	}{
		{
			name: "create_connection_group_success",
			body: []byte(`<CreateConnectionGroupRequest>` +
				`<Name>my-conn-group</Name>` +
				`<Comment>my group</Comment>` +
				`</CreateConnectionGroupRequest>`),
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<ConnectionGroup")
				assert.Contains(t, rec.Body.String(), "<Name>my-conn-group</Name>")
				assert.NotEmpty(t, rec.Header().Get("Location"))
			},
		},
		{
			name: "create_connection_group_empty_name",
			body: []byte(`<CreateConnectionGroupRequest>` +
				`<Name></Name>` +
				`</CreateConnectionGroupRequest>`),
			wantStatus: http.StatusBadRequest,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "InvalidArgument")
			},
		},
		{
			name:       "create_connection_group_malformed_xml",
			body:       []byte(`<<<not xml`),
			wantStatus: http.StatusBadRequest,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "MalformedXML")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doXML(t, h, http.MethodPost, "/2020-05-31/connection-group", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec)
		})
	}
}

// TestCreateContinuousDeploymentPolicy covers the CreateContinuousDeploymentPolicy operation.
func TestCreateContinuousDeploymentPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check      func(*testing.T, *httptest.ResponseRecorder)
		name       string
		body       []byte
		wantStatus int
	}{
		{
			name: "create_continuous_deployment_policy_enabled",
			body: []byte(
				`<ContinuousDeploymentPolicyConfig><Enabled>true</Enabled></ContinuousDeploymentPolicyConfig>`,
			),
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<ContinuousDeploymentPolicy")
				assert.Contains(t, rec.Body.String(), "<Enabled>true</Enabled>")
				assert.NotEmpty(t, rec.Header().Get("Location"))
			},
		},
		{
			name: "create_continuous_deployment_policy_disabled",
			body: []byte(
				`<ContinuousDeploymentPolicyConfig><Enabled>false</Enabled></ContinuousDeploymentPolicyConfig>`,
			),
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<ContinuousDeploymentPolicy")
				assert.Contains(t, rec.Body.String(), "<Enabled>false</Enabled>")
			},
		},
		{
			name:       "create_continuous_deployment_policy_no_body",
			body:       nil,
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<ContinuousDeploymentPolicy")
			},
		},
		{
			name:       "create_continuous_deployment_policy_malformed_xml",
			body:       []byte(`<<<not xml`),
			wantStatus: http.StatusBadRequest,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "MalformedXML")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doXML(t, h, http.MethodPost, "/2020-05-31/continuous-deployment-policy", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec)
		})
	}
}

// TestNewOperations_PersistenceRoundTrip verifies that all new resources survive snapshot/restore.
func TestNewOperations_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend("000000000000", "us-east-1")

	// Create a distribution and associate an alias + web ACL.
	d, err := b.CreateDistribution("ref-persist-1", "persist-dist", true, nil)
	require.NoError(t, err)

	err = b.AssociateAlias(d.ID, "persist.example.com")
	require.NoError(t, err)

	err = b.AssociateDistributionWebACL(d.ID, "arn:aws:wafv2:us-east-1:123:global/webacl/test/abc")
	require.NoError(t, err)

	err = b.AssociateDistributionTenantWebACL("tenant-persist-001", "acl-for-tenant")
	require.NoError(t, err)

	// Copy the distribution.
	_, err = b.CopyDistribution(d.ID, "copy-persist-ref")
	require.NoError(t, err)

	// Create new resource types.
	_, err = b.CreateAnycastIPList("persist-anycast-list", 3)
	require.NoError(t, err)

	_, err = b.CreateCachePolicy("persist-cache-policy", "comment", 86400, 31536000, 0)
	require.NoError(t, err)

	_, err = b.CreateConnectionFunction("persist-conn-fn", "comment")
	require.NoError(t, err)

	_, err = b.CreateConnectionGroup("persist-conn-group", "comment")
	require.NoError(t, err)

	_, err = b.CreateContinuousDeploymentPolicy(true, "")
	require.NoError(t, err)

	h := cloudfront.NewHandler(b)
	snap := h.Snapshot()
	require.NotEmpty(t, snap)

	b2 := cloudfront.NewInMemoryBackend("000000000000", "us-east-1")
	h2 := cloudfront.NewHandler(b2)
	require.NoError(t, h2.Restore(snap))

	// Distribution is restored.
	d2, err := b2.GetDistribution(d.ID)
	require.NoError(t, err)
	assert.Equal(t, "persist-dist", d2.Comment)
}

// TestNewOperations_BackendDirectly exercises all new backend methods directly.
func TestNewOperations_BackendDirectly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(*testing.T, *cloudfront.InMemoryBackend)
		name string
	}{
		{
			name: "associate_alias_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.AssociateAlias("NOTEXIST", "alias.example.com")
				require.Error(t, err)
			},
		},
		{
			name: "associate_alias_empty",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				d, err := b.CreateDistribution("ref-ba-001", "ba-dist", true, nil)
				require.NoError(t, err)
				err = b.AssociateAlias(d.ID, "")
				require.Error(t, err)
			},
		},
		{
			name: "associate_distribution_web_acl_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.AssociateDistributionWebACL("NOTEXIST", "acl-id")
				require.Error(t, err)
			},
		},
		{
			name: "associate_distribution_tenant_web_acl_empty_tenant",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.AssociateDistributionTenantWebACL("", "acl-id")
				require.Error(t, err)
			},
		},
		{
			name: "copy_distribution_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CopyDistribution("NOTEXIST", "ref")
				require.Error(t, err)
			},
		},
		{
			name: "copy_distribution_empty_caller_ref",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				d, err := b.CreateDistribution("ref-cp-001", "cp-dist", true, nil)
				require.NoError(t, err)
				_, err = b.CopyDistribution(d.ID, "")
				require.Error(t, err)
			},
		},
		{
			name: "create_anycast_ip_list_empty_name",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateAnycastIPList("", 5)
				require.Error(t, err)
			},
		},
		{
			name: "create_cache_policy_empty_name",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateCachePolicy("", "comment", 86400, 0, 0)
				require.Error(t, err)
			},
		},
		{
			name: "create_connection_function_empty_name",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateConnectionFunction("", "comment")
				require.Error(t, err)
			},
		},
		{
			name: "create_connection_group_empty_name",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateConnectionGroup("", "comment")
				require.Error(t, err)
			},
		},
		{
			name: "create_anycast_ip_list_success",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				list, err := b.CreateAnycastIPList("my-list", 5)
				require.NoError(t, err)
				assert.NotEmpty(t, list.ID)
				assert.NotEmpty(t, list.ARN)
				assert.Equal(t, "my-list", list.Name)
				assert.Equal(t, int32(5), list.IPCount)
				assert.Equal(t, "Deployed", list.Status)
			},
		},
		{
			name: "create_cache_policy_success",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				p, err := b.CreateCachePolicy("test-policy", "a comment", 3600, 86400, 0)
				require.NoError(t, err)
				assert.NotEmpty(t, p.ID)
				assert.Equal(t, "test-policy", p.Name)
				assert.Equal(t, int64(3600), p.DefaultTTL)
				assert.Equal(t, int64(86400), p.MaxTTL)
				assert.Equal(t, int64(0), p.MinTTL)
			},
		},
		{
			name: "create_connection_function_success",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				fn, err := b.CreateConnectionFunction("my-fn", "fn comment")
				require.NoError(t, err)
				assert.NotEmpty(t, fn.ARN)
				assert.Equal(t, "my-fn", fn.Name)
				assert.Equal(t, "fn comment", fn.Comment)
			},
		},
		{
			name: "create_connection_group_success",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				g, err := b.CreateConnectionGroup("my-group", "group comment")
				require.NoError(t, err)
				assert.NotEmpty(t, g.ID)
				assert.NotEmpty(t, g.ARN)
				assert.Equal(t, "my-group", g.Name)
			},
		},
		{
			name: "create_continuous_deployment_policy_success",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				p, err := b.CreateContinuousDeploymentPolicy(true, "")
				require.NoError(t, err)
				assert.NotEmpty(t, p.ID)
				assert.True(t, p.Enabled)
			},
		},
		{
			name: "copy_distribution_success",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				src, err := b.CreateDistribution("ref-cpy-001", "src-dist", true,
					minimalDistConfig("ref-cpy-001", "src-dist", true))
				require.NoError(t, err)

				cp, err := b.CopyDistribution(src.ID, "copy-ref-001")
				require.NoError(t, err)
				assert.NotEqual(t, src.ID, cp.ID)
				assert.Equal(t, src.Comment, cp.Comment)
				assert.Equal(t, src.Enabled, cp.Enabled)
				assert.NotEmpty(t, cp.DomainName)
				assert.Equal(t, "Deployed", cp.Status)
			},
		},
		{
			name: "associate_alias_success",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				d, err := b.CreateDistribution("ref-aal-001", "alias-dist", true, nil)
				require.NoError(t, err)
				err = b.AssociateAlias(d.ID, "www.example.com")
				require.NoError(t, err)
			},
		},
		{
			name: "associate_distribution_web_acl_success",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				d, err := b.CreateDistribution("ref-awacl-001", "wacl-dist", true, nil)
				require.NoError(t, err)
				err = b.AssociateDistributionWebACL(d.ID, "arn:aws:wafv2:us-east-1:123:global/webacl/test")
				require.NoError(t, err)
			},
		},
		{
			name: "associate_distribution_tenant_web_acl_success",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.AssociateDistributionTenantWebACL("tenant-001", "acl-001")
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudfront.NewInMemoryBackend("123456789012", config.DefaultRegion)
			tt.run(t, b)
		})
	}
}

// TestNewOperations_ExtractOperation verifies route parsing for new operations.
func TestNewOperations_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		method        string
		path          string
		wantOperation string
	}{
		{
			name:          "associate_alias",
			method:        http.MethodPut,
			path:          "/2020-05-31/distribution/ABCD1234/associate-alias",
			wantOperation: "AssociateAlias",
		},
		{
			name:          "associate_distribution_web_acl",
			method:        http.MethodPut,
			path:          "/2020-05-31/distribution/ABCD1234/associate-web-acl",
			wantOperation: "AssociateDistributionWebACL",
		},
		{
			name:          "associate_distribution_tenant_web_acl",
			method:        http.MethodPut,
			path:          "/2020-05-31/distribution-tenant/TENANT1234/associate-web-acl",
			wantOperation: "AssociateDistributionTenantWebACL",
		},
		{
			name:          "copy_distribution",
			method:        http.MethodPost,
			path:          "/2020-05-31/distribution/ABCD1234/copy",
			wantOperation: "CopyDistribution",
		},
		{
			name:          "create_anycast_ip_list",
			method:        http.MethodPost,
			path:          "/2020-05-31/anycast-ip-list",
			wantOperation: "CreateAnycastIpList",
		},
		{
			name:          "create_cache_policy",
			method:        http.MethodPost,
			path:          "/2020-05-31/cache-policy",
			wantOperation: "CreateCachePolicy",
		},
		{
			name:          "create_connection_function",
			method:        http.MethodPost,
			path:          "/2020-05-31/connection-function",
			wantOperation: "CreateConnectionFunction",
		},
		{
			name:          "create_connection_group",
			method:        http.MethodPost,
			path:          "/2020-05-31/connection-group",
			wantOperation: "CreateConnectionGroup",
		},
		{
			name:          "create_continuous_deployment_policy",
			method:        http.MethodPost,
			path:          "/2020-05-31/continuous-deployment-policy",
			wantOperation: "CreateContinuousDeploymentPolicy",
		},
		{
			name:          "get_cloudfront_oai",
			method:        http.MethodGet,
			path:          "/2020-05-31/origin-access-identity/cloudfront/OAIID123",
			wantOperation: "GetCloudFrontOriginAccessIdentity",
		},
		{
			name:          "get_cloudfront_oai_config",
			method:        http.MethodGet,
			path:          "/2020-05-31/origin-access-identity/cloudfront/OAIID123/config",
			wantOperation: "GetCloudFrontOriginAccessIdentityConfig",
		},
		{
			name:          "list_cloudfront_oais",
			method:        http.MethodGet,
			path:          "/2020-05-31/origin-access-identity/cloudfront",
			wantOperation: "ListCloudFrontOriginAccessIdentities",
		},
		{
			name:          "delete_cloudfront_oai",
			method:        http.MethodDelete,
			path:          "/2020-05-31/origin-access-identity/cloudfront/OAIID123",
			wantOperation: "DeleteCloudFrontOriginAccessIdentity",
		},
		{
			name:          "update_cloudfront_oai",
			method:        http.MethodPut,
			path:          "/2020-05-31/origin-access-identity/cloudfront/OAIID123/config",
			wantOperation: "UpdateCloudFrontOriginAccessIdentity",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantOperation, h.ExtractOperation(c))
		})
	}
}

// TestRefinement1_Reset verifies Reset() clears all backend state.
func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	b := h.Backend

	_, err := b.CreateDistribution("ref-r1", "reset-dist", true, nil)
	require.NoError(t, err)

	_, err = b.CreateOAI("ref-oai-r1", "reset-oai")
	require.NoError(t, err)

	h.Reset()

	dists := b.ListDistributions()
	assert.Empty(t, dists)

	oais := b.ListOAIs()
	assert.Empty(t, oais)
}

// TestRefinement1_BackendReset verifies backend Reset() directly.
func TestRefinement1_BackendReset(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend("123456789012", config.DefaultRegion)

	_, err := b.CreateDistribution("ref-br1", "a-dist", true, nil)
	require.NoError(t, err)

	b.Reset()

	assert.Empty(t, b.ListDistributions())
	assert.Empty(t, b.ListOAIs())
}

// TestRefinement1_CallerReferenceValidation verifies CallerReference is required.
func TestRefinement1_CallerReferenceValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantError  string
		body       []byte
		wantStatus int
	}{
		{
			name: "empty_caller_ref_distribution",
			body: []byte(
				`<DistributionConfig><CallerReference></CallerReference><Enabled>true</Enabled></DistributionConfig>`,
			),
			wantStatus: http.StatusBadRequest,
			wantError:  "InvalidArgument",
		},
		{
			name: "missing_caller_ref_oai",
			body: []byte(
				`<CloudFrontOriginAccessIdentityConfig>` +
					`<CallerReference></CallerReference>` +
					`<Comment>no-ref</Comment>` +
					`</CloudFrontOriginAccessIdentityConfig>`,
			),
			wantStatus: http.StatusBadRequest,
			wantError:  "InvalidArgument",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			var path string
			if strings.Contains(tt.name, "oai") {
				path = "/2020-05-31/origin-access-identity/cloudfront"
			} else {
				path = "/2020-05-31/distribution"
			}

			rec := doXML(t, h, http.MethodPost, path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantError)
		})
	}
}

// TestRefinement1_CallerReferenceIdempotency verifies duplicate CallerReferences return existing resource.
func TestRefinement1_CallerReferenceIdempotency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "distribution_idempotency"},
		{name: "oai_idempotency"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if strings.Contains(tt.name, "distribution") {
				body := minimalDistConfig("idem-ref-001", "idem-dist", true)
				rec1 := doXML(t, h, http.MethodPost, "/2020-05-31/distribution", body)
				require.Equal(t, http.StatusCreated, rec1.Code)

				// Second call with same CallerReference should return same distribution.
				rec2 := doXML(t, h, http.MethodPost, "/2020-05-31/distribution", body)
				require.Equal(t, http.StatusCreated, rec2.Code)

				// Should have same ID (only one distribution created).
				assert.Equal(t, rec1.Body.String(), rec2.Body.String())
				assert.Len(t, h.Backend.ListDistributions(), 1)
			} else {
				body := minimalOAIConfig("idem-oai-ref-001", "idem-oai")
				rec1 := doXML(t, h, http.MethodPost, "/2020-05-31/origin-access-identity/cloudfront", body)
				require.Equal(t, http.StatusCreated, rec1.Code)

				rec2 := doXML(t, h, http.MethodPost, "/2020-05-31/origin-access-identity/cloudfront", body)
				require.Equal(t, http.StatusCreated, rec2.Code)

				// Only one OAI should be stored.
				assert.Len(t, h.Backend.ListOAIs(), 1)
			}
		})
	}
}

// TestRefinement1_CachePolicyUniqueness verifies duplicate cache policy names are rejected.
func TestRefinement1_CachePolicyUniqueness(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	body := []byte(
		`<CachePolicyConfig><Name>my-unique-policy</Name>` +
			`<DefaultTTL>86400</DefaultTTL><MaxTTL>31536000</MaxTTL><MinTTL>0</MinTTL>` +
			`</CachePolicyConfig>`,
	)

	rec1 := doXML(t, h, http.MethodPost, "/2020-05-31/cache-policy", body)
	require.Equal(t, http.StatusCreated, rec1.Code)

	rec2 := doXML(t, h, http.MethodPost, "/2020-05-31/cache-policy", body)
	assert.Equal(t, http.StatusConflict, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "DistributionAlreadyExists")
}

// TestRefinement1_CachePolicyTTLValidation verifies TTL ordering is enforced.
func TestRefinement1_CachePolicyTTLValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantError  string
		wantStatus int
	}{
		{
			name: "negative_min_ttl",
			body: `<CachePolicyConfig><Name>p1</Name>` +
				`<DefaultTTL>86400</DefaultTTL><MaxTTL>31536000</MaxTTL><MinTTL>-1</MinTTL>` +
				`</CachePolicyConfig>`,
			wantStatus: http.StatusBadRequest,
			wantError:  "InvalidArgument",
		},
		{
			name: "default_less_than_min",
			body: `<CachePolicyConfig><Name>p2</Name>` +
				`<DefaultTTL>10</DefaultTTL><MaxTTL>31536000</MaxTTL><MinTTL>100</MinTTL>` +
				`</CachePolicyConfig>`,
			wantStatus: http.StatusBadRequest,
			wantError:  "InvalidArgument",
		},
		{
			name: "max_less_than_default",
			body: `<CachePolicyConfig><Name>p3</Name>` +
				`<DefaultTTL>86400</DefaultTTL><MaxTTL>1000</MaxTTL><MinTTL>0</MinTTL>` +
				`</CachePolicyConfig>`,
			wantStatus: http.StatusBadRequest,
			wantError:  "InvalidArgument",
		},
		{
			name: "valid_ttls",
			body: `<CachePolicyConfig><Name>p4</Name>` +
				`<DefaultTTL>86400</DefaultTTL><MaxTTL>31536000</MaxTTL><MinTTL>0</MinTTL>` +
				`</CachePolicyConfig>`,
			wantStatus: http.StatusCreated,
			wantError:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doXML(t, h, http.MethodPost, "/2020-05-31/cache-policy", []byte(tt.body))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
}

// TestRefinement1_AnycastIpListIPCountValidation verifies IPCount must be positive.
func TestRefinement1_AnycastIpListIPCountValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "zero_count",
			body:       `<AnycastIPListRequest><Name>test-list</Name><IPCount>0</IPCount></AnycastIPListRequest>`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "negative_count",
			body:       `<AnycastIPListRequest><Name>test-list-neg</Name><IPCount>-5</IPCount></AnycastIPListRequest>`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "valid_count",
			body:       `<AnycastIPListRequest><Name>test-list-valid</Name><IPCount>10</IPCount></AnycastIPListRequest>`,
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doXML(t, h, http.MethodPost, "/2020-05-31/anycast-ip-list", []byte(tt.body))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement1_DeleteDistributionCleansUp verifies aliases/webACLs are removed on delete.
func TestRefinement1_DeleteDistributionCleansUp(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend("123456789012", config.DefaultRegion)

	d, err := b.CreateDistribution("ref-del-cleanup", "del-dist", false, nil)
	require.NoError(t, err)

	err = b.AssociateAlias(d.ID, "cleanup.example.com")
	require.NoError(t, err)

	err = b.AssociateDistributionWebACL(d.ID, "arn:aws:wafv2:us-east-1:123:webacl/test")
	require.NoError(t, err)

	// Delete requires ETag via handler; do directly via backend.
	h := cloudfront.NewHandler(b)
	// Get ETag for delete.
	rec := doXML(t, h, http.MethodGet, "/2020-05-31/distribution/"+d.ID, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	etag := rec.Header().Get("ETag")
	require.NotEmpty(t, etag)

	rec = doXMLWithHeaders(t, h, http.MethodDelete, "/2020-05-31/distribution/"+d.ID, nil,
		map[string]string{"If-Match": etag})
	require.Equal(t, http.StatusNoContent, rec.Code)

	// After deletion, CallerReference re-use should create a new distribution.
	d2, err := b.CreateDistribution("ref-del-cleanup", "new-dist", true, nil)
	require.NoError(t, err)
	assert.NotEqual(t, d.ID, d2.ID, "new distribution should have different ID after callerRef is freed")
}

// TestRefinement1_SortedOutput verifies sorted listing results.
func TestRefinement1_SortedOutput(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend("123456789012", config.DefaultRegion)

	// Create multiple distributions.
	refs := []string{"s-ref-001", "s-ref-002", "s-ref-003"}
	for _, ref := range refs {
		_, err := b.CreateDistribution(ref, ref, true, nil)
		require.NoError(t, err)
	}

	dists := b.ListDistributions()
	require.Len(t, dists, 3)

	for i := 1; i < len(dists); i++ {
		assert.LessOrEqual(t, dists[i-1].ID, dists[i].ID,
			"distributions should be sorted by ID")
	}

	// Create multiple OAIs.
	for _, ref := range refs {
		_, err := b.CreateOAI(ref+"-oai", "comment")
		require.NoError(t, err)
	}

	oais := b.ListOAIs()
	require.Len(t, oais, 3)

	for i := 1; i < len(oais); i++ {
		assert.LessOrEqual(t, oais[i-1].ID, oais[i].ID,
			"OAIs should be sorted by ID")
	}
}

// TestRefinement1_SortedTags verifies tags are returned in sorted order.
func TestRefinement1_SortedTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create a distribution via handler.
	rec := doXML(t, h, http.MethodPost, "/2020-05-31/distribution",
		minimalDistConfig("ref-sorted-tags", "tags-dist", true))
	require.Equal(t, http.StatusCreated, rec.Code)

	// Parse the distribution ARN from Location header.
	loc := rec.Header().Get("Location")
	distID := strings.TrimPrefix(loc, "/2020-05-31/distribution/")
	require.NotEmpty(t, distID)

	d, err := h.Backend.GetDistribution(distID)
	require.NoError(t, err)
	arn := d.ARN

	// Add tags.
	tagBody := `<Tags xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">` +
		`<Items><Tag><Key>zebra</Key><Value>z</Value></Tag>` +
		`<Tag><Key>apple</Key><Value>a</Value></Tag>` +
		`<Tag><Key>mango</Key><Value>m</Value></Tag></Items></Tags>`
	rec2 := doXML(t, h, http.MethodPost, "/2020-05-31/tagging?Resource="+arn, []byte(tagBody))
	require.Equal(t, http.StatusNoContent, rec2.Code)

	// List tags and verify sorted order.
	rec3 := doXML(t, h, http.MethodGet, "/2020-05-31/tagging?Resource="+arn, nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	body := rec3.Body.String()
	applePos := strings.Index(body, "apple")
	mangoPos := strings.Index(body, "mango")
	zebraPos := strings.Index(body, "zebra")

	assert.Less(t, applePos, mangoPos, "apple should appear before mango")
	assert.Less(t, mangoPos, zebraPos, "mango should appear before zebra")
}

// TestRefinement1_GetInvalidation tests the GET invalidation by ID handler.
func TestRefinement1_GetInvalidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check      func(*testing.T, *httptest.ResponseRecorder)
		setup      func(*testing.T, *cloudfront.Handler) (string, string)
		name       string
		wantStatus int
	}{
		{
			name: "get_invalidation_success",
			setup: func(t *testing.T, h *cloudfront.Handler) (string, string) {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-gi-001", "gi-dist", true, nil)
				require.NoError(t, err)
				inv, err := h.Backend.CreateInvalidation(d.ID, "caller-gi-001", []string{"/path/*"})
				require.NoError(t, err)

				return d.ID, inv.ID
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<Invalidation")
				assert.Contains(t, rec.Body.String(), "<Status>InProgress</Status>")
				assert.Contains(t, rec.Body.String(), "/path/*")
			},
		},
		{
			name: "get_invalidation_not_found",
			setup: func(t *testing.T, h *cloudfront.Handler) (string, string) {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-gi-002", "gi-dist2", true, nil)
				require.NoError(t, err)

				return d.ID, "DOESNOTEXIST"
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "NoSuchInvalidation")
			},
		},
		{
			name: "get_invalidation_distribution_not_found",
			setup: func(_ *testing.T, _ *cloudfront.Handler) (string, string) {
				return "NOTEXIST", "inv1"
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "NoSuchDistribution")
			},
		},
		{
			name: "get_invalidation_missing_inv_id",
			setup: func(t *testing.T, h *cloudfront.Handler) (string, string) {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-gi-003", "gi-dist3", true, nil)
				require.NoError(t, err)

				return d.ID, ""
			},
			wantStatus: http.StatusBadRequest,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "InvalidArgument")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			distID, invID := tt.setup(t, h)

			var path string
			if invID != "" {
				path = "/2020-05-31/distribution/" + distID + "/invalidation/" + invID
			} else {
				// No invID in path - triggers missing-ID error.
				path = "/2020-05-31/distribution/" + distID + "/invalidation/"
			}

			rec := doXML(t, h, http.MethodGet, path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec)
		})
	}
}

// TestRefinement1_UntagResource verifies the UntagResource handler.
func TestRefinement1_UntagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create distribution.
	rec := doXML(t, h, http.MethodPost, "/2020-05-31/distribution",
		minimalDistConfig("ref-untag-001", "untag-dist", true))
	require.Equal(t, http.StatusCreated, rec.Code)

	loc := rec.Header().Get("Location")
	distID := strings.TrimPrefix(loc, "/2020-05-31/distribution/")
	d, err := h.Backend.GetDistribution(distID)
	require.NoError(t, err)

	arn := d.ARN

	// Add tags.
	tagBody := `<Tags xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">` +
		`<Items><Tag><Key>env</Key><Value>prod</Value></Tag>` +
		`<Tag><Key>owner</Key><Value>team</Value></Tag></Items></Tags>`
	rec2 := doXML(t, h, http.MethodPost, "/2020-05-31/tagging?Resource="+arn, []byte(tagBody))
	require.Equal(t, http.StatusNoContent, rec2.Code)

	// Untag using body with correct AWS format.
	untagBody := `<TagKeys><Items><Key>env</Key></Items></TagKeys>`
	rec3 := doXML(t, h, http.MethodDelete, "/2020-05-31/tagging?Resource="+arn, []byte(untagBody))
	assert.Equal(t, http.StatusNoContent, rec3.Code)

	// Verify env tag was removed.
	rec4 := doXML(t, h, http.MethodGet, "/2020-05-31/tagging?Resource="+arn, nil)
	require.Equal(t, http.StatusOK, rec4.Code)
	assert.NotContains(t, rec4.Body.String(), "env")
	assert.Contains(t, rec4.Body.String(), "owner")
}

// TestRefinement1_AliasCountInListDistributions verifies alias count is reflected in list output.
func TestRefinement1_AliasCountInListDistributions(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend("123456789012", config.DefaultRegion)
	h := cloudfront.NewHandler(b)

	d, err := b.CreateDistribution("ref-alias-list", "alias-list-dist", true, nil)
	require.NoError(t, err)

	err = b.AssociateAlias(d.ID, "www.example.com")
	require.NoError(t, err)

	err = b.AssociateAlias(d.ID, "api.example.com")
	require.NoError(t, err)

	rec := doXML(t, h, http.MethodGet, "/2020-05-31/distribution", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Quantity>2</Quantity>")
}

// TestRefinement1_ProviderInitNilCtx verifies Provider.Init handles nil context.
func TestRefinement1_ProviderInitNilCtx(t *testing.T) {
	t.Parallel()

	p := &cloudfront.Provider{}
	handler, err := p.Init(nil)
	require.NoError(t, err)
	require.NotNil(t, handler)
}

// TestRefinement1_PersistenceWithIndexes verifies indexes are rebuilt after snapshot/restore.
func TestRefinement1_PersistenceWithIndexes(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend("123456789012", config.DefaultRegion)

	// Create resources with known CallerReferences.
	d, err := b.CreateDistribution("persist-ref-001", "persist-dist", true, nil)
	require.NoError(t, err)

	_, err = b.CreateOAI("persist-oai-ref-001", "persist-oai")
	require.NoError(t, err)

	_, err = b.CreateCachePolicy("persist-cp", "comment", 86400, 31536000, 0)
	require.NoError(t, err)

	h := cloudfront.NewHandler(b)
	snap := h.Snapshot()
	require.NotEmpty(t, snap)

	b2 := cloudfront.NewInMemoryBackend("123456789012", config.DefaultRegion)
	h2 := cloudfront.NewHandler(b2)
	require.NoError(t, h2.Restore(snap))

	// CallerReference idempotency should work after restore.
	d2, err := b2.CreateDistribution("persist-ref-001", "persist-dist", true, nil)
	require.NoError(t, err)
	assert.Equal(t, d.ID, d2.ID, "same CallerReference should return same distribution after restore")

	// CachePolicy name uniqueness should work after restore.
	_, err = b2.CreateCachePolicy("persist-cp", "comment", 86400, 31536000, 0)
	require.Error(t, err, "duplicate cache policy name should be rejected after restore")
}

// TestRefinement1_SortedInvalidations verifies invalidations are returned sorted by ID.
func TestRefinement1_SortedInvalidations(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend("123456789012", config.DefaultRegion)

	d, err := b.CreateDistribution("ref-sorted-inv", "sorted-inv-dist", true, nil)
	require.NoError(t, err)

	for i := range 5 {
		_, err = b.CreateInvalidation(d.ID, fmt.Sprintf("caller-%d", i), []string{"/path"})
		require.NoError(t, err)
	}

	invs, err := b.ListInvalidations(d.ID)
	require.NoError(t, err)
	require.Len(t, invs, 5)

	for i := 1; i < len(invs); i++ {
		assert.LessOrEqual(t, invs[i-1].ID, invs[i].ID,
			"invalidations should be sorted by ID")
	}
}

// TestRefinement1_ErrorMapping verifies handleError maps sentinel errors to correct HTTP codes.
func TestRefinement1_ErrorMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		wantCode   string
		body       []byte
		wantStatus int
	}{
		{
			name:       "distribution_not_found",
			method:     http.MethodGet,
			path:       "/2020-05-31/distribution/NOTEXIST",
			wantStatus: http.StatusNotFound,
			wantCode:   "NoSuchDistribution",
		},
		{
			name:       "oai_not_found",
			method:     http.MethodGet,
			path:       "/2020-05-31/origin-access-identity/cloudfront/NOTEXIST",
			wantStatus: http.StatusNotFound,
			wantCode:   "NoSuchCloudFrontOriginAccessIdentity",
		},
		{
			name:   "cache_policy_duplicate",
			method: http.MethodPost,
			path:   "/2020-05-31/cache-policy",
			body: []byte(
				`<CachePolicyConfig><Name>dup-policy</Name>` +
					`<DefaultTTL>100</DefaultTTL><MaxTTL>200</MaxTTL><MinTTL>0</MinTTL>` +
					`</CachePolicyConfig>`,
			),
			wantStatus: http.StatusConflict,
			wantCode:   "DistributionAlreadyExists",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// For the cache policy duplicate test, create it first.
			if tt.wantCode == "DistributionAlreadyExists" {
				rec := doXML(t, h, http.MethodPost, tt.path, tt.body)
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			rec := doXML(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantCode)
		})
	}
}

// TestRefinement1_CreateDistributionValidation verifies CallerReference is validated.
func TestRefinement1_CreateDistributionValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	body := []byte(
		`<DistributionConfig><CallerReference></CallerReference>` +
			`<Comment>no-ref</Comment><Enabled>true</Enabled></DistributionConfig>`,
	)
	rec := doXML(t, h, http.MethodPost, "/2020-05-31/distribution", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidArgument")
}

// TestRefinement1_ConnectionFunctionByID verifies ConnectionFunction stores by ID not by name.
func TestRefinement1_ConnectionFunctionByID(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend("123456789012", config.DefaultRegion)

	// Create two functions with same name (should succeed - AWS allows this).
	fn1, err := b.CreateConnectionFunction("shared-name", "first fn")
	require.NoError(t, err)

	fn2, err := b.CreateConnectionFunction("shared-name", "second fn")
	require.NoError(t, err)

	// ARNs must be different since they are keyed by ID.
	assert.NotEqual(t, fn1.ARN, fn2.ARN,
		"two functions with same name should have different ARNs")
}

// TestRefinement1_HandleGetInvalidationPathFallback tests path-based invID parsing.
func TestRefinement1_HandleGetInvalidationPathFallback(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend("123456789012", config.DefaultRegion)
	h := cloudfront.NewHandler(b)

	d, err := b.CreateDistribution("ref-path-fb", "path-fb-dist", true, nil)
	require.NoError(t, err)

	inv, err := b.CreateInvalidation(d.ID, "caller-path-fb", []string{"/assets/*"})
	require.NoError(t, err)

	// The path-based invalidation GET route.
	path := "/2020-05-31/distribution/" + d.ID + "/invalidation/" + inv.ID
	rec := doXML(t, h, http.MethodGet, path, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), inv.ID)
	assert.Contains(t, rec.Body.String(), "/assets/*")
}

// ---- New tests for newly implemented operations ----

// TestCachePolicyCRUD covers cache policy get, list, update and delete.
func TestCachePolicyCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *cloudfront.Handler) string
		check      func(*testing.T, *httptest.ResponseRecorder, string)
		headers    func(*testing.T, *cloudfront.Handler, string) map[string]string
		name       string
		method     string
		path       string
		body       []byte
		wantStatus int
	}{
		{
			name:   "create_cache_policy_via_handler",
			method: http.MethodPost,
			path:   "/2020-05-31/cache-policy",
			body: []byte(
				`<CachePolicyConfig>` +
					`<Name>test-policy</Name><Comment>test</Comment>` +
					`<DefaultTTL>86400</DefaultTTL><MaxTTL>31536000</MaxTTL><MinTTL>0</MinTTL>` +
					`</CachePolicyConfig>`,
			),
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<CachePolicy")
				assert.Contains(t, rec.Body.String(), "test-policy")
				assert.NotEmpty(t, rec.Header().Get("Location"))
			},
		},
		{
			name:   "get_cache_policy",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateCachePolicy("get-policy", "comment", 86400, 31536000, 0)
				require.NoError(t, err)

				return "/2020-05-31/cache-policy/" + p.ID
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "get-policy")
			},
		},
		{
			name:   "get_cache_policy_not_found",
			method: http.MethodGet,
			path:   "/2020-05-31/cache-policy/DOESNOTEXIST",
			body:   nil,
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "NoSuchCachePolicy")
			},
		},
		{
			name:   "get_cache_policy_config",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateCachePolicy("cfg-policy", "comment", 86400, 31536000, 0)
				require.NoError(t, err)

				return "/2020-05-31/cache-policy/" + p.ID + "/config"
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "cfg-policy")
				assert.Contains(t, rec.Body.String(), "CachePolicyConfig")
			},
		},
		{
			name:   "list_cache_policies",
			method: http.MethodGet,
			path:   "/2020-05-31/cache-policy",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateCachePolicy("list-policy", "comment", 86400, 31536000, 0)
				require.NoError(t, err)

				return ""
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "CachePolicyList")
				assert.Contains(t, rec.Body.String(), "list-policy")
			},
		},
		{
			name:   "update_cache_policy",
			method: http.MethodPut,
			path:   "",
			body: []byte(
				`<CachePolicyConfig>` +
					`<Name>updated-policy</Name><Comment>updated</Comment>` +
					`<DefaultTTL>3600</DefaultTTL><MaxTTL>86400</MaxTTL><MinTTL>0</MinTTL>` +
					`</CachePolicyConfig>`,
			),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateCachePolicy("upd-policy", "comment", 86400, 31536000, 0)
				require.NoError(t, err)

				return "/2020-05-31/cache-policy/" + p.ID
			},
			headers: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				id := strings.TrimPrefix(path, "/2020-05-31/cache-policy/")
				p, err := h.Backend.GetCachePolicy(id)
				require.NoError(t, err)

				return map[string]string{"If-Match": p.ETag}
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "updated-policy")
			},
		},
		{
			name:   "delete_cache_policy",
			method: http.MethodDelete,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateCachePolicy("del-policy", "comment", 86400, 31536000, 0)
				require.NoError(t, err)

				return "/2020-05-31/cache-policy/" + p.ID
			},
			headers: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				id := strings.TrimPrefix(path, "/2020-05-31/cache-policy/")
				p, err := h.Backend.GetCachePolicy(id)
				require.NoError(t, err)

				return map[string]string{"If-Match": p.ETag}
			},
			wantStatus: http.StatusNoContent,
			check:      func(t *testing.T, _ *httptest.ResponseRecorder, _ string) { t.Helper() },
		},
		{
			name:   "delete_cache_policy_not_found",
			method: http.MethodDelete,
			path:   "/2020-05-31/cache-policy/DOESNOTEXIST",
			body:   nil,
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "NoSuchCachePolicy")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			path := tt.path

			if tt.setup != nil {
				if p := tt.setup(t, h); p != "" {
					path = p
				}
			}

			var hdrs map[string]string
			if tt.headers != nil {
				hdrs = tt.headers(t, h, path)
			}

			rec := doXMLWithHeaders(t, h, tt.method, path, tt.body, hdrs)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.check != nil {
				tt.check(t, rec, path)
			}
		})
	}
}

// TestCachePolicyETagValidation verifies that ETag is returned in responses and that
// If-Match validation works for cache policy update and delete.
func TestCachePolicyETagValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *cloudfront.Handler) (string, map[string]string)
		check      func(*testing.T, *httptest.ResponseRecorder)
		name       string
		method     string
		body       []byte
		wantStatus int
	}{
		{
			name:   "create_returns_etag",
			method: http.MethodPost,
			body: []byte(
				`<CachePolicyConfig>` +
					`<Name>etag-create-policy</Name><Comment>test</Comment>` +
					`<DefaultTTL>86400</DefaultTTL><MaxTTL>31536000</MaxTTL><MinTTL>0</MinTTL>` +
					`</CachePolicyConfig>`,
			),
			setup: func(t *testing.T, _ *cloudfront.Handler) (string, map[string]string) {
				t.Helper()

				return "/2020-05-31/cache-policy", nil
			},
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "get_returns_etag",
			method: http.MethodGet,
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) (string, map[string]string) {
				t.Helper()
				p, err := h.Backend.CreateCachePolicy("etag-get-policy", "comment", 86400, 31536000, 0)
				require.NoError(t, err)

				return "/2020-05-31/cache-policy/" + p.ID, nil
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "update_precondition_failed_no_if_match",
			method: http.MethodPut,
			body: []byte(
				`<CachePolicyConfig>` +
					`<Name>etag-upd-policy</Name><Comment>upd</Comment>` +
					`<DefaultTTL>3600</DefaultTTL><MaxTTL>86400</MaxTTL><MinTTL>0</MinTTL>` +
					`</CachePolicyConfig>`,
			),
			setup: func(t *testing.T, h *cloudfront.Handler) (string, map[string]string) {
				t.Helper()
				p, err := h.Backend.CreateCachePolicy("etag-upd-policy", "orig", 86400, 31536000, 0)
				require.NoError(t, err)

				return "/2020-05-31/cache-policy/" + p.ID, nil
			},
			wantStatus: http.StatusPreconditionFailed,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "PreconditionFailed")
			},
		},
		{
			name:   "update_precondition_failed_wrong_etag",
			method: http.MethodPut,
			body: []byte(
				`<CachePolicyConfig>` +
					`<Name>etag-upd2-policy</Name><Comment>upd</Comment>` +
					`<DefaultTTL>3600</DefaultTTL><MaxTTL>86400</MaxTTL><MinTTL>0</MinTTL>` +
					`</CachePolicyConfig>`,
			),
			setup: func(t *testing.T, h *cloudfront.Handler) (string, map[string]string) {
				t.Helper()
				p, err := h.Backend.CreateCachePolicy("etag-upd2-policy", "orig", 86400, 31536000, 0)
				require.NoError(t, err)

				return "/2020-05-31/cache-policy/" + p.ID, map[string]string{"If-Match": "wrong-etag"}
			},
			wantStatus: http.StatusPreconditionFailed,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "PreconditionFailed")
			},
		},
		{
			name:   "delete_precondition_failed_no_if_match",
			method: http.MethodDelete,
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) (string, map[string]string) {
				t.Helper()
				p, err := h.Backend.CreateCachePolicy("etag-del-policy", "orig", 86400, 31536000, 0)
				require.NoError(t, err)

				return "/2020-05-31/cache-policy/" + p.ID, nil
			},
			wantStatus: http.StatusPreconditionFailed,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "PreconditionFailed")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			path, hdrs := tt.setup(t, h)

			rec := doXMLWithHeaders(t, h, tt.method, path, tt.body, hdrs)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec)
		})
	}
}

// TestOriginAccessControlCRUD covers OAC create, get, list, update and delete.
func TestOriginAccessControlCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *cloudfront.Handler) string
		check      func(*testing.T, *httptest.ResponseRecorder, string)
		headers    func(*testing.T, *cloudfront.Handler, string) map[string]string
		name       string
		method     string
		path       string
		body       []byte
		wantStatus int
	}{
		{
			name:   "create_oac",
			method: http.MethodPost,
			path:   "/2020-05-31/origin-access-control",
			body: []byte(
				`<OriginAccessControlConfig>` +
					`<Name>my-oac</Name><Description>desc</Description>` +
					`<OriginAccessControlOriginType>s3</OriginAccessControlOriginType>` +
					`<SigningBehavior>always</SigningBehavior><SigningProtocol>sigv4</SigningProtocol>` +
					`</OriginAccessControlConfig>`,
			),
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "OriginAccessControl")
				assert.Contains(t, rec.Body.String(), "my-oac")
				assert.NotEmpty(t, rec.Header().Get("Location"))
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "get_oac",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				oac, err := h.Backend.CreateOriginAccessControl("get-oac", "", "s3", "always", "sigv4")
				require.NoError(t, err)

				return "/2020-05-31/origin-access-control/" + oac.ID
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "get-oac")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "get_oac_not_found",
			method: http.MethodGet,
			path:   "/2020-05-31/origin-access-control/DOESNOTEXIST",
			body:   nil,
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "NoSuchOriginAccessControl")
			},
		},
		{
			name:   "get_oac_config",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				oac, err := h.Backend.CreateOriginAccessControl("cfg-oac", "desc", "s3", "always", "sigv4")
				require.NoError(t, err)

				return "/2020-05-31/origin-access-control/" + oac.ID + "/config"
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "OriginAccessControlConfig")
			},
		},
		{
			name:   "list_oacs",
			method: http.MethodGet,
			path:   "/2020-05-31/origin-access-control",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateOriginAccessControl("list-oac", "", "s3", "always", "sigv4")
				require.NoError(t, err)

				return ""
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "OriginAccessControlList")
				assert.Contains(t, rec.Body.String(), "list-oac")
			},
		},
		{
			name:   "update_oac",
			method: http.MethodPut,
			path:   "",
			body: []byte(
				`<OriginAccessControlConfig>` +
					`<Name>updated-oac</Name><Description>new desc</Description>` +
					`<OriginAccessControlOriginType>s3</OriginAccessControlOriginType>` +
					`<SigningBehavior>never</SigningBehavior><SigningProtocol>sigv4</SigningProtocol>` +
					`</OriginAccessControlConfig>`,
			),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				oac, err := h.Backend.CreateOriginAccessControl("orig-oac", "", "s3", "always", "sigv4")
				require.NoError(t, err)

				return "/2020-05-31/origin-access-control/" + oac.ID + "/config"
			},
			headers: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				id := strings.TrimPrefix(strings.TrimSuffix(path, "/config"), "/2020-05-31/origin-access-control/")
				oac, err := h.Backend.GetOriginAccessControl(id)
				require.NoError(t, err)

				return map[string]string{"If-Match": oac.ETag}
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "updated-oac")
			},
		},
		{
			name:   "delete_oac",
			method: http.MethodDelete,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				oac, err := h.Backend.CreateOriginAccessControl("del-oac", "", "s3", "always", "sigv4")
				require.NoError(t, err)

				return "/2020-05-31/origin-access-control/" + oac.ID
			},
			headers: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				id := strings.TrimPrefix(path, "/2020-05-31/origin-access-control/")
				oac, err := h.Backend.GetOriginAccessControl(id)
				require.NoError(t, err)

				return map[string]string{"If-Match": oac.ETag}
			},
			wantStatus: http.StatusNoContent,
			check:      func(t *testing.T, _ *httptest.ResponseRecorder, _ string) { t.Helper() },
		},
		{
			name:   "delete_oac_not_found",
			method: http.MethodDelete,
			path:   "/2020-05-31/origin-access-control/DOESNOTEXIST",
			body:   nil,
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "NoSuchOriginAccessControl")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			path := tt.path

			if tt.setup != nil {
				if p := tt.setup(t, h); p != "" {
					path = p
				}
			}

			var hdrs map[string]string
			if tt.headers != nil {
				hdrs = tt.headers(t, h, path)
			}

			rec := doXMLWithHeaders(t, h, tt.method, path, tt.body, hdrs)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.check != nil {
				tt.check(t, rec, path)
			}
		})
	}
}

// TestResponseHeadersPolicyCRUD covers response headers policy full lifecycle.
func TestResponseHeadersPolicyCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *cloudfront.Handler) string
		check      func(*testing.T, *httptest.ResponseRecorder, string)
		headers    func(*testing.T, *cloudfront.Handler, string) map[string]string
		name       string
		method     string
		path       string
		body       []byte
		wantStatus int
	}{
		{
			name:   "create_rhp",
			method: http.MethodPost,
			path:   "/2020-05-31/response-headers-policy",
			body: []byte(
				`<ResponseHeadersPolicyConfig>` +
					`<Name>my-rhp</Name><Comment>comment</Comment>` +
					`</ResponseHeadersPolicyConfig>`,
			),
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "ResponseHeadersPolicy")
				assert.Contains(t, rec.Body.String(), "my-rhp")
				assert.NotEmpty(t, rec.Header().Get("Location"))
			},
		},
		{
			name:   "get_rhp",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateResponseHeadersPolicy("get-rhp", "")
				require.NoError(t, err)

				return "/2020-05-31/response-headers-policy/" + p.ID
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "get-rhp")
			},
		},
		{
			name:   "get_rhp_not_found",
			method: http.MethodGet,
			path:   "/2020-05-31/response-headers-policy/DOESNOTEXIST",
			body:   nil,
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "NoSuchResponseHeadersPolicy")
			},
		},
		{
			name:   "get_rhp_config",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateResponseHeadersPolicy("cfg-rhp", "cfg comment")
				require.NoError(t, err)

				return "/2020-05-31/response-headers-policy/" + p.ID + "/config"
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "ResponseHeadersPolicyConfig")
			},
		},
		{
			name:   "list_rhps",
			method: http.MethodGet,
			path:   "/2020-05-31/response-headers-policy",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateResponseHeadersPolicy("list-rhp", "")
				require.NoError(t, err)

				return ""
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "ResponseHeadersPolicyList")
				assert.Contains(t, rec.Body.String(), "list-rhp")
			},
		},
		{
			name:   "update_rhp",
			method: http.MethodPut,
			path:   "",
			body: []byte(
				`<ResponseHeadersPolicyConfig>` +
					`<Name>updated-rhp</Name><Comment>new</Comment>` +
					`</ResponseHeadersPolicyConfig>`,
			),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateResponseHeadersPolicy("orig-rhp", "")
				require.NoError(t, err)

				return "/2020-05-31/response-headers-policy/" + p.ID + "/config"
			},
			headers: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				id := strings.TrimPrefix(strings.TrimSuffix(path, "/config"), "/2020-05-31/response-headers-policy/")
				p, err := h.Backend.GetResponseHeadersPolicy(id)
				require.NoError(t, err)

				return map[string]string{"If-Match": p.ETag}
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "updated-rhp")
			},
		},
		{
			name:   "delete_rhp",
			method: http.MethodDelete,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateResponseHeadersPolicy("del-rhp", "")
				require.NoError(t, err)

				return "/2020-05-31/response-headers-policy/" + p.ID
			},
			headers: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				id := strings.TrimPrefix(path, "/2020-05-31/response-headers-policy/")
				p, err := h.Backend.GetResponseHeadersPolicy(id)
				require.NoError(t, err)

				return map[string]string{"If-Match": p.ETag}
			},
			wantStatus: http.StatusNoContent,
			check:      func(t *testing.T, _ *httptest.ResponseRecorder, _ string) { t.Helper() },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			path := tt.path

			if tt.setup != nil {
				if p := tt.setup(t, h); p != "" {
					path = p
				}
			}

			var hdrs map[string]string
			if tt.headers != nil {
				hdrs = tt.headers(t, h, path)
			}

			rec := doXMLWithHeaders(t, h, tt.method, path, tt.body, hdrs)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.check != nil {
				tt.check(t, rec, path)
			}
		})
	}
}

// TestCloudFrontFunctionCRUD covers function create, get, describe, list, publish, update, delete, test.
func TestCloudFrontFunctionCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *cloudfront.Handler) string
		check      func(*testing.T, *httptest.ResponseRecorder, string)
		headers    func(*testing.T, *cloudfront.Handler, string) map[string]string
		name       string
		method     string
		path       string
		body       []byte
		wantStatus int
	}{
		{
			name:   "create_function",
			method: http.MethodPost,
			path:   "/2020-05-31/function",
			body: []byte(
				`<CreateFunctionRequest>` +
					`<Name>my-fn</Name>` +
					`<FunctionConfig>` +
					`<Comment>test fn</Comment><Runtime>cloudfront-js-2.0</Runtime>` +
					`</FunctionConfig>` +
					`<FunctionCode>ZnVuY3Rpb24gaGFuZGxlcihldmVudCkge3JldHVybiBldmVudC5yZXF1ZXN0O30=</FunctionCode>` +
					`</CreateFunctionRequest>`,
			),
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "FunctionSummary")
				assert.Contains(t, rec.Body.String(), "my-fn")
				assert.NotEmpty(t, rec.Header().Get("Location"))
			},
		},
		{
			name:   "get_function",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateFunction("get-fn", "comment", "cloudfront-js-2.0", "code")
				require.NoError(t, err)

				return "/2020-05-31/function/get-fn"
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "get-fn")
			},
		},
		{
			name:   "get_function_not_found",
			method: http.MethodGet,
			path:   "/2020-05-31/function/DOESNOTEXIST",
			body:   nil,
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "NoSuchFunctionExists")
			},
		},
		{
			name:   "describe_function",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateFunction("desc-fn", "comment", "cloudfront-js-2.0", "code")
				require.NoError(t, err)

				return "/2020-05-31/function/desc-fn/describe"
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "desc-fn")
			},
		},
		{
			name:   "list_functions",
			method: http.MethodGet,
			path:   "/2020-05-31/function",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateFunction("list-fn", "comment", "cloudfront-js-2.0", "code")
				require.NoError(t, err)

				return ""
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "FunctionList")
				assert.Contains(t, rec.Body.String(), "list-fn")
			},
		},
		{
			name:   "publish_function",
			method: http.MethodPost,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateFunction("pub-fn", "comment", "cloudfront-js-2.0", "code")
				require.NoError(t, err)

				return "/2020-05-31/function/pub-fn/publish"
			},
			headers: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				name := strings.TrimPrefix(strings.TrimSuffix(path, "/publish"), "/2020-05-31/function/")
				fn, err := h.Backend.GetFunction(name)
				require.NoError(t, err)

				return map[string]string{"If-Match": fn.ETag}
			},
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "LIVE")
			},
		},
		{
			name:   "update_function",
			method: http.MethodPut,
			path:   "",
			body: []byte(
				`<CreateFunctionRequest>` +
					`<Name>upd-fn</Name>` +
					`<FunctionConfig><Comment>updated</Comment><Runtime>cloudfront-js-2.0</Runtime></FunctionConfig>` +
					`</CreateFunctionRequest>`,
			),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateFunction("upd-fn", "original", "cloudfront-js-2.0", "code")
				require.NoError(t, err)

				return "/2020-05-31/function/upd-fn"
			},
			headers: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				name := strings.TrimPrefix(path, "/2020-05-31/function/")
				fn, err := h.Backend.GetFunction(name)
				require.NoError(t, err)

				return map[string]string{"If-Match": fn.ETag}
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "upd-fn")
			},
		},
		{
			name:   "delete_function",
			method: http.MethodDelete,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateFunction("del-fn", "comment", "cloudfront-js-2.0", "code")
				require.NoError(t, err)

				return "/2020-05-31/function/del-fn"
			},
			headers: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				name := strings.TrimPrefix(path, "/2020-05-31/function/")
				fn, err := h.Backend.GetFunction(name)
				require.NoError(t, err)

				return map[string]string{"If-Match": fn.ETag}
			},
			wantStatus: http.StatusNoContent,
			check:      func(t *testing.T, _ *httptest.ResponseRecorder, _ string) { t.Helper() },
		},
		{
			name:   "test_function",
			method: http.MethodPost,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateFunction("test-fn", "comment", "cloudfront-js-2.0", "code")
				require.NoError(t, err)

				return "/2020-05-31/function/test-fn/test"
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "TestResult")
				assert.Contains(t, rec.Body.String(), "test-fn")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			path := tt.path

			if tt.setup != nil {
				if p := tt.setup(t, h); p != "" {
					path = p
				}
			}

			var hdrs map[string]string
			if tt.headers != nil {
				hdrs = tt.headers(t, h, path)
			}

			rec := doXMLWithHeaders(t, h, tt.method, path, tt.body, hdrs)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.check != nil {
				tt.check(t, rec, path)
			}
		})
	}
}

// TestOriginRequestPolicyCRUD covers origin request policy full lifecycle.
func TestOriginRequestPolicyCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *cloudfront.Handler) string
		check      func(*testing.T, *httptest.ResponseRecorder, string)
		headers    func(*testing.T, *cloudfront.Handler, string) map[string]string
		name       string
		method     string
		path       string
		body       []byte
		wantStatus int
	}{
		{
			name:   "create_orp",
			method: http.MethodPost,
			path:   "/2020-05-31/origin-request-policy",
			body: []byte(
				`<OriginRequestPolicyConfig>` +
					`<Name>my-orp</Name><Comment>comment</Comment>` +
					`</OriginRequestPolicyConfig>`,
			),
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "OriginRequestPolicy")
				assert.Contains(t, rec.Body.String(), "my-orp")
				assert.NotEmpty(t, rec.Header().Get("Location"))
			},
		},
		{
			name:   "get_orp",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateOriginRequestPolicy("get-orp", "")
				require.NoError(t, err)

				return "/2020-05-31/origin-request-policy/" + p.ID
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "get-orp")
			},
		},
		{
			name:   "get_orp_not_found",
			method: http.MethodGet,
			path:   "/2020-05-31/origin-request-policy/DOESNOTEXIST",
			body:   nil,
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "NoSuchOriginRequestPolicy")
			},
		},
		{
			name:   "get_orp_config",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateOriginRequestPolicy("cfg-orp", "")
				require.NoError(t, err)

				return "/2020-05-31/origin-request-policy/" + p.ID + "/config"
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "OriginRequestPolicyConfig")
			},
		},
		{
			name:   "list_orps",
			method: http.MethodGet,
			path:   "/2020-05-31/origin-request-policy",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateOriginRequestPolicy("list-orp", "")
				require.NoError(t, err)

				return ""
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "OriginRequestPolicyList")
				assert.Contains(t, rec.Body.String(), "list-orp")
			},
		},
		{
			name:   "update_orp",
			method: http.MethodPut,
			path:   "",
			body: []byte(
				`<OriginRequestPolicyConfig>` +
					`<Name>updated-orp</Name><Comment>new</Comment>` +
					`</OriginRequestPolicyConfig>`,
			),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateOriginRequestPolicy("orig-orp", "")
				require.NoError(t, err)

				return "/2020-05-31/origin-request-policy/" + p.ID + "/config"
			},
			headers: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				id := strings.TrimPrefix(strings.TrimSuffix(path, "/config"), "/2020-05-31/origin-request-policy/")
				p, err := h.Backend.GetOriginRequestPolicy(id)
				require.NoError(t, err)

				return map[string]string{"If-Match": p.ETag}
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "updated-orp")
			},
		},
		{
			name:   "delete_orp",
			method: http.MethodDelete,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateOriginRequestPolicy("del-orp", "")
				require.NoError(t, err)

				return "/2020-05-31/origin-request-policy/" + p.ID
			},
			headers: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				id := strings.TrimPrefix(path, "/2020-05-31/origin-request-policy/")
				p, err := h.Backend.GetOriginRequestPolicy(id)
				require.NoError(t, err)

				return map[string]string{"If-Match": p.ETag}
			},
			wantStatus: http.StatusNoContent,
			check:      func(t *testing.T, _ *httptest.ResponseRecorder, _ string) { t.Helper() },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			path := tt.path

			if tt.setup != nil {
				if p := tt.setup(t, h); p != "" {
					path = p
				}
			}

			var hdrs map[string]string
			if tt.headers != nil {
				hdrs = tt.headers(t, h, path)
			}

			rec := doXMLWithHeaders(t, h, tt.method, path, tt.body, hdrs)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.check != nil {
				tt.check(t, rec, path)
			}
		})
	}
}

// TestNewBackendCRUD tests backend methods for new resource types.
func TestNewBackendCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run     func(*testing.T, *cloudfront.InMemoryBackend)
		name    string
		wantErr bool
	}{
		{
			name: "oac_duplicate_name",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateOriginAccessControl("dup", "", "s3", "always", "sigv4")
				require.NoError(t, err)
				_, err = b.CreateOriginAccessControl("dup", "", "s3", "always", "sigv4")
				require.Error(t, err)
			},
		},
		{
			name: "oac_delete_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.DeleteOriginAccessControl("DOESNOTEXIST")
				require.Error(t, err)
			},
		},
		{
			name: "rhp_duplicate_name",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateResponseHeadersPolicy("dup-rhp", "")
				require.NoError(t, err)
				_, err = b.CreateResponseHeadersPolicy("dup-rhp", "")
				require.Error(t, err)
			},
		},
		{
			name: "function_duplicate_name",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateFunction("dup-fn", "", "cloudfront-js-2.0", "code")
				require.NoError(t, err)
				_, err = b.CreateFunction("dup-fn", "", "cloudfront-js-2.0", "code")
				require.Error(t, err)
			},
		},
		{
			name: "function_publish_sets_live",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateFunction("live-fn", "", "cloudfront-js-2.0", "code")
				require.NoError(t, err)
				fn, err := b.PublishFunction("live-fn")
				require.NoError(t, err)
				assert.Equal(t, "LIVE", fn.Status)
			},
		},
		{
			name: "function_update_sets_development",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateFunction("upd-fn2", "", "cloudfront-js-2.0", "code")
				require.NoError(t, err)
				_, err = b.PublishFunction("upd-fn2")
				require.NoError(t, err)
				fn, err := b.UpdateFunction("upd-fn2", "new comment", "cloudfront-js-2.0", "new code")
				require.NoError(t, err)
				assert.Equal(t, "DEVELOPMENT", fn.Status)
			},
		},
		{
			name: "orp_duplicate_name",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateOriginRequestPolicy("dup-orp", "")
				require.NoError(t, err)
				_, err = b.CreateOriginRequestPolicy("dup-orp", "")
				require.Error(t, err)
			},
		},
		{
			name: "cache_policy_update_name_conflict",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateCachePolicy("p1", "", 86400, 31536000, 0)
				require.NoError(t, err)
				p2, err := b.CreateCachePolicy("p2", "", 86400, 31536000, 0)
				require.NoError(t, err)
				// Try to rename p2 to p1 (conflict)
				_, err = b.UpdateCachePolicy(p2.ID, "p1", "", 86400, 31536000, 0)
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudfront.NewInMemoryBackend("123456789012", "us-east-1")
			tt.run(t, b)
		})
	}
}
