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
				d, err := h.Backend.CreateDistribution("ref-006", "del-dist", true,
					minimalDistConfig("ref-006", "del-dist", true))
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
				d, err := h.Backend.CreateDistribution("ref-008", "del-dist-2", true,
					minimalDistConfig("ref-008", "del-dist-2", true))
				require.NoError(t, err)

				return "/2020-05-31/distribution/" + d.ID
			},
			wantStatus: http.StatusPreconditionFailed,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "PreconditionFailed")
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
			wantOperation: "CreateOriginAccessIdentity",
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
