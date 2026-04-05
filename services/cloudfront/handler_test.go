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
				tt.callerRef, len(tt.paths), pathItems.String())

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

	_, err = b.CreateContinuousDeploymentPolicy(true)
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
				p, err := b.CreateContinuousDeploymentPolicy(true)
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
