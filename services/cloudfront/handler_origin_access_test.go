package cloudfront_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestOAICanonicalUserID verifies that OAI S3 canonical user IDs are SHA256 hex strings.
func TestOAICanonicalUserID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(*testing.T, *cloudfront.InMemoryBackend)
		name string
	}{
		{
			name: "canonical_user_id_is_64_hex_chars",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				oai, err := b.CreateOAI("ref-1", "comment-1")
				require.NoError(t, err)
				assert.Len(t, oai.S3CanonicalUserID, 64, "S3CanonicalUserID should be 64 hex chars (SHA256)")
				for _, c := range oai.S3CanonicalUserID {
					assert.True(t, (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'),
						"S3CanonicalUserID should be lowercase hex, got char %c", c)
				}
			},
		},
		{
			name: "different_oais_have_different_canonical_ids",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				oai1, err := b.CreateOAI("ref-a", "comment-a")
				require.NoError(t, err)
				oai2, err := b.CreateOAI("ref-b", "comment-b")
				require.NoError(t, err)
				assert.NotEqual(t, oai1.S3CanonicalUserID, oai2.S3CanonicalUserID)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newAuditBackend(t)
			tt.run(t, b)
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

			h := newTestHandler(t)
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

// TestCreateCloudFrontOriginAccessIdentity verifies that CreateCloudFrontOriginAccessIdentity
// (the renamed op) still works and is reported correctly.
func TestCreateCloudFrontOriginAccessIdentity(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/2020-05-31/origin-access-identity/cloudfront", nil)
	c := e.NewContext(req, httptest.NewRecorder())

	assert.Equal(t, "CreateCloudFrontOriginAccessIdentity", h.ExtractOperation(c))
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

			h := newTestHandler(t)
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
