package cloudfront_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestOriginRequestPolicyConfig tests ORP creation with full config.
func TestOriginRequestPolicyConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantIn   string
		wantCode int
	}{
		{
			name: "create_with_headers_config",
			body: `<OriginRequestPolicyConfig>
				<Name>headers-policy</Name>
				<HeadersConfig>
					<HeaderBehavior>whitelist</HeaderBehavior>
					<Headers>
						<Items><Name>Accept</Name><Name>Accept-Language</Name></Items>
						<Quantity>2</Quantity>
					</Headers>
				</HeadersConfig>
				<CookiesConfig><CookieBehavior>none</CookieBehavior></CookiesConfig>
				<QueryStringsConfig><QueryStringBehavior>none</QueryStringBehavior></QueryStringsConfig>
			</OriginRequestPolicyConfig>`,
			wantCode: http.StatusCreated,
			wantIn:   "headers-policy",
		},
		{
			name: "create_with_cookies_config",
			body: `<OriginRequestPolicyConfig>
				<Name>cookies-policy</Name>
				<HeadersConfig><HeaderBehavior>none</HeaderBehavior></HeadersConfig>
				<CookiesConfig>
					<CookieBehavior>whitelist</CookieBehavior>
					<Cookies>
						<Items><Name>session</Name></Items>
						<Quantity>1</Quantity>
					</Cookies>
				</CookiesConfig>
				<QueryStringsConfig><QueryStringBehavior>all</QueryStringBehavior></QueryStringsConfig>
			</OriginRequestPolicyConfig>`,
			wantCode: http.StatusCreated,
			wantIn:   "cookies-policy",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newAuditBackend()
			h := cloudfront.NewHandler(b)
			rec := doReq(t, h, http.MethodPost, "/2020-05-31/origin-request-policy", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), tt.wantIn)
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
