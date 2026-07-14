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

// TestResponseHeadersPolicyConfig tests RHP creation with full config.
func TestResponseHeadersPolicyConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantIn   string
		wantCode int
	}{
		{
			name: "create_with_cors_config",
			body: `<ResponseHeadersPolicyConfig>
				<Name>cors-policy</Name>
				<Comment>test cors</Comment>
				<CorsConfig>
					<AccessControlAllowCredentials>true</AccessControlAllowCredentials>
					<AccessControlMaxAgeSec>600</AccessControlMaxAgeSec>
					<OriginOverride>true</OriginOverride>
				</CorsConfig>
			</ResponseHeadersPolicyConfig>`,
			wantCode: http.StatusCreated,
			wantIn:   "cors-policy",
		},
		{
			name: "create_with_security_headers",
			body: `<ResponseHeadersPolicyConfig>
				<Name>sec-headers</Name>
				<SecurityHeadersConfig>
					<StrictTransportSecurity>
						<AccessControlMaxAgeSec>31536000</AccessControlMaxAgeSec>
						<IncludeSubdomains>true</IncludeSubdomains>
						<Preload>true</Preload>
					</StrictTransportSecurity>
					<FrameOptions><FrameOption>SAMEORIGIN</FrameOption></FrameOptions>
				</SecurityHeadersConfig>
			</ResponseHeadersPolicyConfig>`,
			wantCode: http.StatusCreated,
			wantIn:   "sec-headers",
		},
		{
			name: "create_with_custom_headers",
			body: `<ResponseHeadersPolicyConfig>
				<Name>custom-headers</Name>
				<CustomHeadersConfig>
					<Items>
						<ResponseHeadersPolicyCustomHeader>
							<Header>X-Custom</Header>
							<Value>custom-value</Value>
							<Override>true</Override>
						</ResponseHeadersPolicyCustomHeader>
					</Items>
					<Quantity>1</Quantity>
				</CustomHeadersConfig>
			</ResponseHeadersPolicyConfig>`,
			wantCode: http.StatusCreated,
			wantIn:   "custom-headers",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newAuditBackend()
			h := cloudfront.NewHandler(b)
			rec := doReq(t, h, http.MethodPost, "/2020-05-31/response-headers-policy", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), tt.wantIn)
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
