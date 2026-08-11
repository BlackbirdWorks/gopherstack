package cloudfront_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestCachePolicyParams tests that CachePolicy stores and returns Params fields.
func TestCachePolicyParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		body   string
		wantIn string
	}{
		{
			name: "create_with_headers_config",
			body: `<CachePolicyConfig>
				<Name>test-policy</Name>
				<DefaultTTL>86400</DefaultTTL>
				<MaxTTL>31536000</MaxTTL>
				<MinTTL>0</MinTTL>
				<ParametersInCacheKeyAndForwardedToOrigin>
					<HeadersConfig>
						<HeaderBehavior>whitelist</HeaderBehavior>
						<Headers><Quantity>1</Quantity><Items><Name>Accept</Name></Items></Headers>
					</HeadersConfig>
					<CookiesConfig><CookieBehavior>none</CookieBehavior></CookiesConfig>
					<QueryStringsConfig><QueryStringBehavior>none</QueryStringBehavior></QueryStringsConfig>
					<EnableAcceptEncodingGzip>true</EnableAcceptEncodingGzip>
				</ParametersInCacheKeyAndForwardedToOrigin>
			</CachePolicyConfig>`,
			wantIn: "test-policy",
		},
		{
			name: "create_minimal",
			body: `<CachePolicyConfig>
				<Name>minimal</Name>
				<DefaultTTL>3600</DefaultTTL>
				<MaxTTL>86400</MaxTTL>
				<MinTTL>0</MinTTL>
			</CachePolicyConfig>`,
			wantIn: "minimal",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newAuditBackend(t)
			h := cloudfront.NewHandler(b)
			rec := doReq(t, h, http.MethodPost, "/2020-05-31/cache-policy", tt.body)
			assert.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), tt.wantIn)
		})
	}
}

// TestCachePolicyMaxTTL verifies that cache policies reject MaxTTL > 31536000.
func TestCachePolicyMaxTTL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		maxTTL   int
		wantCode int
	}{
		{name: "max_ttl_one_year_accepted", maxTTL: 31536000, wantCode: http.StatusCreated},
		{name: "max_ttl_exceeded_rejected", maxTTL: 31536001, wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newAuditBackend(t)
			h := cloudfront.NewHandler(b)

			body := fmt.Sprintf(`<CachePolicyConfig>
				<Name>ttl-test-%d</Name>
				<DefaultTTL>86400</DefaultTTL>
				<MaxTTL>%d</MaxTTL>
				<MinTTL>0</MinTTL>
			</CachePolicyConfig>`, tt.maxTTL, tt.maxTTL)

			rec := doReq(t, h, http.MethodPost, "/2020-05-31/cache-policy", body)
			assert.Equal(t, tt.wantCode, rec.Code, "maxTTL=%d body=%s", tt.maxTTL, rec.Body.String())
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

			h := newTestHandler(t)
			rec := doXML(t, h, http.MethodPost, "/2020-05-31/cache-policy", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec)
		})
	}
}

// TestCachePolicyUniqueness verifies duplicate cache policy names are rejected.
func TestCachePolicyUniqueness(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := []byte(
		`<CachePolicyConfig><Name>my-unique-policy</Name>` +
			`<DefaultTTL>86400</DefaultTTL><MaxTTL>31536000</MaxTTL><MinTTL>0</MinTTL>` +
			`</CachePolicyConfig>`,
	)

	rec1 := doXML(t, h, http.MethodPost, "/2020-05-31/cache-policy", body)
	require.Equal(t, http.StatusCreated, rec1.Code)

	rec2 := doXML(t, h, http.MethodPost, "/2020-05-31/cache-policy", body)
	assert.Equal(t, http.StatusConflict, rec2.Code)
	// The real aws-sdk-go-v2 cloudfront error type for a cache policy name collision is
	// CachePolicyAlreadyExists, not DistributionAlreadyExists (which the emulator used to
	// return for every AlreadyExists collision regardless of resource type).
	assert.Contains(t, rec2.Body.String(), "CachePolicyAlreadyExists")
}

// TestCachePolicyTTLValidation verifies TTL ordering is enforced.
func TestCachePolicyTTLValidation(t *testing.T) {
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

			h := newTestHandler(t)
			rec := doXML(t, h, http.MethodPost, "/2020-05-31/cache-policy", []byte(tt.body))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
}

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

			h := newTestHandler(t)
			path, hdrs := tt.setup(t, h)

			rec := doXMLWithHeaders(t, h, tt.method, path, tt.body, hdrs)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec)
		})
	}
}

// TestCachePolicyWhitelistItems_WireRoundTrip proves that whitelisted header/cookie/
// query-string names survive a full Create -> Get -> GetConfig -> List round trip.
// Before this fix, the request parser used the wrong XML paths (Headers>Header
// instead of the real Headers>Items>Name) so any real SDK-generated whitelist
// request silently lost every listed name on unmarshal, and every read response
// omitted the Items list entirely -- a real SDK client had no way to discover which
// headers/cookies/query-strings a policy actually whitelists.
func TestCachePolicyWhitelistItems_WireRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := []byte(`<CachePolicyConfig><Name>wire-cp</Name>` +
		`<DefaultTTL>1</DefaultTTL><MaxTTL>2</MaxTTL><MinTTL>0</MinTTL>` +
		`<ParametersInCacheKeyAndForwardedToOrigin>` +
		`<EnableAcceptEncodingGzip>true</EnableAcceptEncodingGzip>` +
		`<EnableAcceptEncodingBrotli>false</EnableAcceptEncodingBrotli>` +
		`<HeadersConfig><HeaderBehavior>whitelist</HeaderBehavior>` +
		`<Headers><Items><Name>X-Custom-Header</Name></Items><Quantity>1</Quantity></Headers>` +
		`</HeadersConfig>` +
		`<CookiesConfig><CookieBehavior>whitelist</CookieBehavior>` +
		`<Cookies><Items><Name>session-id</Name></Items><Quantity>1</Quantity></Cookies>` +
		`</CookiesConfig>` +
		`<QueryStringsConfig><QueryStringBehavior>whitelist</QueryStringBehavior>` +
		`<QueryStrings><Items><Name>utm_source</Name></Items><Quantity>1</Quantity></QueryStrings>` +
		`</QueryStringsConfig>` +
		`</ParametersInCacheKeyAndForwardedToOrigin>` +
		`</CachePolicyConfig>`)

	createRec := doXML(t, h, http.MethodPost, "/2020-05-31/cache-policy", body)
	require.Equal(t, http.StatusCreated, createRec.Code, createRec.Body.String())

	// The backend model itself must have parsed the whitelisted names (proves the
	// request-side Headers>Items>Name / Cookies>Items>Name / QueryStrings>Items>Name
	// tags are correct, not just that some XML happened to round-trip).
	policies := h.Backend.ListCachePolicies()
	var created *cloudfront.CachePolicy
	for _, p := range policies {
		if p.Name == "wire-cp" {
			created = p
		}
	}
	require.NotNil(t, created)
	require.NotNil(t, created.Params)
	assert.Equal(t, []string{"X-Custom-Header"}, created.Params.HeadersConfig.Headers)
	assert.Equal(t, []string{"session-id"}, created.Params.CookiesConfig.Cookies)
	assert.Equal(t, []string{"utm_source"}, created.Params.QueryStringsConfig.QueryStrings)

	for _, path := range []string{
		"/2020-05-31/cache-policy/" + created.ID,
		"/2020-05-31/cache-policy/" + created.ID + "/config",
		"/2020-05-31/cache-policy",
	} {
		rec := doXML(t, h, http.MethodGet, path, nil)
		require.Equal(t, http.StatusOK, rec.Code, path)
		body := rec.Body.String()
		assert.Contains(t, body, "<Name>X-Custom-Header</Name>", "path %s", path)
		assert.Contains(t, body, "<Name>session-id</Name>", "path %s", path)
		assert.Contains(t, body, "<Name>utm_source</Name>", "path %s", path)
	}
}
