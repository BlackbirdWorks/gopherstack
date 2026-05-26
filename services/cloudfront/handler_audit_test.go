package cloudfront_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	echo "github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// doReq is a test helper that fires a request through the handler and returns the recorder.
func doReq(
	t *testing.T,
	h *cloudfront.Handler,
	method, path, body string,
) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/xml")

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// doJSONReq fires a JSON-body request through the handler.
func doJSONReq(
	t *testing.T,
	h *cloudfront.Handler,
	method, path, body string,
	headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
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

// newAuditBackend creates a fresh backend for testing.
func newAuditBackend() *cloudfront.InMemoryBackend {
	return cloudfront.NewInMemoryBackend("123456789012", "us-east-1")
}

// TestKVSDataPlane covers the full KVS data-plane lifecycle via the HTTP handler.
func TestKVSDataPlane(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name      string
		setup     func(*testing.T, *cloudfront.InMemoryBackend) string // returns kvsID
		method    string
		pathFn    func(kvsID string) string
		body      string
		header    map[string]string
		wantCode  int
		checkBody func(*testing.T, *httptest.ResponseRecorder)
	}

	makeKVS := func(t *testing.T, b *cloudfront.InMemoryBackend) string {
		t.Helper()
		kvs, err := b.CreateKeyValueStore("test-kvs", "comment")
		require.NoError(t, err)

		return kvs.ID
	}

	makeKVSWithKey := func(key, value string) func(*testing.T, *cloudfront.InMemoryBackend) string {
		return func(t *testing.T, b *cloudfront.InMemoryBackend) string {
			t.Helper()
			kvsID := makeKVS(t, b)
			_, err := b.PutKVSValue(kvsID, key, value, "")
			require.NoError(t, err)

			return kvsID
		}
	}

	tests := []testCase{
		{
			name:     "put_key_creates_entry",
			setup:    makeKVS,
			method:   http.MethodPut,
			pathFn:   func(id string) string { return "/2020-05-31/key-value-store/" + id + "/keys/mykey" },
			body:     `{"value":"myvalue"}`,
			wantCode: http.StatusOK,
			checkBody: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "myvalue", resp["value"])
			},
		},
		{
			name:     "get_key_returns_value",
			setup:    makeKVSWithKey("getkey", "getvalue"),
			method:   http.MethodGet,
			pathFn:   func(id string) string { return "/2020-05-31/key-value-store/" + id + "/keys/getkey" },
			wantCode: http.StatusOK,
			checkBody: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "getvalue", resp["value"])
			},
		},
		{
			name:     "get_key_not_found_returns_404",
			setup:    makeKVS,
			method:   http.MethodGet,
			pathFn:   func(id string) string { return "/2020-05-31/key-value-store/" + id + "/keys/missing" },
			wantCode: http.StatusNotFound,
		},
		{
			name:     "delete_key_removes_entry",
			setup:    makeKVSWithKey("delkey", "delval"),
			method:   http.MethodDelete,
			pathFn:   func(id string) string { return "/2020-05-31/key-value-store/" + id + "/keys/delkey" },
			wantCode: http.StatusNoContent,
		},
		{
			name:     "list_keys_returns_all",
			setup:    makeKVSWithKey("k1", "v1"),
			method:   http.MethodGet,
			pathFn:   func(id string) string { return "/2020-05-31/key-value-store/" + id + "/keys" },
			wantCode: http.StatusOK,
			checkBody: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), `"k1"`)
				assert.Contains(t, rec.Body.String(), `"v1"`)
			},
		},
		{
			name:     "list_keys_empty_store",
			setup:    makeKVS,
			method:   http.MethodGet,
			pathFn:   func(id string) string { return "/2020-05-31/key-value-store/" + id + "/keys" },
			wantCode: http.StatusOK,
			checkBody: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), `"quantity":0`)
			},
		},
		{
			name:     "batch_update_puts_and_deletes",
			setup:    makeKVSWithKey("existing", "old"),
			method:   http.MethodPost,
			pathFn:   func(id string) string { return "/2020-05-31/key-value-store/" + id + "/keys" },
			body:     `{"puts":[{"key":"newkey","value":"newval"}],"deletes":["existing"]}`,
			wantCode: http.StatusOK,
			checkBody: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["eTag"])
			},
		},
		{
			name:     "kvs_not_found_returns_404",
			setup:    func(_ *testing.T, _ *cloudfront.InMemoryBackend) string { return "doesnotexist" },
			method:   http.MethodGet,
			pathFn:   func(id string) string { return "/2020-05-31/key-value-store/" + id + "/keys/key" },
			wantCode: http.StatusNotFound,
		},
		{
			name:     "put_key_etag_mismatch_returns_412",
			setup:    makeKVSWithKey("etagkey", "val"),
			method:   http.MethodPut,
			pathFn:   func(id string) string { return "/2020-05-31/key-value-store/" + id + "/keys/etagkey" },
			body:     `{"value":"newval"}`,
			header:   map[string]string{"If-Match": "wrong-etag"},
			wantCode: http.StatusPreconditionFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newAuditBackend()
			h := cloudfront.NewHandler(b)

			kvsID := tt.setup(t, b)
			path := tt.pathFn(kvsID)

			rec := doJSONReq(t, h, tt.method, path, tt.body, tt.header)
			assert.Equal(t, tt.wantCode, rec.Code, "path=%s body=%s", path, rec.Body.String())
			if tt.checkBody != nil {
				tt.checkBody(t, rec)
			}
		})
	}
}

// TestKVSBackendDataPlane tests KVS data plane backend methods directly.
func TestKVSBackendDataPlane(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(*testing.T, *cloudfront.InMemoryBackend)
	}{
		{
			name: "put_get_value",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				kvs, err := b.CreateKeyValueStore("kvs", "")
				require.NoError(t, err)
				_, err = b.PutKVSValue(kvs.ID, "key1", "val1", "")
				require.NoError(t, err)
				val, _, err := b.GetKVSValue(kvs.ID, "key1")
				require.NoError(t, err)
				assert.Equal(t, "val1", val)
			},
		},
		{
			name: "delete_value",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				kvs, err := b.CreateKeyValueStore("kvs", "")
				require.NoError(t, err)
				_, err = b.PutKVSValue(kvs.ID, "key1", "val1", "")
				require.NoError(t, err)
				_, err = b.DeleteKVSValue(kvs.ID, "key1", "")
				require.NoError(t, err)
				_, _, err = b.GetKVSValue(kvs.ID, "key1")
				require.Error(t, err)
			},
		},
		{
			name: "list_values_sorted",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				kvs, err := b.CreateKeyValueStore("kvs", "")
				require.NoError(t, err)
				_, err = b.PutKVSValue(kvs.ID, "beta", "2", "")
				require.NoError(t, err)
				_, err = b.PutKVSValue(kvs.ID, "alpha", "1", "")
				require.NoError(t, err)
				items, _, err := b.ListKVSValues(kvs.ID)
				require.NoError(t, err)
				assert.Len(t, items, 2)
				assert.Equal(t, "alpha", items[0].Key)
				assert.Equal(t, "beta", items[1].Key)
			},
		},
		{
			name: "batch_update_keys",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				kvs, err := b.CreateKeyValueStore("kvs", "")
				require.NoError(t, err)
				_, err = b.PutKVSValue(kvs.ID, "del-me", "gone", "")
				require.NoError(t, err)
				puts := []*cloudfront.KVSItem{{Key: "new", Value: "added"}}
				_, err = b.UpdateKVSValues(kvs.ID, "", puts, []string{"del-me"})
				require.NoError(t, err)
				items, _, err := b.ListKVSValues(kvs.ID)
				require.NoError(t, err)
				assert.Len(t, items, 1)
				assert.Equal(t, "new", items[0].Key)
			},
		},
		{
			name: "etag_mismatch_rejected",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				kvs, err := b.CreateKeyValueStore("kvs", "")
				require.NoError(t, err)
				_, err = b.PutKVSValue(kvs.ID, "k", "v", "wrong-etag")
				require.Error(t, err)
			},
		},
		{
			name: "etag_match_accepted",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				kvs, err := b.CreateKeyValueStore("kvs", "")
				require.NoError(t, err)
				etag1, err := b.PutKVSValue(kvs.ID, "k", "v1", "")
				require.NoError(t, err)
				_, err = b.PutKVSValue(kvs.ID, "k", "v2", etag1)
				require.NoError(t, err)
				val, _, err := b.GetKVSValue(kvs.ID, "k")
				require.NoError(t, err)
				assert.Equal(t, "v2", val)
			},
		},
		{
			name: "get_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				kvs, err := b.CreateKeyValueStore("kvs", "")
				require.NoError(t, err)
				_, _, err = b.GetKVSValue(kvs.ID, "missing")
				require.Error(t, err)
			},
		},
		{
			name: "kvs_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, _, err := b.GetKVSValue("nope", "key")
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newAuditBackend()
			tt.run(t, b)
		})
	}
}

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

			b := newAuditBackend()
			h := cloudfront.NewHandler(b)
			rec := doReq(t, h, http.MethodPost, "/2020-05-31/cache-policy", tt.body)
			assert.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), tt.wantIn)
		})
	}
}

// TestResponseHeadersPolicyConfig tests RHP creation with full config.
func TestResponseHeadersPolicyConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
		wantIn   string
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

// TestOriginRequestPolicyConfig tests ORP creation with full config.
func TestOriginRequestPolicyConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
		wantIn   string
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

// TestFunctionStatusDevelopment verifies created/updated functions use DEVELOPMENT status.
func TestFunctionStatusDevelopment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		method   string
		path     string
		body     string
		wantCode int
		wantIn   string
	}{
		{
			name:   "create_function_status_development",
			method: http.MethodPost,
			path:   "/2020-05-31/function",
			body: `<CreateFunctionRequest>
				<Name>my-function</Name>
				<FunctionConfig>
					<Runtime>cloudfront-js-2.0</Runtime>
					<Comment>test</Comment>
				</FunctionConfig>
				<FunctionCode>ZnVuY3Rpb24gaGFuZGxlcihldmVudCkge30=</FunctionCode>
			</CreateFunctionRequest>`,
			wantCode: http.StatusCreated,
			wantIn:   "DEVELOPMENT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newAuditBackend()
			h := cloudfront.NewHandler(b)
			rec := doReq(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), tt.wantIn)
		})
	}
}

// TestFunctionRuntimeValidation verifies invalid runtimes are rejected.
func TestFunctionRuntimeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name: "invalid_runtime_rejected",
			body: `<CreateFunctionRequest>
				<Name>bad-function</Name>
				<FunctionConfig>
					<Runtime>nodejs18.x</Runtime>
					<Comment>bad runtime</Comment>
				</FunctionConfig>
				<FunctionCode>ZnVuY3Rpb24gaGFuZGxlcihldmVudCkge30=</FunctionCode>
			</CreateFunctionRequest>`,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "valid_runtime_js1_accepted",
			body: `<CreateFunctionRequest>
				<Name>js1-function</Name>
				<FunctionConfig>
					<Runtime>cloudfront-js-1.0</Runtime>
					<Comment>js1</Comment>
				</FunctionConfig>
				<FunctionCode>ZnVuY3Rpb24gaGFuZGxlcihldmVudCkge30=</FunctionCode>
			</CreateFunctionRequest>`,
			wantCode: http.StatusCreated,
		},
		{
			name: "valid_runtime_js2_accepted",
			body: `<CreateFunctionRequest>
				<Name>js2-function</Name>
				<FunctionConfig>
					<Runtime>cloudfront-js-2.0</Runtime>
					<Comment>js2</Comment>
				</FunctionConfig>
				<FunctionCode>ZnVuY3Rpb24gaGFuZGxlcihldmVudCkge30=</FunctionCode>
			</CreateFunctionRequest>`,
			wantCode: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newAuditBackend()
			h := cloudfront.NewHandler(b)
			rec := doReq(t, h, http.MethodPost, "/2020-05-31/function", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
		})
	}
}

// TestInvalidationPathValidation verifies that invalid paths are rejected.
func TestInvalidationPathValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name: "valid_paths_accepted",
			body: `<InvalidationBatch>
				<CallerReference>ref-1</CallerReference>
				<Paths>
					<Quantity>2</Quantity>
					<Items>
						<Path>/images/*</Path>
						<Path>/css/main.css</Path>
					</Items>
				</Paths>
			</InvalidationBatch>`,
			wantCode: http.StatusCreated,
		},
		{
			name: "path_without_slash_rejected",
			body: `<InvalidationBatch>
				<CallerReference>ref-2</CallerReference>
				<Paths>
					<Quantity>1</Quantity>
					<Items><Path>images/no-slash</Path></Items>
				</Paths>
			</InvalidationBatch>`,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newAuditBackend()
			d, err := b.CreateDistribution("test-dist", "example.com", true, nil)
			require.NoError(t, err)

			h := cloudfront.NewHandler(b)
			rec := doReq(t, h, http.MethodPost,
				"/2020-05-31/distribution/"+d.ID+"/invalidation",
				tt.body)

			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
		})
	}
}

// TestPublicKeyPEMValidation verifies PEM-encoded public keys are validated.
func TestPublicKeyPEMValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name: "valid_rsa_key_accepted",
			body: fmt.Sprintf(`<PublicKeyConfig>
				<CallerReference>ref-1</CallerReference>
				<Name>valid-key</Name>
				<EncodedKey>%s</EncodedKey>
			</PublicKeyConfig>`, testRSA2048PublicKeyPEM),
			wantCode: http.StatusCreated,
		},
		{
			name: "invalid_pem_rejected",
			body: `<PublicKeyConfig>
				<CallerReference>ref-2</CallerReference>
				<Name>bad-key</Name>
				<EncodedKey>not-a-pem-key</EncodedKey>
			</PublicKeyConfig>`,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newAuditBackend()
			h := cloudfront.NewHandler(b)
			rec := doReq(t, h, http.MethodPost, "/2020-05-31/public-key", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
		})
	}
}

// TestSamplingRateValidation verifies that realtime log configs enforce valid sampling rates.
func TestSamplingRateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		rate     int
		wantCode int
	}{
		{name: "rate_1_accepted", rate: 1, wantCode: http.StatusCreated},
		{name: "rate_50_accepted", rate: 50, wantCode: http.StatusCreated},
		{name: "rate_100_accepted", rate: 100, wantCode: http.StatusCreated},
		{name: "rate_0_rejected", rate: 0, wantCode: http.StatusBadRequest},
		{name: "rate_101_rejected", rate: 101, wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newAuditBackend()
			h := cloudfront.NewHandler(b)

			body := fmt.Sprintf(`<RealtimeLogConfig>
				<Name>log-config-%d</Name>
				<SamplingRate>%d</SamplingRate>
				<EndPoints>
					<member>
						<StreamType>Kinesis</StreamType>
						<KinesisStreamConfig>
							<StreamARN>arn:aws:kinesis:us-east-1:123456789012:stream/test</StreamARN>
							<RoleARN>arn:aws:iam::123456789012:role/test</RoleARN>
						</KinesisStreamConfig>
					</member>
				</EndPoints>
				<Fields><member>timestamp</member></Fields>
			</RealtimeLogConfig>`, tt.rate, tt.rate)

			rec := doReq(t, h, http.MethodPost, "/2020-05-31/realtime-log-config", body)
			assert.Equal(t, tt.wantCode, rec.Code, "rate=%d body=%s", tt.rate, rec.Body.String())
		})
	}
}

// TestOAICanonicalUserID verifies that OAI S3 canonical user IDs are SHA256 hex strings.
func TestOAICanonicalUserID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(*testing.T, *cloudfront.InMemoryBackend)
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
			b := newAuditBackend()
			tt.run(t, b)
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

			b := newAuditBackend()
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

// TestKeyGroupItemValidation verifies key groups reject items that aren't valid public key IDs.
func TestKeyGroupItemValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		items    []string
		wantCode int
	}{
		{
			name:     "nonexistent_key_id_rejected",
			items:    []string{"pk-doesnotexist"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newAuditBackend()
			h := cloudfront.NewHandler(b)

			var sb strings.Builder
			for _, id := range tt.items {
				fmt.Fprintf(&sb, "<PublicKey>%s</PublicKey>", id)
			}
			itemsXML := sb.String()

			body := fmt.Sprintf(`<KeyGroupConfig>
				<Name>test-kg</Name>
				<Items>%s</Items>
			</KeyGroupConfig>`, itemsXML)

			rec := doReq(t, h, http.MethodPost, "/2020-05-31/key-group", body)
			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
		})
	}
}
