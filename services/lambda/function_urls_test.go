package lambda_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/lambda"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Function URL HTTP tests ---

func TestFunctionURL_CreateGetUpdateDelete(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	fnName := "url-fn"
	createFunctionForTest(t, h, fnName)

	// Create Function URL config
	rec := callInMemoryHandler(
		t, h, http.MethodPost,
		"/2021-10-31/functions/"+fnName+"/url",
		`{"AuthType":"NONE"}`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)
	assert.Contains(t, rec.Body.String(), "FunctionUrl")

	// Get Function URL config
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2021-10-31/functions/"+fnName+"/url", "{}")
	require.Equal(t, http.StatusOK, rec.Code)

	// List Function URL configs
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2021-10-31/functions/"+fnName+"/urls", "{}")
	require.Equal(t, http.StatusOK, rec.Code)

	// Update Function URL config
	rec = callInMemoryHandler(
		t, h, http.MethodPut,
		"/2021-10-31/functions/"+fnName+"/url",
		`{"AuthType":"AWS_IAM"}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete Function URL config
	rec = callInMemoryHandler(t, h, http.MethodDelete,
		"/2021-10-31/functions/"+fnName+"/url", "{}")
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

// ============================================================
// FunctionURL: complete coverage
// ============================================================

func TestFunctionURL_CORS(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "url-cors-fn")

	body := `{
		"AuthType":"NONE",
		"Cors":{
			"AllowOrigins":["https://example.com"],
			"AllowMethods":["GET","POST"],
			"AllowHeaders":["content-type"],
			"MaxAge":300
		}
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/url-cors-fn/url", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var cfg lambda.FunctionURLConfig
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&cfg))
	require.NotNil(t, cfg.Cors)
	assert.Equal(t, []string{"https://example.com"}, cfg.Cors.AllowOrigins)
	assert.Equal(t, 300, cfg.Cors.MaxAge)
}

func TestFunctionURL_InvokeMode(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "url-stream-fn")

	body := `{"AuthType":"NONE","InvokeMode":"RESPONSE_STREAM"}`
	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/url-stream-fn/url", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var cfg lambda.FunctionURLConfig
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&cfg))
	assert.Equal(t, "RESPONSE_STREAM", cfg.InvokeMode)
}

func TestFunctionURL_AWSIAMAuth(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "url-iam-fn")

	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/url-iam-fn/url",
		`{"AuthType":"AWS_IAM"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var cfg lambda.FunctionURLConfig
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&cfg))
	assert.Equal(t, "AWS_IAM", cfg.AuthType)
}

func TestFunctionURL_FunctionURLIsSet(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "url-set-fn")

	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/url-set-fn/url",
		`{"AuthType":"NONE"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var cfg lambda.FunctionURLConfig
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&cfg))
	assert.NotEmpty(t, cfg.FunctionURL)
}

// TestUpdateFunctionURLConfig_CORS verifies that UpdateFunctionUrlConfig
// updates CORS fields, matching AWS Lambda behaviour. Previously only AuthType was
// passed to the backend; the Cors field was silently dropped.
func TestUpdateFunctionURLConfig_CORS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		body            string
		wantAllowOrigin string
		wantAuthType    string
	}{
		{
			name:            "cors_origins_updated",
			body:            `{"AuthType":"NONE","Cors":{"AllowOrigins":["https://example.com"],"AllowMethods":["GET","POST"]}}`,
			wantAllowOrigin: "https://example.com",
			wantAuthType:    "NONE",
		},
		{
			name:            "cors_cleared_when_empty_cors",
			body:            `{"AuthType":"AWS_IAM"}`,
			wantAllowOrigin: "",
			wantAuthType:    "AWS_IAM",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			createFunctionForTest(t, h, "url-cors-fn")

			// Create URL config first.
			createBody := `{"AuthType":"NONE"}`
			rec := callInMemoryHandler(t, h, http.MethodPost,
				"/2021-10-31/functions/url-cors-fn/url", createBody)
			require.Equal(t, http.StatusCreated, rec.Code)

			// Update URL config with the test body.
			rec2 := callInMemoryHandler(t, h, http.MethodPut,
				"/2021-10-31/functions/url-cors-fn/url", tt.body)
			require.Equal(t, http.StatusOK, rec2.Code)

			var cfg lambda.FunctionURLConfig
			require.NoError(t, json.NewDecoder(rec2.Body).Decode(&cfg))

			assert.Equal(t, tt.wantAuthType, cfg.AuthType)

			if tt.wantAllowOrigin != "" {
				require.NotNil(t, cfg.Cors, "Cors must be present when origins are set")
				require.NotEmpty(t, cfg.Cors.AllowOrigins)
				assert.Equal(t, tt.wantAllowOrigin, cfg.Cors.AllowOrigins[0],
					"first AllowOrigin must match what was sent")
			}
		})
	}
}

// ============================================================
// Function URL config — Cors round-trip
// ============================================================

func TestFunctionURL_CorsAllowOrigins_RoundTrip(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "url-cors-fn")

	createRec := callInMemoryHandler(
		t, h, http.MethodPost,
		"/2015-03-31/functions/url-cors-fn/url",
		`{"AuthType":"NONE","Cors":{"AllowOrigins":["https://example.com"],"AllowMethods":["GET","POST"]}}`,
	)
	require.Equal(t, http.StatusCreated, createRec.Code)

	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/url-cors-fn/url", "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&out))
	cors, _ := out["Cors"].(map[string]any)
	require.NotNil(t, cors)
	origins, _ := cors["AllowOrigins"].([]any)
	assert.Len(t, origins, 1)
	assert.Equal(t, "https://example.com", origins[0])
}

// --- Function URL config 2021 path tests ---

func TestFunctionURLConfig_2021(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "url-fn-2021")

	// Create via 2021 path
	createRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2021-10-31/functions/url-fn-2021/url", `{"AuthType":"NONE"}`)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var cfg lambda.FunctionURLConfig
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&cfg))
	assert.Equal(t, "NONE", cfg.AuthType)

	// Get via 2021 path
	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2021-10-31/functions/url-fn-2021/url", "")
	assert.Equal(t, http.StatusOK, getRec.Code)

	// List via 2021 path
	listRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2021-10-31/functions/url-fn-2021/urls", "")
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut lambda.ListFunctionURLConfigsOutput
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	assert.Len(t, listOut.FunctionURLConfigs, 1)

	// Update via 2021 path
	updateRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2021-10-31/functions/url-fn-2021/url", `{"AuthType":"AWS_IAM"}`)
	assert.Equal(t, http.StatusOK, updateRec.Code)

	// Delete via 2021 path
	delRec := callInMemoryHandler(t, h, http.MethodDelete,
		"/2021-10-31/functions/url-fn-2021/url", "")
	assert.Equal(t, http.StatusNoContent, delRec.Code)

	// Get after delete → 404
	getRec2 := callInMemoryHandler(t, h, http.MethodGet,
		"/2021-10-31/functions/url-fn-2021/url", "")
	assert.Equal(t, http.StatusNotFound, getRec2.Code)
}

func TestFunctionURLConfig_2021_UpdateNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "fn-no-url")

	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2021-10-31/functions/fn-no-url/url", `{"AuthType":"AWS_IAM"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestFunctionURLConfig_2021_ListEmpty(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "fn-no-url-empty")

	// List for a function without a URL config returns empty list (not 404)
	listRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2021-10-31/functions/fn-no-url-empty/urls", "")
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut lambda.ListFunctionURLConfigsOutput
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	assert.Empty(t, listOut.FunctionURLConfigs)
}

func TestLambda_FunctionURL_CORSPreflight(t *testing.T) {
	t.Parallel()

	backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
	closeBackend(t, backend)

	lambda.SetFunctionURLConfigForTest(backend, "cors-fn", &lambda.FunctionURLConfig{
		AuthType: "NONE",
		Cors: &lambda.FunctionURLCors{
			AllowOrigins: []string{"https://example.com"},
			AllowMethods: []string{"GET", "POST"},
			AllowHeaders: []string{"content-type"},
			MaxAge:       600,
		},
	})

	handler := lambda.BuildFunctionURLHandler(backend, "cors-fn")

	req := httptest.NewRequest(http.MethodOptions, "/", nil)
	req.Header.Set("Origin", "https://example.com")
	rec := httptest.NewRecorder()
	handler(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "https://example.com", rec.Header().Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "GET,POST", rec.Header().Get("Access-Control-Allow-Methods"))
	assert.Equal(t, "content-type", rec.Header().Get("Access-Control-Allow-Headers"))
	assert.Equal(t, "600", rec.Header().Get("Access-Control-Max-Age"))
}

func TestLambda_FunctionURL_AWSIAMRejectsUnsigned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		authValue string
	}{
		{name: "missing signature", authValue: ""},
		{
			name:      "malformed signature",
			authValue: "AWS4-HMAC-SHA256 Credential=bad, SignedHeaders=host, Signature=deadbeef",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
			closeBackend(t, backend)

			lambda.SetFunctionURLConfigForTest(backend, "iam-fn", &lambda.FunctionURLConfig{AuthType: "AWS_IAM"})

			handler := lambda.BuildFunctionURLHandler(backend, "iam-fn")

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.authValue != "" {
				req.Header.Set("Authorization", tt.authValue)
			}

			rec := httptest.NewRecorder()
			handler(rec, req)

			assert.Equal(t, http.StatusForbidden, rec.Code)
			assert.Contains(t, rec.Body.String(), "Forbidden")
		})
	}
}

func TestLambda_FunctionURL_EventPayloadEnrichment(t *testing.T) {
	t.Parallel()

	backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
	closeBackend(t, backend)

	req := httptest.NewRequest(http.MethodGet, "/path?foo=bar&baz=qux", nil)
	req.Header.Set("Cookie", "session=abc; theme=dark")

	payload, err := lambda.BuildURLEventPayload(backend, req)
	require.NoError(t, err)

	var event struct {
		QueryStringParameters map[string]string `json:"queryStringParameters"`
		Headers               map[string]string `json:"headers"`
		Cookies               []string          `json:"cookies"`
	}
	require.NoError(t, json.Unmarshal(payload, &event))

	assert.ElementsMatch(t, []string{"session=abc", "theme=dark"}, event.Cookies)
	assert.Equal(t, "bar", event.QueryStringParameters["foo"])
	assert.Equal(t, "qux", event.QueryStringParameters["baz"])
	// Cookie header is omitted from headers (surfaced via cookies instead).
	assert.NotContains(t, event.Headers, "cookie")
}
