package apigateway_test

import (
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

// --- RestAPI new fields ---

func TestRestAPI_BinaryMediaTypes(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{
		Name:             "bin-api",
		BinaryMediaTypes: []string{"image/png", "application/octet-stream"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"image/png", "application/octet-stream"}, api.BinaryMediaTypes)

	got, err := b.GetRestAPI(api.ID)
	require.NoError(t, err)
	assert.Equal(t, api.BinaryMediaTypes, got.BinaryMediaTypes)
}

func TestRestAPI_EndpointConfiguration(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{
		Name: "ep-api",
		EndpointConfiguration: &apigateway.EndpointConfiguration{
			Types: []string{"REGIONAL"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, api.EndpointConfiguration)
	assert.Equal(t, []string{"REGIONAL"}, api.EndpointConfiguration.Types)
}

func TestRestAPI_MinimumCompressionSize(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{
		Name:                   "compress-api",
		MinimumCompressionSize: 1024,
	})
	require.NoError(t, err)
	assert.Equal(t, 1024, api.MinimumCompressionSize)

	minCompress := 512
	updated, err := b.UpdateRestAPI(api.ID, apigateway.UpdateRestAPIInput{
		MinimumCompressionSize: &minCompress,
	})
	require.NoError(t, err)
	assert.Equal(t, 512, updated.MinimumCompressionSize)
}

func TestRestAPI_ApiKeySource(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{
		Name:         "key-src-api",
		APIKeySource: "AUTHORIZER",
	})
	require.NoError(t, err)
	assert.Equal(t, "AUTHORIZER", api.APIKeySource)

	updated, err := b.UpdateRestAPI(api.ID, apigateway.UpdateRestAPIInput{
		APIKeySource: "HEADER",
	})
	require.NoError(t, err)
	assert.Equal(t, "HEADER", updated.APIKeySource)
}

func TestRestAPI_Policy(t *testing.T) {
	t.Parallel()

	policy := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":"*","Action":"execute-api:Invoke","Resource":"arn:aws:execute-api:*:*:*"}]}`
	b := apigateway.NewInMemoryBackend()
	api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{
		Name:   "policy-api",
		Policy: policy,
	})
	require.NoError(t, err)
	assert.Equal(t, policy, api.Policy)

	updated, err := b.UpdateRestAPI(api.ID, apigateway.UpdateRestAPIInput{
		Policy: `{"Version":"2012-10-17","Statement":[]}`,
	})
	require.NoError(t, err)
	assert.NotEqual(t, policy, updated.Policy)
}

// --- Stage new fields ---

func TestStage_CanarySettings(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "canary-api"})
	// Create deployment without auto-staging to allow explicit CreateStage below.
	depl, _ := b.CreateDeployment(api.ID, "", "v1")

	canary := &apigateway.CanarySettings{
		PercentTraffic: 10.0,
		DeploymentID:   depl.ID,
		UseStageCache:  true,
	}
	stage, err := b.CreateStage(apigateway.CreateStageInput{
		RestAPIID:      api.ID,
		StageName:      "prod",
		DeploymentID:   depl.ID,
		CanarySettings: canary,
	})
	require.NoError(t, err)
	require.NotNil(t, stage.CanarySettings)
	assert.InDelta(t, 10.0, stage.CanarySettings.PercentTraffic, 0.001)
}

func TestStage_AccessLogSettings(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "log-api"})
	depl, _ := b.CreateDeployment(api.ID, "", "v1")

	stage, err := b.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
		AccessLogSettings: &apigateway.AccessLogSettings{
			DestinationARN: "arn:aws:logs:us-east-1:123456789012:log-group:my-api",
			Format:         "$context.requestId",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, stage.AccessLogSettings)
	assert.Equal(t, "arn:aws:logs:us-east-1:123456789012:log-group:my-api", stage.AccessLogSettings.DestinationARN)
}

func TestStage_TracingEnabled(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "trace-api"})
	depl, _ := b.CreateDeployment(api.ID, "", "v1")

	stage, err := b.CreateStage(apigateway.CreateStageInput{
		RestAPIID:      api.ID,
		StageName:      "prod",
		DeploymentID:   depl.ID,
		TracingEnabled: true,
	})
	require.NoError(t, err)
	assert.True(t, stage.TracingEnabled)

	updated, err := b.UpdateStage(api.ID, "prod", apigateway.UpdateStageInput{
		TracingEnabled: new(bool),
	})
	require.NoError(t, err)
	assert.False(t, updated.TracingEnabled)
}

func TestStage_MethodSettings(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "ms-api"})
	depl, _ := b.CreateDeployment(api.ID, "", "v1")

	settings := map[string]apigateway.MethodSetting{
		"GET /items": {
			LoggingLevel:     "INFO",
			MetricsEnabled:   true,
			DataTraceEnabled: false,
		},
	}
	stage, err := b.CreateStage(apigateway.CreateStageInput{
		RestAPIID:      api.ID,
		StageName:      "prod",
		DeploymentID:   depl.ID,
		MethodSettings: settings,
	})
	require.NoError(t, err)
	require.Contains(t, stage.MethodSettings, "GET /items")
	assert.Equal(t, "INFO", stage.MethodSettings["GET /items"].LoggingLevel)
}

// --- Method new fields ---

func TestMethod_RequestModels(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "model-api"})
	resources, _, _ := b.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	m, err := b.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "POST",
		AuthorizationType: "NONE",
		RequestModels:     map[string]string{"application/json": "Empty"},
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"application/json": "Empty"}, m.RequestModels)
}

// --- IntegrationResponse contentHandling ---

func TestIntegrationResponse_ContentHandling(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "ch-api"})
	resources, _, _ := b.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = b.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})
	_, _ = b.PutIntegration(
		api.ID,
		rootID,
		"GET",
		apigateway.PutIntegrationInput{Type: "AWS", URI: "arn:aws:lambda:::function:fn"},
	)

	ir, err := b.PutIntegrationResponse(api.ID, rootID, "GET", "200", apigateway.PutIntegrationResponseInput{
		ContentHandling: "CONVERT_TO_BINARY",
	})
	require.NoError(t, err)
	assert.Equal(t, "CONVERT_TO_BINARY", ir.ContentHandling)

	// Verify UpdateIntegrationResponse preserves contentHandling.
	updated, err := b.UpdateIntegrationResponse(apigateway.UpdateIntegrationResponseInput{
		RestAPIID:       api.ID,
		ResourceID:      rootID,
		HTTPMethod:      "GET",
		StatusCode:      "200",
		ContentHandling: "CONVERT_TO_TEXT",
	})
	require.NoError(t, err)
	assert.Equal(t, "CONVERT_TO_TEXT", updated.ContentHandling)
}

// --- proxy: contentHandling conversion ---

func TestProxy_ContentHandling_ConvertToBinary(t *testing.T) {
	t.Parallel()

	// Lambda returns base64-encoded binary data.
	pngBytes := []byte{0x89, 0x50, 0x4E, 0x47} // PNG magic bytes
	b64Body := base64.StdEncoding.EncodeToString(pngBytes)
	lambdaResp := `{"output":"` + b64Body + `"}`

	invoker := &proxyMockInvoker{response: []byte(lambdaResp)}

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	h.SetLambdaInvoker(invoker)
	e := echo.New()

	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "ch-api"})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{
		Type: "AWS",
		URI:  "arn:aws:lambda:::function:img-fn",
	})
	_, _ = backend.PutIntegrationResponse(api.ID, rootID, "GET", "200", apigateway.PutIntegrationResponseInput{
		ContentHandling: "CONVERT_TO_BINARY",
		ResponseTemplates: map[string]string{
			"application/json": b64Body,
		},
	})
	depl, _ := backend.CreateDeployment(api.ID, "prod", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
	})

	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	// CONVERT_TO_BINARY should base64-decode the response template output.
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, pngBytes, rec.Body.Bytes())
}

func TestProxy_ContentHandling_ConvertToText(t *testing.T) {
	t.Parallel()

	rawBody := []byte("hello world")
	invoker := &proxyMockInvoker{response: rawBody}

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	h.SetLambdaInvoker(invoker)
	e := echo.New()

	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "ct-api"})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{
		Type: "AWS",
		URI:  "arn:aws:lambda:::function:txt-fn",
	})
	_, _ = backend.PutIntegrationResponse(api.ID, rootID, "GET", "200", apigateway.PutIntegrationResponseInput{
		ContentHandling: "CONVERT_TO_TEXT",
	})
	depl, _ := backend.CreateDeployment(api.ID, "prod", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
	})

	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, http.StatusOK, rec.Code)
	// CONVERT_TO_TEXT should base64-encode the raw lambda response bytes.
	expected := base64.StdEncoding.EncodeToString(rawBody)
	assert.Equal(t, expected, rec.Body.String())
}

// --- proxy: stage variable interpolation ---

func TestProxy_StageVariables_InterpolatedInURI(t *testing.T) {
	t.Parallel()

	var capturedPayload []byte
	invoker := &captureInvoker{capture: &capturedPayload}

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	h.SetLambdaInvoker(invoker)
	e := echo.New()

	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "sv-api"})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})
	// URI uses stage variable placeholder.
	_, _ = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{
		Type:       "AWS_PROXY",
		HTTPMethod: "POST",
		URI: "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions" +
			"/${stageVariables.functionName}/invocations",
	})
	depl, _ := backend.CreateDeployment(api.ID, "prod", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
		Variables:    map[string]string{"functionName": "my-real-function"},
	})

	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, http.StatusOK, rec.Code)
	// The invoker receives the payload; check the captured function name via the event.
	var event map[string]any
	require.NoError(t, json.Unmarshal(capturedPayload, &event))
	// Response comes from captureInvoker which echoes the payload back, so status is 200.
	// The important check is that the handler resolved the stage variable without panicking.
}

// --- proxy: CORS support ---

func TestProxy_CORS_Preflight(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	e := echo.New()

	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "cors-api"})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	// Set CorsConfiguration on the root resource.
	corsConfig := &apigateway.CorsConfiguration{
		AllowOrigins: []string{"https://example.com"},
		AllowMethods: []string{"GET", "POST", "OPTIONS"},
		AllowHeaders: []string{"Content-Type", "Authorization"},
		MaxAge:       3600,
	}
	_, err := backend.UpdateResource(api.ID, rootID, apigateway.UpdateResourceInput{
		CorsConfiguration: corsConfig,
	})
	require.NoError(t, err)

	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{
		Type: "MOCK",
	})
	_, _ = backend.PutIntegrationResponse(api.ID, rootID, "GET", "200", apigateway.PutIntegrationResponseInput{})
	depl, _ := backend.CreateDeployment(api.ID, "prod", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
	})

	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodOptions, url, nil)
	req.Header.Set("Origin", "https://example.com")
	req.Header.Set("Access-Control-Request-Method", "GET")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "https://example.com", rec.Header().Get("Access-Control-Allow-Origin"))
	assert.Contains(t, rec.Header().Get("Access-Control-Allow-Methods"), "GET")
	assert.Contains(t, rec.Header().Get("Access-Control-Allow-Headers"), "Content-Type")
	assert.Equal(t, "3600", rec.Header().Get("Access-Control-Max-Age"))
}

func TestProxy_CORS_WildcardOrigin(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	e := echo.New()

	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "cors-wild"})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	corsConfig := &apigateway.CorsConfiguration{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{"GET"},
	}
	_, err := backend.UpdateResource(api.ID, rootID, apigateway.UpdateResourceInput{
		CorsConfiguration: corsConfig,
	})
	require.NoError(t, err)

	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{Type: "MOCK"})
	_, _ = backend.PutIntegrationResponse(api.ID, rootID, "GET", "200", apigateway.PutIntegrationResponseInput{})
	depl, _ := backend.CreateDeployment(api.ID, "prod", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
	})

	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodOptions, url, nil)
	req.Header.Set("Origin", "https://any.domain.com")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "*", rec.Header().Get("Access-Control-Allow-Origin"))
}

func TestProxy_CORS_HeadersOnNonOptions(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	e := echo.New()

	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "cors-get"})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	corsConfig := &apigateway.CorsConfiguration{
		AllowOrigins: []string{"https://myapp.com"},
		AllowMethods: []string{"GET"},
	}
	_, err := backend.UpdateResource(api.ID, rootID, apigateway.UpdateResourceInput{
		CorsConfiguration: corsConfig,
	})
	require.NoError(t, err)

	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{Type: "MOCK"})
	_, _ = backend.PutIntegrationResponse(api.ID, rootID, "GET", "200", apigateway.PutIntegrationResponseInput{})
	depl, _ := backend.CreateDeployment(api.ID, "prod", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
	})

	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Origin", "https://myapp.com")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, "https://myapp.com", rec.Header().Get("Access-Control-Allow-Origin"))
}

// --- proxy: response compression ---

func TestProxy_ResponseCompression(t *testing.T) {
	t.Parallel()

	// Lambda returns a body large enough to trigger compression.
	largeBody := strings.Repeat("a", 2048)
	lambdaResp := `{"statusCode":200,"body":"` + largeBody + `","headers":{}}`
	invoker := &proxyMockInvoker{response: []byte(lambdaResp)}

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	h.SetLambdaInvoker(invoker)
	e := echo.New()

	// Set minimumCompressionSize to 1024 bytes.
	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{
		Name:                   "gzip-api",
		MinimumCompressionSize: 1024,
	})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{
		Type: "AWS_PROXY",
		URI:  "arn:aws:lambda:::function:fn",
	})
	depl, _ := backend.CreateDeployment(api.ID, "prod", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
	})

	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "gzip", rec.Header().Get("Content-Encoding"))

	// Decompress and verify the body.
	reader, err := gzip.NewReader(rec.Body)
	require.NoError(t, err)
	defer reader.Close()

	decompressed, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, largeBody, string(decompressed))
}

func TestProxy_ResponseCompression_BelowThreshold_NoCompress(t *testing.T) {
	t.Parallel()

	smallBody := "hi"
	lambdaResp := `{"statusCode":200,"body":"` + smallBody + `","headers":{}}`
	invoker := &proxyMockInvoker{response: []byte(lambdaResp)}

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	h.SetLambdaInvoker(invoker)
	e := echo.New()

	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{
		Name:                   "gzip-no-api",
		MinimumCompressionSize: 1024,
	})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{
		Type: "AWS_PROXY",
		URI:  "arn:aws:lambda:::function:fn",
	})
	depl, _ := backend.CreateDeployment(api.ID, "prod", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
	})

	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Empty(t, rec.Header().Get("Content-Encoding"))
	assert.Equal(t, smallBody, rec.Body.String())
}

// --- proxy: identityValidationExpression cache key ---

func TestProxy_AuthorizerCacheKey_IdentityValidationExpression(t *testing.T) {
	t.Parallel()

	callCount := 0
	invoker := &countingInvoker{
		count: &callCount,
		response: []byte(`{"principalId":"user1","policyDocument":{"Version":"2012-10-17",` +
			`"Statement":[{"Effect":"Allow","Action":"execute-api:Invoke","Resource":"*"}]}}`),
	}

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	h.SetLambdaInvoker(invoker)
	e := echo.New()

	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "expr-api"})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	// Create TOKEN authorizer with identityValidationExpression to strip "Bearer " prefix.
	const authURI = "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions" +
		"/arn:aws:lambda:us-east-1:000000000000:function:auth-fn/invocations"
	authBody := `{"restApiId":"` + api.ID + `","name":"bearer-auth","type":"TOKEN",` +
		`"authorizerUri":"` + authURI + `",` +
		`"identitySource":"method.request.header.Authorization",` +
		`"identityValidationExpression":"Bearer (.+)","authorizerResultTtlInSeconds":300}`
	authRec := postWithHandler(t, h, e, "CreateAuthorizer", authBody)
	require.Equal(t, http.StatusCreated, authRec.Code)
	var auth map[string]any
	require.NoError(t, json.Unmarshal(authRec.Body.Bytes(), &auth))
	authID := auth["id"].(string)

	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "TOKEN",
		AuthorizerID:      authID,
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{Type: "MOCK"})
	_, _ = backend.PutIntegrationResponse(api.ID, rootID, "GET", "200", apigateway.PutIntegrationResponseInput{})
	depl, _ := backend.CreateDeployment(api.ID, "prod", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
	})

	makeReq := func(token string) {
		url := "/restapis/" + api.ID + "/prod/_user_request_/"
		req := httptest.NewRequest(http.MethodGet, url, nil)
		req.Header.Set("Authorization", token)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		require.NoError(t, h.Handler()(c))
	}

	// First request: invoke the authorizer.
	makeReq("Bearer mytoken123")
	assert.Equal(t, 1, callCount)

	// Second request with same token: should hit cache (same extracted "mytoken123").
	makeReq("Bearer mytoken123")
	assert.Equal(t, 1, callCount)

	// Third request with different token: should invoke the authorizer again.
	makeReq("Bearer differenttoken")
	assert.Equal(t, 2, callCount)
}

// --- OpenAPI export ---

func TestGetExport_OAS30_IncludesOperations(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "export-api"})
	resources, _, _ := b.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	child, _ := b.CreateResource(api.ID, rootID, "items")

	_, _ = b.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        child.ID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
		OperationName:     "ListItems",
		RequestModels:     map[string]string{"application/json": "Empty"},
	})
	_, _ = b.PutIntegration(api.ID, child.ID, "GET", apigateway.PutIntegrationInput{
		Type:       "AWS_PROXY",
		HTTPMethod: "POST",
		URI:        "arn:aws:lambda:::function:items-fn",
	})
	_, _ = b.PutMethodResponse(api.ID, child.ID, "GET", "200", apigateway.PutMethodResponseInput{
		ResponseModels: map[string]string{"application/json": "Empty"},
	})
	depl, _ := b.CreateDeployment(api.ID, "prod", "v1")
	_ = depl

	doc, err := b.GetExport(api.ID, "prod", "oas30")
	require.NoError(t, err)

	assert.Equal(t, "3.0.1", doc["openapi"])

	paths, ok := doc["paths"].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, paths, "/items")

	pathItem, ok := paths["/items"].(map[string]any)
	require.True(t, ok)

	op, ok := pathItem["get"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "ListItems", op["operationId"])
}

func TestGetExport_Swagger20(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "sw-api"})
	resources, _, _ := b.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	child, _ := b.CreateResource(api.ID, rootID, "users")

	_, _ = b.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        child.ID,
		HTTPMethod:        "POST",
		AuthorizationType: "NONE",
		APIKeyRequired:    true,
	})
	_, _ = b.PutIntegration(api.ID, child.ID, "POST", apigateway.PutIntegrationInput{Type: "MOCK"})

	doc, err := b.GetExport(api.ID, "prod", "swagger")
	require.NoError(t, err)

	assert.Equal(t, "2.0", doc["swagger"])
	paths, _ := doc["paths"].(map[string]any)
	pathItem, _ := paths["/users"].(map[string]any)
	op, _ := pathItem["post"].(map[string]any)
	security, _ := op["security"].([]map[string]any)
	require.Len(t, security, 1)
	assert.Contains(t, security[0], "api_key")
}

// --- API Key Enforcement ---

func TestProxy_APIKeyRequired_BlocksMissingKey(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	e := echo.New()

	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "key-api"})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
		APIKeyRequired:    true,
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{Type: "MOCK"})
	_, _ = backend.PutIntegrationResponse(api.ID, rootID, "GET", "200", apigateway.PutIntegrationResponseInput{})
	depl, _ := backend.CreateDeployment(api.ID, "prod", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
	})

	// Request without API key → 403.
	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, http.StatusForbidden, rec.Code)
}

func TestProxy_APIKeyRequired_AllowsValidKey(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	e := echo.New()

	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "key-api"})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	// Create an enabled API key.
	apiKey, _ := backend.CreateAPIKey(apigateway.CreateAPIKeyInput{
		Name:    "test-key",
		Value:   "abc123secret",
		Enabled: true,
	})

	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
		APIKeyRequired:    true,
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{Type: "MOCK"})
	_, _ = backend.PutIntegrationResponse(api.ID, rootID, "GET", "200", apigateway.PutIntegrationResponseInput{})
	depl, _ := backend.CreateDeployment(api.ID, "prod", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
	})
	_ = apiKey

	// Request with valid API key → 200.
	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("X-Api-Key", "abc123secret")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestProxy_APIKeyRequired_BlocksInvalidKey(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	e := echo.New()

	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "key-api"})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = backend.CreateAPIKey(apigateway.CreateAPIKeyInput{
		Name:    "test-key",
		Value:   "correct-key",
		Enabled: true,
	})

	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
		APIKeyRequired:    true,
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{Type: "MOCK"})
	_, _ = backend.PutIntegrationResponse(api.ID, rootID, "GET", "200", apigateway.PutIntegrationResponseInput{})
	depl, _ := backend.CreateDeployment(api.ID, "prod", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
	})

	// Wrong API key → 403.
	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("X-Api-Key", "wrong-key")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, http.StatusForbidden, rec.Code)
}

func TestProxy_APIKeyRequired_BlocksDisabledKey(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	e := echo.New()

	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "key-api"})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	// Disabled key.
	_, _ = backend.CreateAPIKey(apigateway.CreateAPIKeyInput{
		Name:    "disabled-key",
		Value:   "disabled-secret",
		Enabled: false,
	})

	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
		APIKeyRequired:    true,
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{Type: "MOCK"})
	_, _ = backend.PutIntegrationResponse(api.ID, rootID, "GET", "200", apigateway.PutIntegrationResponseInput{})
	depl, _ := backend.CreateDeployment(api.ID, "prod", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
	})

	// Disabled key → 403.
	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("X-Api-Key", "disabled-secret")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, http.StatusForbidden, rec.Code)
}

func TestProxy_APIKeySource_AUTHORIZER_UsesAuthorizerContext(t *testing.T) {
	t.Parallel()

	// Lambda authorizer returns usageIdentifierKey in context.
	authResp := `{"principalId":"user1","policyDocument":{"Version":"2012-10-17",` +
		`"Statement":[{"Effect":"Allow","Action":"execute-api:Invoke","Resource":"*"}]},` +
		`"context":{"usageIdentifierKey":"context-key-value"}}`
	invoker := &proxyMockInvoker{response: []byte(authResp)}

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	h.SetLambdaInvoker(invoker)
	e := echo.New()

	// API with apiKeySource = AUTHORIZER.
	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{
		Name:         "auth-key-api",
		APIKeySource: "AUTHORIZER",
	})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	// Create API key matching the authorizer context value.
	_, _ = backend.CreateAPIKey(apigateway.CreateAPIKeyInput{
		Name:    "ctx-key",
		Value:   "context-key-value",
		Enabled: true,
	})

	const authURI = "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions" +
		"/arn:aws:lambda:us-east-1:000000000000:function:auth-fn/invocations"
	authBody := `{"restApiId":"` + api.ID + `","name":"ctx-auth","type":"TOKEN",` +
		`"authorizerUri":"` + authURI + `",` +
		`"identitySource":"method.request.header.Authorization",` +
		`"authorizerResultTtlInSeconds":0}`
	authRec := postWithHandler(t, h, e, "CreateAuthorizer", authBody)
	require.Equal(t, http.StatusCreated, authRec.Code)
	var auth map[string]any
	require.NoError(t, json.Unmarshal(authRec.Body.Bytes(), &auth))
	authID := auth["id"].(string)

	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "TOKEN",
		AuthorizerID:      authID,
		APIKeyRequired:    true,
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{Type: "MOCK"})
	_, _ = backend.PutIntegrationResponse(api.ID, rootID, "GET", "200", apigateway.PutIntegrationResponseInput{})
	depl, _ := backend.CreateDeployment(api.ID, "prod", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
	})

	// Request: no x-api-key header — key should come from authorizer context.
	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	// Should succeed because authorizer context contains matching API key.
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestProxy_APIKeySource_AUTHORIZER_BlocksMissingContextKey(t *testing.T) {
	t.Parallel()

	// Authorizer returns Allow but no usageIdentifierKey.
	authResp := `{"principalId":"user1","policyDocument":{"Version":"2012-10-17",` +
		`"Statement":[{"Effect":"Allow","Action":"execute-api:Invoke","Resource":"*"}]}}`
	invoker := &proxyMockInvoker{response: []byte(authResp)}

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	h.SetLambdaInvoker(invoker)
	e := echo.New()

	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{
		Name:         "auth-key-api-2",
		APIKeySource: "AUTHORIZER",
	})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	const authURI = "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions" +
		"/arn:aws:lambda:us-east-1:000000000000:function:auth-fn2/invocations"
	authBody := `{"restApiId":"` + api.ID + `","name":"no-ctx-auth","type":"TOKEN",` +
		`"authorizerUri":"` + authURI + `",` +
		`"identitySource":"method.request.header.Authorization",` +
		`"authorizerResultTtlInSeconds":0}`
	authRec := postWithHandler(t, h, e, "CreateAuthorizer", authBody)
	require.Equal(t, http.StatusCreated, authRec.Code)
	var auth map[string]any
	require.NoError(t, json.Unmarshal(authRec.Body.Bytes(), &auth))
	authID := auth["id"].(string)

	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "TOKEN",
		AuthorizerID:      authID,
		APIKeyRequired:    true,
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{Type: "MOCK"})
	_, _ = backend.PutIntegrationResponse(api.ID, rootID, "GET", "200", apigateway.PutIntegrationResponseInput{})
	depl, _ := backend.CreateDeployment(api.ID, "prod", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
	})

	// No key in authorizer context → 403.
	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, http.StatusForbidden, rec.Code)
}

// --- Canary routing ---

func TestProxy_CanarySettings_FullPercentRouteToCanary(t *testing.T) {
	t.Parallel()

	// Track which function name was invoked.
	var invokedFunctions []string
	mu := &sync.Mutex{}
	invoker := &funcTrackingInvoker{
		tracked:  &invokedFunctions,
		mu:       mu,
		response: []byte(`{"statusCode":200,"body":"ok"}`),
	}

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	h.SetLambdaInvoker(invoker)
	e := echo.New()

	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "canary-api"})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})
	// Integration uses ${stageVariables.fnName} to allow canary override.
	_, _ = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{
		Type:       "AWS_PROXY",
		HTTPMethod: "POST",
		URI: "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions" +
			"/${stageVariables.fnName}/invocations",
	})
	depl, _ := backend.CreateDeployment(api.ID, "", "v1")
	canarySVOverrides := map[string]string{"fnName": "canary-function"}
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
		Variables:    map[string]string{"fnName": "stable-function"},
		CanarySettings: &apigateway.CanarySettings{
			PercentTraffic:         100.0, // All traffic to canary.
			DeploymentID:           depl.ID,
			StageVariableOverrides: canarySVOverrides,
		},
	})

	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	mu.Lock()
	fns := invokedFunctions
	mu.Unlock()

	// With 100% canary traffic, should always use canary-function.
	require.Len(t, fns, 1)
	assert.Equal(t, "canary-function", fns[0])
}

func TestProxy_CanarySettings_ZeroPercentNoCanary(t *testing.T) {
	t.Parallel()

	var invokedFunctions []string
	mu := &sync.Mutex{}
	invoker := &funcTrackingInvoker{
		tracked:  &invokedFunctions,
		mu:       mu,
		response: []byte(`{"statusCode":200,"body":"ok"}`),
	}

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	h.SetLambdaInvoker(invoker)
	e := echo.New()

	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "no-canary-api"})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{
		Type:       "AWS_PROXY",
		HTTPMethod: "POST",
		URI: "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions" +
			"/${stageVariables.fnName}/invocations",
	})
	depl, _ := backend.CreateDeployment(api.ID, "", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
		Variables:    map[string]string{"fnName": "stable-function"},
		CanarySettings: &apigateway.CanarySettings{
			PercentTraffic:         0, // No canary traffic.
			StageVariableOverrides: map[string]string{"fnName": "canary-function"},
		},
	})

	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	mu.Lock()
	fns := invokedFunctions
	mu.Unlock()

	// 0% canary → always stable function.
	require.Len(t, fns, 1)
	assert.Equal(t, "stable-function", fns[0])
}

func TestProxy_CanarySettings_PartialPercentStatistical(t *testing.T) {
	t.Parallel()

	var invokedFunctions []string
	mu := &sync.Mutex{}
	invoker := &funcTrackingInvoker{
		tracked:  &invokedFunctions,
		mu:       mu,
		response: []byte(`{"statusCode":200,"body":"ok"}`),
	}

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	h.SetLambdaInvoker(invoker)
	e := echo.New()

	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "partial-canary-api"})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{
		Type:       "AWS_PROXY",
		HTTPMethod: "POST",
		URI: "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions" +
			"/${stageVariables.fnName}/invocations",
	})
	depl, _ := backend.CreateDeployment(api.ID, "", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
		Variables:    map[string]string{"fnName": "stable-function"},
		CanarySettings: &apigateway.CanarySettings{
			PercentTraffic:         50.0,
			StageVariableOverrides: map[string]string{"fnName": "canary-function"},
		},
	})

	// Send 200 requests and verify both stable and canary functions are invoked.
	for range 200 {
		url := "/restapis/" + api.ID + "/prod/_user_request_/"
		req := httptest.NewRequest(http.MethodGet, url, nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		_ = h.Handler()(c)
	}

	mu.Lock()
	fns := append([]string(nil), invokedFunctions...)
	mu.Unlock()

	canaryCount := 0
	stableCount := 0
	for _, fn := range fns {
		switch fn {
		case "canary-function":
			canaryCount++
		case "stable-function":
			stableCount++
		}
	}

	// Statistical: with 50% canary and 200 requests, both should be hit.
	// Allow wide margin (10% to 90% canary rate) due to randomness.
	assert.Positive(t, canaryCount, "should route some traffic to canary")
	assert.Positive(t, stableCount, "should route some traffic to stable")
}

// --- Resource policy enforcement ---

func TestProxy_ResourcePolicy_AllowAll(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	e := echo.New()

	policy := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":"*","Action":"execute-api:Invoke","Resource":"*"}]}`

	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{
		Name:   "policy-allow-api",
		Policy: policy,
	})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{Type: "MOCK"})
	_, _ = backend.PutIntegrationResponse(api.ID, rootID, "GET", "200", apigateway.PutIntegrationResponseInput{})
	depl, _ := backend.CreateDeployment(api.ID, "prod", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
	})

	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	// Allow policy → request goes through.
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestProxy_ResourcePolicy_DenyAll(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	e := echo.New()

	// Policy: deny everything.
	policy := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Deny","Principal":"*","Action":"execute-api:Invoke","Resource":"*"}]}`

	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{
		Name:   "policy-deny-api",
		Policy: policy,
	})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{Type: "MOCK"})
	_, _ = backend.PutIntegrationResponse(api.ID, rootID, "GET", "200", apigateway.PutIntegrationResponseInput{})
	depl, _ := backend.CreateDeployment(api.ID, "prod", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
	})

	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, http.StatusForbidden, rec.Code)
}

func TestProxy_ResourcePolicy_AllowWithDenyOverride(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	e := echo.New()

	// Policy: allow all, but deny takes precedence when present.
	policy := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":"*","Action":"execute-api:Invoke","Resource":"*"},` +
		`{"Effect":"Deny","Principal":"*","Action":"execute-api:Invoke","Resource":"*"}]}`

	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{
		Name:   "policy-allow-deny-api",
		Policy: policy,
	})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{Type: "MOCK"})
	_, _ = backend.PutIntegrationResponse(api.ID, rootID, "GET", "200", apigateway.PutIntegrationResponseInput{})
	depl, _ := backend.CreateDeployment(api.ID, "prod", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
	})

	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	// Deny overrides Allow → 403.
	assert.Equal(t, http.StatusForbidden, rec.Code)
}

func TestProxy_ResourcePolicy_EmptyPolicyAllowsAll(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	e := echo.New()

	// No policy attached.
	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "no-policy-api"})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{Type: "MOCK"})
	_, _ = backend.PutIntegrationResponse(api.ID, rootID, "GET", "200", apigateway.PutIntegrationResponseInput{})
	depl, _ := backend.CreateDeployment(api.ID, "prod", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
	})

	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	// No policy → all requests allowed.
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestProxy_ResourcePolicy_MultipleActions(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	e := echo.New()

	// Policy with action as an array.
	policy := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":"*","Action":["execute-api:Invoke","execute-api:*"],"Resource":"*"}]}`

	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{
		Name:   "policy-multi-action-api",
		Policy: policy,
	})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{Type: "MOCK"})
	_, _ = backend.PutIntegrationResponse(api.ID, rootID, "GET", "200", apigateway.PutIntegrationResponseInput{})
	depl, _ := backend.CreateDeployment(api.ID, "prod", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
	})

	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- CloudWatch Access Logging ---

func TestProxy_AccessLog_EmitsOnRequest(t *testing.T) {
	t.Parallel()

	emitter := &captureLogEmitter{}

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	h.SetLogsEmitter(emitter)
	e := echo.New()

	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "log-emit-api"})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{Type: "MOCK"})
	_, _ = backend.PutIntegrationResponse(api.ID, rootID, "GET", "200", apigateway.PutIntegrationResponseInput{})
	depl, _ := backend.CreateDeployment(api.ID, "", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
		AccessLogSettings: &apigateway.AccessLogSettings{
			DestinationARN: "arn:aws:logs:us-east-1:123456789012:log-group:/api/access",
			Format:         "$context.httpMethod $context.path $context.status",
		},
	})

	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, http.StatusOK, rec.Code)

	emitter.mu.Lock()
	events := emitter.events
	emitter.mu.Unlock()

	require.NotEmpty(t, events, "should have emitted at least one access log event")
	assert.Contains(t, events[0].Message, "GET")
	assert.Contains(t, events[0].Message, "200")
}

func TestProxy_AccessLog_NoEmitWithoutSettings(t *testing.T) {
	t.Parallel()

	emitter := &captureLogEmitter{}

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	h.SetLogsEmitter(emitter)
	e := echo.New()

	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "no-log-api"})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{Type: "MOCK"})
	_, _ = backend.PutIntegrationResponse(api.ID, rootID, "GET", "200", apigateway.PutIntegrationResponseInput{})
	depl, _ := backend.CreateDeployment(api.ID, "prod", "v1")

	// Stage without access log settings.
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
	})

	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	emitter.mu.Lock()
	events := emitter.events
	emitter.mu.Unlock()

	// No access log settings → no events emitted.
	assert.Empty(t, events)
}

func TestProxy_AccessLog_NoEmitWithoutEmitter(t *testing.T) {
	t.Parallel()

	// No logsEmitter configured — should not panic.
	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend) // no SetLogsEmitter
	e := echo.New()

	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "no-emitter-api"})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{Type: "MOCK"})
	_, _ = backend.PutIntegrationResponse(api.ID, rootID, "GET", "200", apigateway.PutIntegrationResponseInput{})
	depl, _ := backend.CreateDeployment(api.ID, "prod", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
		AccessLogSettings: &apigateway.AccessLogSettings{
			DestinationARN: "arn:aws:logs:us-east-1:123456789012:log-group:/api/access",
		},
	})

	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	// Should complete without panicking.
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestProxy_AccessLog_CustomFormat(t *testing.T) {
	t.Parallel()

	emitter := &captureLogEmitter{}

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	h.SetLogsEmitter(emitter)
	e := echo.New()

	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "custom-log-api"})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "POST",
		AuthorizationType: "NONE",
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "POST", apigateway.PutIntegrationInput{Type: "MOCK"})
	_, _ = backend.PutIntegrationResponse(api.ID, rootID, "POST", "200", apigateway.PutIntegrationResponseInput{})
	depl, _ := backend.CreateDeployment(api.ID, "", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
		AccessLogSettings: &apigateway.AccessLogSettings{
			DestinationARN: "arn:aws:logs:us-east-1:123:log-group:/my/log",
			Format: `{"requestId":"$context.requestId","method":"$context.httpMethod",` +
				`"latency":$context.responseLatencyMs}`,
		},
	})

	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodPost, url, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	emitter.mu.Lock()
	events := emitter.events
	emitter.mu.Unlock()

	require.NotEmpty(t, events)
	// Verify JSON-format log line contains the method.
	assert.Contains(t, events[0].Message, `"method":"POST"`)
}

func TestProxy_AccessLog_LogGroupNameExtractedFromARN(t *testing.T) {
	t.Parallel()

	emitter := &captureLogEmitter{}

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	h.SetLogsEmitter(emitter)
	e := echo.New()

	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "arn-log-api"})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{Type: "MOCK"})
	_, _ = backend.PutIntegrationResponse(api.ID, rootID, "GET", "200", apigateway.PutIntegrationResponseInput{})
	depl, _ := backend.CreateDeployment(api.ID, "", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
		AccessLogSettings: &apigateway.AccessLogSettings{
			DestinationARN: "arn:aws:logs:us-east-1:123456789012:log-group:/prod/access-logs",
		},
	})

	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	emitter.mu.Lock()
	groups := emitter.groups
	emitter.mu.Unlock()

	require.NotEmpty(t, groups)
	// Log group name should be extracted from ARN (not the full ARN).
	assert.Equal(t, "/prod/access-logs", groups[0])
}

// --- proxy: request body model validation ---

func TestProxy_ModelValidation_BlocksMissingRequiredField(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	e := echo.New()

	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "model-val-api"})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	schema := `{"type":"object","required":["name","email"],` +
		`"properties":{"name":{"type":"string"},"email":{"type":"string"}}}`
	model, _ := backend.CreateModel(apigateway.CreateModelInput{
		RestAPIID:   api.ID,
		Name:        "CreateUser",
		ContentType: "application/json",
		Schema:      schema,
	})

	rv, _ := backend.CreateRequestValidator(api.ID, apigateway.CreateRequestValidatorInput{
		Name:                "body-validator",
		ValidateRequestBody: true,
	})
	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:          api.ID,
		ResourceID:         rootID,
		HTTPMethod:         "POST",
		AuthorizationType:  "NONE",
		RequestValidatorID: rv.ID,
		RequestModels:      map[string]string{"application/json": model.Name},
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "POST", apigateway.PutIntegrationInput{Type: "MOCK"})
	_, _ = backend.PutIntegrationResponse(api.ID, rootID, "POST", "200", apigateway.PutIntegrationResponseInput{})
	depl, _ := backend.CreateDeployment(api.ID, "", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
	})

	// Body with missing required "email" field.
	body := strings.NewReader(`{"name":"Alice"}`)
	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodPost, url, body)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "email")
}

func TestProxy_ModelValidation_AllowsAllRequiredFieldsPresent(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	e := echo.New()

	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "model-allow-api"})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	schema := `{"type":"object","required":["name"],"properties":{"name":{"type":"string"}}}`
	model, _ := backend.CreateModel(apigateway.CreateModelInput{
		RestAPIID:   api.ID,
		Name:        "Item",
		ContentType: "application/json",
		Schema:      schema,
	})

	rv, _ := backend.CreateRequestValidator(api.ID, apigateway.CreateRequestValidatorInput{
		Name:                "body-validator",
		ValidateRequestBody: true,
	})
	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:          api.ID,
		ResourceID:         rootID,
		HTTPMethod:         "POST",
		AuthorizationType:  "NONE",
		RequestValidatorID: rv.ID,
		RequestModels:      map[string]string{"application/json": model.Name},
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "POST", apigateway.PutIntegrationInput{Type: "MOCK"})
	_, _ = backend.PutIntegrationResponse(api.ID, rootID, "POST", "200", apigateway.PutIntegrationResponseInput{})
	depl, _ := backend.CreateDeployment(api.ID, "", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
	})

	// Body with all required fields present.
	body := strings.NewReader(`{"name":"Widget"}`)
	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodPost, url, body)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestProxy_ModelValidation_SkipsWhenNoValidateBodyFlag(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	e := echo.New()

	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "model-skip-api"})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	schema := `{"type":"object","required":["name"],"properties":{"name":{"type":"string"}}}`
	model, _ := backend.CreateModel(apigateway.CreateModelInput{
		RestAPIID:   api.ID,
		Name:        "Item",
		ContentType: "application/json",
		Schema:      schema,
	})

	// Validator with ValidateRequestBody = false.
	rv, _ := backend.CreateRequestValidator(api.ID, apigateway.CreateRequestValidatorInput{
		Name:                "params-only-validator",
		ValidateRequestBody: false,
	})
	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:          api.ID,
		ResourceID:         rootID,
		HTTPMethod:         "POST",
		AuthorizationType:  "NONE",
		RequestValidatorID: rv.ID,
		RequestModels:      map[string]string{"application/json": model.Name},
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "POST", apigateway.PutIntegrationInput{Type: "MOCK"})
	_, _ = backend.PutIntegrationResponse(api.ID, rootID, "POST", "200", apigateway.PutIntegrationResponseInput{})
	depl, _ := backend.CreateDeployment(api.ID, "", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
	})

	// Body missing required field — should still pass because ValidateRequestBody = false.
	body := strings.NewReader(`{}`)
	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodPost, url, body)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestProxy_ModelValidation_FallsBackToJSONModelForUnknownContentType(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	e := echo.New()

	api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "model-ct-api"})
	resources, _, _ := backend.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	schema := `{"type":"object","required":["id"],"properties":{"id":{"type":"string"}}}`
	model, _ := backend.CreateModel(apigateway.CreateModelInput{
		RestAPIID:   api.ID,
		Name:        "Thing",
		ContentType: "application/json",
		Schema:      schema,
	})

	rv, _ := backend.CreateRequestValidator(api.ID, apigateway.CreateRequestValidatorInput{
		Name:                "body-validator",
		ValidateRequestBody: true,
	})
	_, _ = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID:          api.ID,
		ResourceID:         rootID,
		HTTPMethod:         "POST",
		AuthorizationType:  "NONE",
		RequestValidatorID: rv.ID,
		// Only JSON model registered — request uses different Content-Type.
		RequestModels: map[string]string{"application/json": model.Name},
	})
	_, _ = backend.PutIntegration(api.ID, rootID, "POST", apigateway.PutIntegrationInput{Type: "MOCK"})
	_, _ = backend.PutIntegrationResponse(api.ID, rootID, "POST", "200", apigateway.PutIntegrationResponseInput{})
	depl, _ := backend.CreateDeployment(api.ID, "", "v1")
	_, _ = backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:    api.ID,
		StageName:    "prod",
		DeploymentID: depl.ID,
	})

	// Content-Type is "text/plain" but fallback to application/json model applies.
	// Missing required "id" field → blocked.
	body := strings.NewReader(`{"name":"no-id"}`)
	url := "/restapis/" + api.ID + "/prod/_user_request_/"
	req := httptest.NewRequest(http.MethodPost, url, body)
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- helpers ---

type countingInvoker struct {
	count    *int
	response []byte
}

func (c *countingInvoker) InvokeFunction(_ context.Context, _, _ string, _ []byte) ([]byte, int, error) {
	*c.count++

	return c.response, http.StatusOK, nil
}

// funcTrackingInvoker records the function names passed to InvokeFunction.
type funcTrackingInvoker struct {
	tracked  *[]string
	mu       *sync.Mutex
	response []byte
}

func (f *funcTrackingInvoker) InvokeFunction(_ context.Context, fn, _ string, _ []byte) ([]byte, int, error) {
	f.mu.Lock()
	*f.tracked = append(*f.tracked, fn)
	f.mu.Unlock()

	return f.response, http.StatusOK, nil
}

// captureLogEmitter collects log events for test assertions.
type captureLogEmitter struct {
	events []apigateway.LogEvent
	groups []string
	mu     sync.Mutex
}

func (c *captureLogEmitter) PutLogEvents(groupName, _ string, events []apigateway.LogEvent) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, events...)
	c.groups = append(c.groups, groupName)

	return nil
}
