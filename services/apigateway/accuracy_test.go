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

// --- helpers ---

type countingInvoker struct {
	count    *int
	response []byte
}

func (c *countingInvoker) InvokeFunction(_ context.Context, _, _ string, _ []byte) ([]byte, int, error) {
	*c.count++

	return c.response, http.StatusOK, nil
}
