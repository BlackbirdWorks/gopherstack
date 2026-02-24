package apigateway_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/apigateway"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// mockAPIGWConfigProvider implements config.Provider for testing.
type mockAPIGWConfigProvider struct{}

func (m *mockAPIGWConfigProvider) GetGlobalConfig() config.GlobalConfig {
	return config.GlobalConfig{AccountID: "111111111111", Region: "eu-west-1"}
}

func TestProvider_APIGateway_Name(t *testing.T) {
	t.Parallel()
	p := &apigateway.Provider{}
	assert.Equal(t, "APIGateway", p.Name())
}

func TestProvider_APIGateway_Init_WithConfig(t *testing.T) {
	t.Parallel()
	p := &apigateway.Provider{}
	ctx := &service.AppContext{
		Logger: slog.Default(),
		Config: &mockAPIGWConfigProvider{},
	}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, svc)
	assert.Equal(t, "APIGateway", svc.Name())
}

func TestProvider_APIGateway_Init_WithoutConfig(t *testing.T) {
	t.Parallel()
	p := &apigateway.Provider{}
	ctx := &service.AppContext{Logger: slog.Default()}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, svc)
}

func TestHandler_APIGateway_Name(t *testing.T) {
	t.Parallel()
	h := apigateway.NewHandler(apigateway.NewInMemoryBackend(), slog.Default())
	assert.Equal(t, "APIGateway", h.Name())
}

func TestHandler_APIGateway_MatchPriority(t *testing.T) {
	t.Parallel()
	h := apigateway.NewHandler(apigateway.NewInMemoryBackend(), slog.Default())
	assert.Equal(t, 100, h.MatchPriority())
}

func TestHandler_APIGateway_RouteMatcher(t *testing.T) {
	t.Parallel()
	h := apigateway.NewHandler(apigateway.NewInMemoryBackend(), slog.Default())
	matcher := h.RouteMatcher()
	e := echo.New()

	reqMatch := httptest.NewRequest(http.MethodPost, "/", nil)
	reqMatch.Header.Set("X-Amz-Target", "APIGateway.CreateRestApi")
	assert.True(t, matcher(e.NewContext(reqMatch, httptest.NewRecorder())))

	reqNoMatch := httptest.NewRequest(http.MethodPost, "/", nil)
	reqNoMatch.Header.Set("X-Amz-Target", "AmazonSQS.CreateQueue")
	assert.False(t, matcher(e.NewContext(reqNoMatch, httptest.NewRecorder())))
}

func TestHandler_APIGateway_GetSupportedOperations(t *testing.T) {
	t.Parallel()
	h := apigateway.NewHandler(apigateway.NewInMemoryBackend(), slog.Default())
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateRestApi")
	assert.Contains(t, ops, "GetRestApis")
	assert.Contains(t, ops, "PutMethod")
}

func TestHandler_APIGateway_ExtractOperation(t *testing.T) {
	t.Parallel()
	h := apigateway.NewHandler(apigateway.NewInMemoryBackend(), slog.Default())
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "APIGateway.CreateRestApi")
	assert.Equal(t, "CreateRestApi", h.ExtractOperation(e.NewContext(req, httptest.NewRecorder())))

	reqUnknown := httptest.NewRequest(http.MethodPost, "/", nil)
	assert.Equal(t, "Unknown", h.ExtractOperation(e.NewContext(reqUnknown, httptest.NewRecorder())))
}

func TestHandler_APIGateway_ExtractResource(t *testing.T) {
	t.Parallel()
	h := apigateway.NewHandler(apigateway.NewInMemoryBackend(), slog.Default())
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"restApiId":"abc123"}`))
	assert.Equal(t, "abc123", h.ExtractResource(e.NewContext(req, httptest.NewRecorder())))

	reqEmpty := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	assert.Empty(t, h.ExtractResource(e.NewContext(reqEmpty, httptest.NewRecorder())))
}

func TestHandler_APIGateway_MissingTarget(t *testing.T) {
	t.Parallel()
	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	h := apigateway.NewHandler(apigateway.NewInMemoryBackend(), log)

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_APIGateway_MethodNotAllowed(t *testing.T) {
	t.Parallel()
	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	h := apigateway.NewHandler(apigateway.NewInMemoryBackend(), log)

	// A PUT request (not GET "/" or POST) should return 405
	req := httptest.NewRequest(http.MethodPut, "/something", nil)
	req.Header.Set("X-Amz-Target", "APIGateway.CreateRestApi")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_APIGateway_NotFoundErrors(t *testing.T) {
	t.Parallel()

	rec := post(t, "DeleteRestApi", `{"restApiId":"nonexistent"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = post(t, "GetRestApi", `{"restApiId":"nonexistent"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = post(t, "GetResources", `{"restApiId":"nonexistent"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = post(t, "GetResource", `{"restApiId":"nonexistent","resourceId":"r1"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = post(t, "CreateResource", `{"restApiId":"nonexistent","parentId":"r1","pathPart":"pets"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = post(t, "DeleteResource", `{"restApiId":"nonexistent","resourceId":"r1"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = post(
		t,
		"PutMethod",
		`{"restApiId":"nonexistent","resourceId":"r1","httpMethod":"GET","authorizationType":"NONE"}`,
	)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = post(t, "GetMethod", `{"restApiId":"nonexistent","resourceId":"r1","httpMethod":"GET"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = post(t, "DeleteMethod", `{"restApiId":"nonexistent","resourceId":"r1","httpMethod":"GET"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = post(t, "PutIntegration",
		`{"restApiId":"nonexistent","resourceId":"r1","httpMethod":"GET","type":"MOCK"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = post(t, "GetIntegration",
		`{"restApiId":"nonexistent","resourceId":"r1","httpMethod":"GET"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = post(t, "DeleteIntegration",
		`{"restApiId":"nonexistent","resourceId":"r1","httpMethod":"GET"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = post(t, "CreateDeployment", `{"restApiId":"nonexistent","stageName":"prod"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = post(t, "GetDeployments", `{"restApiId":"nonexistent"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = post(t, "GetStages", `{"restApiId":"nonexistent"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = post(t, "GetStage", `{"restApiId":"nonexistent","stageName":"prod"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = post(t, "DeleteStage", `{"restApiId":"nonexistent","stageName":"prod"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_APIGateway_InvalidJSON(t *testing.T) {
	t.Parallel()
	rec := post(t, "CreateRestApi", `not-json`)
	// JSON parse errors map to 500 InternalServerError
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestHandler_APIGateway_FullWorkflow(t *testing.T) {
	t.Parallel()
	h, e := sharedSetup()

	// Create REST API
	createRec := postWithHandler(t, h, e, "CreateRestApi", `{"name":"my-api","description":"Test API"}`)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var createResp map[string]any
	require.NoError(t, parseJSON(createRec, &createResp))
	apiID := createResp["id"].(string)
	require.NotEmpty(t, apiID)

	// GetRestApis with position
	listRec := postWithHandler(t, h, e, "GetRestApis", `{"limit":10,"position":""}`)
	assert.Equal(t, http.StatusOK, listRec.Code)

	// CreateResource
	var resourcesResp map[string]any
	resListRec := postWithHandler(t, h, e, "GetResources",
		`{"restApiId":"`+apiID+`","limit":10}`)
	require.Equal(t, http.StatusOK, resListRec.Code)
	require.NoError(t, parseJSON(resListRec, &resourcesResp))

	items := resourcesResp["item"].([]any)
	require.Len(t, items, 1)
	rootID := items[0].(map[string]any)["id"].(string)

	childRec := postWithHandler(t, h, e, "CreateResource",
		`{"restApiId":"`+apiID+`","parentId":"`+rootID+`","pathPart":"pets"}`)
	require.Equal(t, http.StatusCreated, childRec.Code)

	var childResp map[string]any
	require.NoError(t, parseJSON(childRec, &childResp))
	resourceID := childResp["id"].(string)

	// PutMethod
	methodRec := postWithHandler(t, h, e, "PutMethod",
		`{"restApiId":"`+apiID+`","resourceId":"`+resourceID+`","httpMethod":"GET","authorizationType":"NONE"}`)
	assert.Equal(t, http.StatusCreated, methodRec.Code)

	// PutIntegration
	intRec := postWithHandler(t, h, e, "PutIntegration",
		`{"restApiId":"`+apiID+`","resourceId":"`+resourceID+`","httpMethod":"GET","type":"MOCK"}`)
	assert.Equal(t, http.StatusCreated, intRec.Code)

	// CreateDeployment
	deplRec := postWithHandler(t, h, e, "CreateDeployment",
		`{"restApiId":"`+apiID+`","stageName":"prod","description":"Initial"}`)
	assert.Equal(t, http.StatusCreated, deplRec.Code)

	// GetDeployments
	deplListRec := postWithHandler(t, h, e, "GetDeployments",
		`{"restApiId":"`+apiID+`"}`)
	assert.Equal(t, http.StatusOK, deplListRec.Code)

	// GetStage
	stageRec := postWithHandler(t, h, e, "GetStage",
		`{"restApiId":"`+apiID+`","stageName":"prod"}`)
	assert.Equal(t, http.StatusOK, stageRec.Code)

	// DeleteStage
	delStageRec := postWithHandler(t, h, e, "DeleteStage",
		`{"restApiId":"`+apiID+`","stageName":"prod"}`)
	assert.Equal(t, http.StatusNoContent, delStageRec.Code)

	// DeleteMethod
	delMethodRec := postWithHandler(t, h, e, "DeleteMethod",
		`{"restApiId":"`+apiID+`","resourceId":"`+resourceID+`","httpMethod":"GET"}`)
	assert.Equal(t, http.StatusNoContent, delMethodRec.Code)

	// DeleteResource
	delResRec := postWithHandler(t, h, e, "DeleteResource",
		`{"restApiId":"`+apiID+`","resourceId":"`+resourceID+`"}`)
	assert.Equal(t, http.StatusNoContent, delResRec.Code)

	// DeleteRestApi
	delRec := postWithHandler(t, h, e, "DeleteRestApi", `{"restApiId":"`+apiID+`"}`)
	assert.Equal(t, http.StatusAccepted, delRec.Code)
}

func TestHandler_APIGateway_MissingParams(t *testing.T) {
	t.Parallel()
	rec := post(t, "CreateRestApi", `{}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// parseJSON is a helper to decode JSON from a response recorder.
func parseJSON(rec *httptest.ResponseRecorder, v any) error {
	return json.Unmarshal(rec.Body.Bytes(), v)
}
