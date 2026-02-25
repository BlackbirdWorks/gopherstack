package apigateway_test

import (
	"context"
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
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// post sends a POST request to the APIGateway handler.
func post(t *testing.T, action, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	backend := apigateway.NewInMemoryBackend()
	handler := apigateway.NewHandler(backend, log)

	return postWithHandler(t, handler, e, action, body)
}

// postWithHandler sends a POST to a specific handler instance.
func postWithHandler(
	t *testing.T,
	handler *apigateway.Handler,
	e *echo.Echo,
	action, body string,
) *httptest.ResponseRecorder {
	t.Helper()

	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	} else {
		req = httptest.NewRequest(http.MethodPost, "/", nil)
	}
	if action != "" {
		req.Header.Set("X-Amz-Target", "APIGateway."+action)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := handler.Handler()(c)
	require.NoError(t, err)

	return rec
}

// sharedSetup creates a handler and Echo instance for multi-step tests.
func sharedSetup() (*apigateway.Handler, *echo.Echo) {
	log := logger.NewLogger(slog.LevelDebug)
	backend := apigateway.NewInMemoryBackend()
	handler := apigateway.NewHandler(backend, log)
	e := echo.New()

	return handler, e
}

func TestHandler_CreateAndGetRestApi(t *testing.T) {
	t.Parallel()

	handler, e := sharedSetup()

	rec := postWithHandler(
		t,
		handler,
		e,
		"CreateRestApi",
		`{"name":"my-api","description":"desc","tags":{"env":"test"}}`,
	)
	assert.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	assert.NotEmpty(t, created["id"])
	assert.Equal(t, "my-api", created["name"])

	apiID := created["id"].(string)

	rec2 := postWithHandler(t, handler, e, "GetRestApi", `{"restApiId":"`+apiID+`"}`)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var got map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &got))
	assert.Equal(t, apiID, got["id"])
	assert.Equal(t, "my-api", got["name"])
}

func TestHandler_GetRestApis(t *testing.T) {
	t.Parallel()

	handler, e := sharedSetup()

	postWithHandler(t, handler, e, "CreateRestApi", `{"name":"api-a"}`)
	postWithHandler(t, handler, e, "CreateRestApi", `{"name":"api-b"}`)

	rec := postWithHandler(t, handler, e, "GetRestApis", `{}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["item"].([]any)
	assert.Len(t, items, 2)
}

func TestHandler_DeleteRestApi(t *testing.T) {
	t.Parallel()

	handler, e := sharedSetup()

	rec := postWithHandler(t, handler, e, "CreateRestApi", `{"name":"to-delete"}`)
	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	apiID := created["id"].(string)

	rec2 := postWithHandler(t, handler, e, "DeleteRestApi", `{"restApiId":"`+apiID+`"}`)
	assert.Equal(t, http.StatusAccepted, rec2.Code)

	rec3 := postWithHandler(t, handler, e, "GetRestApi", `{"restApiId":"`+apiID+`"}`)
	assert.Equal(t, http.StatusNotFound, rec3.Code)
}

func TestHandler_CreateAndGetResources(t *testing.T) {
	t.Parallel()

	handler, e := sharedSetup()

	rec := postWithHandler(t, handler, e, "CreateRestApi", `{"name":"api"}`)
	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	apiID := created["id"].(string)

	// Get resources (should have root resource)
	rec2 := postWithHandler(t, handler, e, "GetResources", `{"restApiId":"`+apiID+`"}`)
	assert.Equal(t, http.StatusOK, rec2.Code)
	var res map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &res))
	items := res["item"].([]any)
	assert.Len(t, items, 1)
	root := items[0].(map[string]any)
	rootID := root["id"].(string)
	assert.Equal(t, "/", root["path"])

	// Create a child resource
	rec3 := postWithHandler(
		t,
		handler,
		e,
		"CreateResource",
		`{"restApiId":"`+apiID+`","parentId":"`+rootID+`","pathPart":"users"}`,
	)
	assert.Equal(t, http.StatusCreated, rec3.Code)
	var child map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &child))
	assert.Equal(t, "/users", child["path"])
}

func TestHandler_DeleteResource(t *testing.T) {
	t.Parallel()

	handler, e := sharedSetup()

	rec := postWithHandler(t, handler, e, "CreateRestApi", `{"name":"api"}`)
	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	apiID := created["id"].(string)

	rec2 := postWithHandler(t, handler, e, "GetResources", `{"restApiId":"`+apiID+`"}`)
	var res map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &res))
	rootID := res["item"].([]any)[0].(map[string]any)["id"].(string)

	rec3 := postWithHandler(
		t,
		handler,
		e,
		"CreateResource",
		`{"restApiId":"`+apiID+`","parentId":"`+rootID+`","pathPart":"items"}`,
	)
	var child map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &child))
	childID := child["id"].(string)

	rec4 := postWithHandler(t, handler, e, "DeleteResource", `{"restApiId":"`+apiID+`","resourceId":"`+childID+`"}`)
	assert.Equal(t, http.StatusNoContent, rec4.Code)

	rec5 := postWithHandler(t, handler, e, "GetResource", `{"restApiId":"`+apiID+`","resourceId":"`+childID+`"}`)
	assert.Equal(t, http.StatusNotFound, rec5.Code)
}

func TestHandler_PutGetDeleteMethod(t *testing.T) {
	t.Parallel()

	handler, e := sharedSetup()

	rec := postWithHandler(t, handler, e, "CreateRestApi", `{"name":"api"}`)
	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	apiID := created["id"].(string)

	rec2 := postWithHandler(t, handler, e, "GetResources", `{"restApiId":"`+apiID+`"}`)
	var res map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &res))
	rootID := res["item"].([]any)[0].(map[string]any)["id"].(string)

	// PutMethod
	rec3 := postWithHandler(t, handler, e, "PutMethod",
		`{"restApiId":"`+apiID+`","resourceId":"`+rootID+`","httpMethod":"GET","authorizationType":"NONE"}`)
	assert.Equal(t, http.StatusCreated, rec3.Code)
	var m map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &m))
	assert.Equal(t, "GET", m["httpMethod"])

	// GetMethod
	rec4 := postWithHandler(t, handler, e, "GetMethod",
		`{"restApiId":"`+apiID+`","resourceId":"`+rootID+`","httpMethod":"GET"}`)
	assert.Equal(t, http.StatusOK, rec4.Code)

	// DeleteMethod
	rec5 := postWithHandler(t, handler, e, "DeleteMethod",
		`{"restApiId":"`+apiID+`","resourceId":"`+rootID+`","httpMethod":"GET"}`)
	assert.Equal(t, http.StatusNoContent, rec5.Code)

	rec6 := postWithHandler(t, handler, e, "GetMethod",
		`{"restApiId":"`+apiID+`","resourceId":"`+rootID+`","httpMethod":"GET"}`)
	assert.Equal(t, http.StatusNotFound, rec6.Code)
}

func TestHandler_PutGetDeleteIntegration(t *testing.T) {
	t.Parallel()

	handler, e := sharedSetup()

	rec := postWithHandler(t, handler, e, "CreateRestApi", `{"name":"api"}`)
	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	apiID := created["id"].(string)

	rec2 := postWithHandler(t, handler, e, "GetResources", `{"restApiId":"`+apiID+`"}`)
	var res map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &res))
	rootID := res["item"].([]any)[0].(map[string]any)["id"].(string)

	postWithHandler(t, handler, e, "PutMethod",
		`{"restApiId":"`+apiID+`","resourceId":"`+rootID+`","httpMethod":"POST","authorizationType":"NONE"}`)

	// PutIntegration
	rec3 := postWithHandler(t, handler, e, "PutIntegration",
		`{"restApiId":"`+apiID+`","resourceId":"`+rootID+`","httpMethod":"POST","type":"MOCK"}`)
	assert.Equal(t, http.StatusCreated, rec3.Code)
	var integ map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &integ))
	assert.Equal(t, "MOCK", integ["type"])

	// GetIntegration
	rec4 := postWithHandler(t, handler, e, "GetIntegration",
		`{"restApiId":"`+apiID+`","resourceId":"`+rootID+`","httpMethod":"POST"}`)
	assert.Equal(t, http.StatusOK, rec4.Code)

	// DeleteIntegration
	rec5 := postWithHandler(t, handler, e, "DeleteIntegration",
		`{"restApiId":"`+apiID+`","resourceId":"`+rootID+`","httpMethod":"POST"}`)
	assert.Equal(t, http.StatusNoContent, rec5.Code)

	rec6 := postWithHandler(t, handler, e, "GetIntegration",
		`{"restApiId":"`+apiID+`","resourceId":"`+rootID+`","httpMethod":"POST"}`)
	assert.Equal(t, http.StatusNotFound, rec6.Code)
}

func TestHandler_CreateDeploymentAndGetDeployments(t *testing.T) {
	t.Parallel()

	handler, e := sharedSetup()

	rec := postWithHandler(t, handler, e, "CreateRestApi", `{"name":"api"}`)
	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	apiID := created["id"].(string)

	rec2 := postWithHandler(t, handler, e, "CreateDeployment",
		`{"restApiId":"`+apiID+`","stageName":"prod","description":"first"}`)
	assert.Equal(t, http.StatusCreated, rec2.Code)
	var depl map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &depl))
	assert.NotEmpty(t, depl["id"])

	rec3 := postWithHandler(t, handler, e, "GetDeployments", `{"restApiId":"`+apiID+`"}`)
	assert.Equal(t, http.StatusOK, rec3.Code)
	var depls map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &depls))
	assert.Len(t, depls["item"].([]any), 1)
}

func TestHandler_GetStagesAndDeleteStage(t *testing.T) {
	t.Parallel()

	handler, e := sharedSetup()

	rec := postWithHandler(t, handler, e, "CreateRestApi", `{"name":"api"}`)
	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	apiID := created["id"].(string)

	postWithHandler(t, handler, e, "CreateDeployment",
		`{"restApiId":"`+apiID+`","stageName":"staging","description":""}`)

	rec2 := postWithHandler(t, handler, e, "GetStages", `{"restApiId":"`+apiID+`"}`)
	assert.Equal(t, http.StatusOK, rec2.Code)
	var stages map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &stages))
	assert.Len(t, stages["item"].([]any), 1)

	rec3 := postWithHandler(t, handler, e, "GetStage", `{"restApiId":"`+apiID+`","stageName":"staging"}`)
	assert.Equal(t, http.StatusOK, rec3.Code)

	rec4 := postWithHandler(t, handler, e, "DeleteStage", `{"restApiId":"`+apiID+`","stageName":"staging"}`)
	assert.Equal(t, http.StatusNoContent, rec4.Code)

	rec5 := postWithHandler(t, handler, e, "GetStage", `{"restApiId":"`+apiID+`","stageName":"staging"}`)
	assert.Equal(t, http.StatusNotFound, rec5.Code)
}

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	rec := post(t, "UnknownOp", `{}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "UnknownOperationException", resp["__type"])
}

func TestHandler_MissingTarget(t *testing.T) {
	t.Parallel()

	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	backend := apigateway.NewInMemoryBackend()
	handler := apigateway.NewHandler(backend, log)

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := handler.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// mockLambdaInvoker is a simple mock for Lambda invocation in tests.
type mockLambdaInvoker struct {
	returnError error
	response    []byte
	statusCode  int
}

func (m *mockLambdaInvoker) InvokeFunction(_ context.Context, _, _ string, _ []byte) ([]byte, int, error) {
	if m.returnError != nil {
		return nil, 500, m.returnError
	}

	if m.response != nil {
		return m.response, m.statusCode, nil
	}

	return []byte(`{"statusCode":200,"body":"hello"}`), 200, nil
}

func TestHandler_SetLambdaInvoker(t *testing.T) {
	t.Parallel()

	log := logger.NewLogger(slog.LevelDebug)
	backend := apigateway.NewInMemoryBackend()
	handler := apigateway.NewHandler(backend, log)

	mock := &mockLambdaInvoker{}
	handler.SetLambdaInvoker(mock)
	// If SetLambdaInvoker doesn't panic, the test passes.
}

func TestBuildProxyEvent(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodPost, "/my-stage/items?key=val", strings.NewReader(`{"data":"test"}`))
	req.Header.Set("Content-Type", "application/json")

	event, err := apigateway.BuildProxyEvent(req, "abc123", "my-stage", "/items", "/my-stage/items")
	require.NoError(t, err)
	assert.Equal(t, http.MethodPost, event.HTTPMethod)
	assert.Equal(t, "/my-stage/items", event.Path)
	assert.Equal(t, "/items", event.Resource)
	assert.Equal(t, "val", event.QueryStringParameters["key"])
	assert.JSONEq(t, `{"data":"test"}`, event.Body)
	assert.Equal(t, "my-stage", event.RequestContext.Stage)
	assert.Equal(t, "abc123", event.RequestContext.APIId)
}

func TestBuildProxyEvent_BinaryBody(t *testing.T) {
	t.Parallel()

	// Use non-UTF8 bytes to trigger base64 encoding.
	binaryData := []byte{0xFF, 0xFE, 0x00, 0x01}
	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(string(binaryData)))

	event, err := apigateway.BuildProxyEvent(req, "id1", "stage1", "/", "/")
	require.NoError(t, err)
	assert.True(t, event.IsBase64Encoded)
}
