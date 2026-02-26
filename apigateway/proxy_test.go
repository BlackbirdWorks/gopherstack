package apigateway_test

import (
	"context"
	"encoding/json"
	"errors"
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

var (
	errFunctionError = errors.New("function error")
	errLambdaError   = errors.New("lambda error")
)

// proxyMockInvoker records the payload sent to InvokeFunction.
type proxyMockInvoker struct {
	returnError error
	capturedFn  string
	response    []byte
	statusCode  int
}

func (m *proxyMockInvoker) InvokeFunction(_ context.Context, fn, _ string, _ []byte) ([]byte, int, error) {
	m.capturedFn = fn
	if m.returnError != nil {
		return nil, http.StatusInternalServerError, m.returnError
	}

	if m.response != nil {
		return m.response, m.statusCode, nil
	}

	return []byte(`{"statusCode":200,"body":"ok","headers":{}}`), http.StatusOK, nil
}

// captureInvoker records the last payload sent to InvokeFunction.
type captureInvoker struct {
	capture *[]byte
}

func (c *captureInvoker) InvokeFunction(_ context.Context, _, _ string, payload []byte) ([]byte, int, error) {
	*c.capture = payload

	return payload, http.StatusOK, nil
}

// setupProxyAPIViaHandler creates a full API setup using HTTP handler calls.
// Returns (handler, echoEngine, apiID).
func setupProxyAPIViaHandler(
	t *testing.T,
	integrationType, uri string,
) (*apigateway.Handler, *echo.Echo, string) {
	t.Helper()

	log := logger.NewLogger(slog.LevelDebug)
	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend, log)
	e := echo.New()

	// Create REST API.
	createRec := postWithHandler(t, h, e, "CreateRestApi", `{"name":"proxy-api","description":"test"}`)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	apiID := createResp["id"].(string)

	// Get root resource.
	listRec := postWithHandler(t, h, e, "GetResources", `{"restApiId":"`+apiID+`"}`)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	rootID := listResp["item"].([]any)[0].(map[string]any)["id"].(string)

	// Create child resource.
	childRec := postWithHandler(t, h, e, "CreateResource",
		`{"restApiId":"`+apiID+`","parentId":"`+rootID+`","pathPart":"items"}`)
	require.Equal(t, http.StatusCreated, childRec.Code)

	var childResp map[string]any
	require.NoError(t, json.Unmarshal(childRec.Body.Bytes(), &childResp))
	childID := childResp["id"].(string)

	// PutMethod.
	methodRec := postWithHandler(t, h, e, "PutMethod",
		`{"restApiId":"`+apiID+`","resourceId":"`+childID+`","httpMethod":"POST","authorizationType":"NONE"}`)
	require.Equal(t, http.StatusCreated, methodRec.Code)

	// PutIntegration.
	integBody := `{"restApiId":"` + apiID + `","resourceId":"` + childID + `","httpMethod":"POST","type":"` +
		integrationType + `","uri":"` + uri + `"}`
	integRec := postWithHandler(t, h, e, "PutIntegration", integBody)
	require.Equal(t, http.StatusCreated, integRec.Code)

	// CreateDeployment.
	deplRec := postWithHandler(t, h, e, "CreateDeployment",
		`{"restApiId":"`+apiID+`","stageName":"prod","description":"v1"}`)
	require.Equal(t, http.StatusCreated, deplRec.Code)

	return h, e, apiID
}

const testStageName = "prod"

// proxyReq makes a POST request via the /proxy/{apiId}/prod/{path} endpoint.
func proxyReq(
	t *testing.T,
	h *apigateway.Handler,
	e *echo.Echo,
	apiID, path, body string,
) *httptest.ResponseRecorder {
	t.Helper()

	url := "/proxy/" + apiID + "/" + testStageName + path
	var req *http.Request

	if body != "" {
		req = httptest.NewRequest(http.MethodPost, url, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(http.MethodPost, url, nil)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandleAWSProxy_Success(t *testing.T) {
	t.Parallel()

	h, e, apiID := setupProxyAPIViaHandler(t, "AWS_PROXY", "arn:aws:lambda:us-east-1:123:function:myFn")

	mock := &proxyMockInvoker{}
	h.SetLambdaInvoker(mock)

	rec := proxyReq(t, h, e, apiID, "/items", `{"key":"val"}`)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "ok", rec.Body.String())
}

func TestHandleAWSProxy_LambdaError(t *testing.T) {
	t.Parallel()

	h, e, apiID := setupProxyAPIViaHandler(t, "AWS_PROXY", "arn:aws:lambda:us-east-1:123:function:errFn")

	mock := &proxyMockInvoker{returnError: errFunctionError}
	h.SetLambdaInvoker(mock)

	rec := proxyReq(t, h, e, apiID, "/items", `{}`)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestHandleAWSProxy_NoLambda(t *testing.T) {
	t.Parallel()

	h, e, apiID := setupProxyAPIViaHandler(t, "AWS_PROXY", "arn:aws:lambda:us-east-1:123:function:fn")
	// No lambda invoker set.

	rec := proxyReq(t, h, e, apiID, "/items", `{}`)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestHandleAWSProxy_NotFound(t *testing.T) {
	t.Parallel()

	h, e, apiID := setupProxyAPIViaHandler(t, "AWS_PROXY", "arn:aws:lambda:us-east-1:123:function:fn")
	mock := &proxyMockInvoker{}
	h.SetLambdaInvoker(mock)

	// Request a path that doesn't match any resource.
	rec := proxyReq(t, h, e, apiID, "/unknown/path", `{}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleAWSProxy_Base64Response(t *testing.T) {
	t.Parallel()

	h, e, apiID := setupProxyAPIViaHandler(t, "AWS_PROXY", "arn:aws:lambda:us-east-1:123:function:fn")

	b64Body := "aGVsbG8=" // base64 of "hello"
	mock := &proxyMockInvoker{
		response: []byte(`{"statusCode":200,"body":"` + b64Body + `","isBase64Encoded":true}`),
	}
	h.SetLambdaInvoker(mock)

	rec := proxyReq(t, h, e, apiID, "/items", `{}`)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "hello", rec.Body.String())
}

func TestHandleAWSProxy_NonProxyResponse(t *testing.T) {
	t.Parallel()

	h, e, apiID := setupProxyAPIViaHandler(t, "AWS_PROXY", "arn:aws:lambda:us-east-1:123:function:fn")

	// Non-JSON response should be returned as-is.
	mock := &proxyMockInvoker{response: []byte(`not json`)}
	h.SetLambdaInvoker(mock)

	rec := proxyReq(t, h, e, apiID, "/items", `{}`)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "not json", rec.Body.String())
}

func TestHandleAWSIntegration_WithRequestTemplate(t *testing.T) {
	t.Parallel()

	log := logger.NewLogger(slog.LevelDebug)
	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend, log)
	e := echo.New()

	// Create REST API.
	createRec := postWithHandler(t, h, e, "CreateRestApi", `{"name":"vtl-api","description":"test"}`)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	apiID := createResp["id"].(string)

	// Get root resource.
	listRec := postWithHandler(t, h, e, "GetResources", `{"restApiId":"`+apiID+`"}`)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	rootID := listResp["item"].([]any)[0].(map[string]any)["id"].(string)

	// Create child resource.
	childRec := postWithHandler(t, h, e, "CreateResource",
		`{"restApiId":"`+apiID+`","parentId":"`+rootID+`","pathPart":"transform"}`)
	var childResp map[string]any
	require.NoError(t, json.Unmarshal(childRec.Body.Bytes(), &childResp))
	childID := childResp["id"].(string)

	postWithHandler(t, h, e, "PutMethod",
		`{"restApiId":"`+apiID+`","resourceId":"`+childID+`","httpMethod":"POST","authorizationType":"NONE"}`)

	// PutIntegration with requestTemplates.
	integBody := `{
		"restApiId":"` + apiID + `",
		"resourceId":"` + childID + `",
		"httpMethod":"POST",
		"type":"AWS",
		"uri":"arn:aws:lambda:us-east-1:123:function:fn",
		"requestTemplates":{"application/json":"{\"name\":$input.json('$.user')}"}
	}`
	integRec := postWithHandler(t, h, e, "PutIntegration", integBody)
	require.Equal(t, http.StatusCreated, integRec.Code)

	postWithHandler(t, h, e, "CreateDeployment",
		`{"restApiId":"`+apiID+`","stageName":"prod","description":"v1"}`)

	var capturedPayload []byte
	mock := &captureInvoker{capture: &capturedPayload}
	h.SetLambdaInvoker(mock)

	rec := proxyReq(t, h, e, apiID, "/transform", `{"user":"alice"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	var got map[string]any
	require.NoError(t, json.Unmarshal(capturedPayload, &got))
	assert.Equal(t, "alice", got["name"])
}

func TestHandleAWSIntegration_LambdaError(t *testing.T) {
	t.Parallel()

	h, e, apiID := setupProxyAPIViaHandler(t, "AWS", "arn:aws:lambda:us-east-1:123:function:fn")

	mock := &proxyMockInvoker{returnError: errLambdaError}
	h.SetLambdaInvoker(mock)

	rec := proxyReq(t, h, e, apiID, "/items", `{}`)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestHandleProxy_UnsupportedIntegrationType(t *testing.T) {
	t.Parallel()

	h, e, apiID := setupProxyAPIViaHandler(t, "MOCK", "")
	mock := &proxyMockInvoker{}
	h.SetLambdaInvoker(mock)

	rec := proxyReq(t, h, e, apiID, "/items", `{}`)
	assert.Equal(t, http.StatusNotImplemented, rec.Code)
}

func TestHandleStageProxy_InvalidPath(t *testing.T) {
	t.Parallel()

	log := logger.NewLogger(slog.LevelDebug)
	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend, log)
	e := echo.New()

	req := httptest.NewRequest(http.MethodGet, "/proxy/onlyonepart", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleAWSIntegration_NoRequestTemplate(t *testing.T) {
	t.Parallel()

	h, e, apiID := setupProxyAPIViaHandler(t, "AWS", "arn:aws:lambda:us-east-1:123:function:fn")

	var capturedPayload []byte
	mock := &captureInvoker{capture: &capturedPayload}
	h.SetLambdaInvoker(mock)

	body := `{"data":"test"}`
	rec := proxyReq(t, h, e, apiID, "/items", body)
	assert.Equal(t, http.StatusOK, rec.Code)
	// Without a template, raw body is forwarded.
	assert.Equal(t, body, string(capturedPayload))
}
