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
	h := apigateway.NewHandler(backend)
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

func TestHandleAWSProxy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupInvoker func(*apigateway.Handler)
		name         string
		path         string
		body         string
		wantBody     string
		wantStatus   int
	}{
		{
			name: "success",
			path: "/items",
			body: `{"key":"val"}`,
			setupInvoker: func(h *apigateway.Handler) {
				h.SetLambdaInvoker(&proxyMockInvoker{})
			},
			wantStatus: http.StatusOK,
			wantBody:   "ok",
		},
		{
			name: "lambda_error",
			path: "/items",
			body: `{}`,
			setupInvoker: func(h *apigateway.Handler) {
				h.SetLambdaInvoker(&proxyMockInvoker{returnError: errFunctionError})
			},
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:         "no_lambda_invoker",
			path:         "/items",
			body:         `{}`,
			setupInvoker: nil,
			wantStatus:   http.StatusServiceUnavailable,
		},
		{
			name: "not_found",
			path: "/unknown/path",
			body: `{}`,
			setupInvoker: func(h *apigateway.Handler) {
				h.SetLambdaInvoker(&proxyMockInvoker{})
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "base64_response",
			path: "/items",
			body: `{}`,
			setupInvoker: func(h *apigateway.Handler) {
				h.SetLambdaInvoker(&proxyMockInvoker{
					response: []byte(`{"statusCode":200,"body":"aGVsbG8=","isBase64Encoded":true}`),
				})
			},
			wantStatus: http.StatusOK,
			wantBody:   "hello",
		},
		{
			name: "non_proxy_response",
			path: "/items",
			body: `{}`,
			setupInvoker: func(h *apigateway.Handler) {
				h.SetLambdaInvoker(&proxyMockInvoker{response: []byte(`not json`)})
			},
			wantStatus: http.StatusOK,
			wantBody:   "not json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e, apiID := setupProxyAPIViaHandler(t, "AWS_PROXY", "arn:aws:lambda:us-east-1:123:function:myFn")
			if tt.setupInvoker != nil {
				tt.setupInvoker(h)
			}

			rec := proxyReq(t, h, e, apiID, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Equal(t, tt.wantBody, rec.Body.String())
			}
		})
	}
}

// setupVTLAPI creates an API with a /transform resource that has a requestTemplates integration.
func setupVTLAPI(t *testing.T) (*apigateway.Handler, *echo.Echo, string) {
	t.Helper()

	log := logger.NewLogger(slog.LevelDebug)
	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	e := echo.New()

	createRec := postWithHandler(t, h, e, "CreateRestApi", `{"name":"vtl-api","description":"test"}`)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	apiID := createResp["id"].(string)

	listRec := postWithHandler(t, h, e, "GetResources", `{"restApiId":"`+apiID+`"}`)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	rootID := listResp["item"].([]any)[0].(map[string]any)["id"].(string)

	childRec := postWithHandler(t, h, e, "CreateResource",
		`{"restApiId":"`+apiID+`","parentId":"`+rootID+`","pathPart":"transform"}`)
	var childResp map[string]any
	require.NoError(t, json.Unmarshal(childRec.Body.Bytes(), &childResp))
	childID := childResp["id"].(string)

	postWithHandler(t, h, e, "PutMethod",
		`{"restApiId":"`+apiID+`","resourceId":"`+childID+`","httpMethod":"POST","authorizationType":"NONE"}`)

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

	return h, e, apiID
}

func TestHandleAWSIntegration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn      func(t *testing.T) (*apigateway.Handler, *echo.Echo, string)
		invokerFn    func(capture *[]byte) apigateway.LambdaInvoker
		checkCapture func(t *testing.T, captured []byte)
		name         string
		path         string
		body         string
		wantStatus   int
	}{
		{
			name: "with_request_template",
			setupFn: func(t *testing.T) (*apigateway.Handler, *echo.Echo, string) {
				t.Helper()

				return setupVTLAPI(t)
			},
			invokerFn: func(capture *[]byte) apigateway.LambdaInvoker {
				return &captureInvoker{capture: capture}
			},
			path:       "/transform",
			body:       `{"user":"alice"}`,
			wantStatus: http.StatusOK,
			checkCapture: func(t *testing.T, captured []byte) {
				t.Helper()
				var got map[string]any
				require.NoError(t, json.Unmarshal(captured, &got))
				assert.Equal(t, "alice", got["name"])
			},
		},
		{
			name: "lambda_error",
			setupFn: func(t *testing.T) (*apigateway.Handler, *echo.Echo, string) {
				t.Helper()

				return setupProxyAPIViaHandler(t, "AWS", "arn:aws:lambda:us-east-1:123:function:fn")
			},
			invokerFn: func(_ *[]byte) apigateway.LambdaInvoker {
				return &proxyMockInvoker{returnError: errLambdaError}
			},
			path:       "/items",
			body:       `{}`,
			wantStatus: http.StatusInternalServerError,
		},
		{
			name: "no_request_template",
			setupFn: func(t *testing.T) (*apigateway.Handler, *echo.Echo, string) {
				t.Helper()

				return setupProxyAPIViaHandler(t, "AWS", "arn:aws:lambda:us-east-1:123:function:fn")
			},
			invokerFn: func(capture *[]byte) apigateway.LambdaInvoker {
				return &captureInvoker{capture: capture}
			},
			path:       "/items",
			body:       `{"data":"test"}`,
			wantStatus: http.StatusOK,
			checkCapture: func(t *testing.T, captured []byte) {
				t.Helper()
				assert.JSONEq(t, `{"data":"test"}`, string(captured))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e, apiID := tt.setupFn(t)

			var capturedPayload []byte
			h.SetLambdaInvoker(tt.invokerFn(&capturedPayload))

			rec := proxyReq(t, h, e, apiID, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.checkCapture != nil {
				tt.checkCapture(t, capturedPayload)
			}
		})
	}
}

func TestHandleProxy_UnsupportedIntegrationType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		intType    string
		uri        string
		wantStatus int
	}{
		{
			name:       "mock_type_not_implemented",
			intType:    "MOCK",
			uri:        "",
			wantStatus: http.StatusNotImplemented,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e, apiID := setupProxyAPIViaHandler(t, tt.intType, tt.uri)
			h.SetLambdaInvoker(&proxyMockInvoker{})

			rec := proxyReq(t, h, e, apiID, "/items", `{}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandleStageProxy_InvalidPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		url        string
		wantStatus int
	}{
		{
			name:       "single_path_segment",
			url:        "/proxy/onlyonepart",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			log := logger.NewLogger(slog.LevelDebug)
			backend := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(backend)
			e := echo.New()

			req := httptest.NewRequest(http.MethodGet, tt.url, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
