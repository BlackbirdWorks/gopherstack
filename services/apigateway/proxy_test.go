package apigateway_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
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
			name:       "unknown_type_not_implemented",
			intType:    "UNKNOWN_CUSTOM",
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

// setupProxyAPIWithResource creates a handler, Echo instance, and API with a specific resource path.
// Each segment of resourcePath is created as a nested resource under root.
func setupProxyAPIWithResource(
	t *testing.T,
	resourcePath, integrationType, uri string,
) (*apigateway.Handler, *echo.Echo, string) {
	t.Helper()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	e := echo.New()

	// Create REST API.
	createRec := postWithHandler(t, h, e, "CreateRestApi", `{"name":"path-api","description":"test"}`)
	require.Equal(t, http.StatusCreated, createRec.Code)
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	apiID := createResp["id"].(string)

	// Get root resource ID by finding the resource with path "/".
	listRec := postWithHandler(t, h, e, "GetResources", `{"restApiId":"`+apiID+`"}`)
	require.Equal(t, http.StatusOK, listRec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))

	items, _ := listResp["item"].([]any)
	var parentID string

	for _, item := range items {
		res, ok := item.(map[string]any)
		if !ok {
			continue
		}

		if path, _ := res["path"].(string); path == "/" {
			parentID, _ = res["id"].(string)

			break
		}
	}

	require.NotEmpty(t, parentID, "root resource with path '/' not found")

	// Create each path segment as a nested resource.
	if resourcePath != "" {
		parts := strings.SplitSeq(strings.Trim(resourcePath, "/"), "/")
		for part := range parts {
			r := postWithHandler(t, h, e, "CreateResource",
				`{"restApiId":"`+apiID+`","parentId":"`+parentID+`","pathPart":"`+part+`"}`)
			require.Equal(t, http.StatusCreated, r.Code)
			var resp map[string]any
			require.NoError(t, json.Unmarshal(r.Body.Bytes(), &resp))
			parentID = resp["id"].(string)
		}
	}

	childID := parentID

	// PutMethod.
	methodRec := postWithHandler(t, h, e, "PutMethod",
		`{"restApiId":"`+apiID+`","resourceId":"`+childID+`","httpMethod":"GET","authorizationType":"NONE"}`)
	require.Equal(t, http.StatusCreated, methodRec.Code)

	// PutIntegration.
	integBody := `{"restApiId":"` + apiID + `","resourceId":"` + childID + `","httpMethod":"GET","type":"` +
		integrationType + `","uri":"` + uri + `"}`
	integRec := postWithHandler(t, h, e, "PutIntegration", integBody)
	require.Equal(t, http.StatusCreated, integRec.Code)

	// CreateDeployment.
	deployRec := postWithHandler(t, h, e, "CreateDeployment",
		`{"restApiId":"`+apiID+`","stageName":"prod","description":"v1"}`)
	require.Equal(t, http.StatusCreated, deployRec.Code)

	return h, e, apiID
}

// userReq sends a GET request via the /restapis/{apiId}/prod/_user_request_/{path} endpoint.
func userReq(
	t *testing.T,
	h *apigateway.Handler,
	e *echo.Echo,
	apiID, path string,
) *httptest.ResponseRecorder {
	t.Helper()

	url := "/restapis/" + apiID + "/prod/_user_request_" + path
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestUserRequestEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupInvoker func(*apigateway.Handler)
		name         string
		resourcePath string
		requestPath  string
		wantStatus   int
	}{
		{
			name:         "exact_match",
			resourcePath: "items",
			requestPath:  "/items",
			setupInvoker: func(h *apigateway.Handler) {
				h.SetLambdaInvoker(&proxyMockInvoker{})
			},
			wantStatus: http.StatusOK,
		},
		{
			name:         "not_found",
			resourcePath: "items",
			requestPath:  "/unknown",
			setupInvoker: func(h *apigateway.Handler) {
				h.SetLambdaInvoker(&proxyMockInvoker{})
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name:         "no_lambda_invoker",
			resourcePath: "items",
			requestPath:  "/items",
			wantStatus:   http.StatusServiceUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e, apiID := setupProxyAPIWithResource(t, tt.resourcePath, "AWS_PROXY", "fn")
			if tt.setupInvoker != nil {
				tt.setupInvoker(h)
			}

			rec := userReq(t, h, e, apiID, tt.requestPath)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestPathVariableMatching(t *testing.T) {
	t.Parallel()

	tests := []struct {
		checkEvent   func(t *testing.T, payload []byte)
		name         string
		resourcePath string
		requestPath  string
		wantStatus   int
	}{
		{
			name:         "single_param",
			resourcePath: "items/{id}",
			requestPath:  "/items/42",
			wantStatus:   http.StatusOK,
			checkEvent: func(t *testing.T, payload []byte) {
				t.Helper()
				var event map[string]any
				require.NoError(t, json.Unmarshal(payload, &event))
				params, _ := event["pathParameters"].(map[string]any)
				assert.Equal(t, "42", params["id"])
			},
		},
		{
			name:         "greedy_param",
			resourcePath: "{proxy+}",
			requestPath:  "/a/b/c",
			wantStatus:   http.StatusOK,
			checkEvent: func(t *testing.T, payload []byte) {
				t.Helper()
				var event map[string]any
				require.NoError(t, json.Unmarshal(payload, &event))
				params, _ := event["pathParameters"].(map[string]any)
				assert.Equal(t, "/a/b/c", params["proxy"])
			},
		},
		{
			name:         "param_no_match_wrong_depth",
			resourcePath: "items/{id}/details",
			requestPath:  "/items/42",
			wantStatus:   http.StatusNotFound,
		},
		{
			name:         "exact_match_resource",
			resourcePath: "items/special",
			requestPath:  "/items/special",
			wantStatus:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var captured []byte
			h, e, apiID := setupProxyAPIWithResource(t, tt.resourcePath, "AWS_PROXY", "fn")
			h.SetLambdaInvoker(&captureInvoker{capture: &captured})

			rec := userReq(t, h, e, apiID, tt.requestPath)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.checkEvent != nil {
				tt.checkEvent(t, captured)
			}
		})
	}
}

func TestMockIntegration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "default_200",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e, apiID := setupProxyAPIWithResource(t, "items", "MOCK", "")
			rec := userReq(t, h, e, apiID, "/items")
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHTTPProxyIntegration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		intType    string
		serverResp string
		wantBody   string
		serverCode int
		wantStatus int
	}{
		{
			name:       "http_proxy_success",
			intType:    "HTTP_PROXY",
			serverResp: `{"result":"upstream"}`,
			serverCode: http.StatusOK,
			wantStatus: http.StatusOK,
			wantBody:   `{"result":"upstream"}`,
		},
		{
			name:       "http_proxy_upstream_not_found",
			intType:    "HTTP_PROXY",
			serverResp: `not found`,
			serverCode: http.StatusNotFound,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "http_integration_success",
			intType:    "HTTP",
			serverResp: `{"ok":true}`,
			serverCode: http.StatusOK,
			wantStatus: http.StatusOK,
			wantBody:   `{"ok":true}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Start a local upstream server.
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.serverCode)
				_, _ = w.Write([]byte(tt.serverResp))
			}))
			defer upstream.Close()

			h, e, apiID := setupProxyAPIWithResource(t, "items", tt.intType, upstream.URL+"/items")
			h.SetHTTPClient(upstream.Client())

			rec := userReq(t, h, e, apiID, "/items")
			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Equal(t, tt.wantBody, rec.Body.String())
			}
		})
	}
}

func TestHTTPProxyIntegration_BadURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		uri        string
		wantStatus int
	}{
		{
			name:       "invalid_uri",
			uri:        "://invalid-uri",
			wantStatus: http.StatusBadGateway,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e, apiID := setupProxyAPIWithResource(t, "items", "HTTP_PROXY", tt.uri)
			rec := userReq(t, h, e, apiID, "/items")
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestExtractLambdaFunctionName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		uri    string
		wantFn string
	}{
		{
			name:   "plain_name",
			uri:    "my-function",
			wantFn: "my-function",
		},
		{
			name:   "lambda_arn",
			uri:    "arn:aws:lambda:us-east-1:123456789012:function:my-function",
			wantFn: "my-function",
		},
		{
			name:   "lambda_arn_with_qualifier",
			uri:    "arn:aws:lambda:us-east-1:123456789012:function:my-function:prod",
			wantFn: "my-function:prod",
		},
		{
			name: "apigateway_invoke_uri",
			uri: "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/" +
				"arn:aws:lambda:us-east-1:123456789012:function:my-function/invocations",
			wantFn: "my-function",
		},
		{
			name: "apigateway_invoke_uri_with_qualifier",
			uri: "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/" +
				"arn:aws:lambda:us-east-1:123456789012:function:my-function:prod/invocations",
			wantFn: "my-function:prod",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := apigateway.ExtractLambdaFunctionName(tt.uri)
			assert.Equal(t, tt.wantFn, got)
		})
	}
}

// captureAuthInvoker records the payload and returns a configurable response.
type captureAuthInvoker struct {
	returnError error
	capturedFn  string
	response    []byte
}

func (m *captureAuthInvoker) InvokeFunction(_ context.Context, fn, _ string, _ []byte) ([]byte, int, error) {
	m.capturedFn = fn
	if m.returnError != nil {
		return nil, http.StatusInternalServerError, m.returnError
	}

	if m.response != nil {
		return m.response, http.StatusOK, nil
	}

	return []byte(`{}`), http.StatusOK, nil
}

// setupAuthorizerAPI creates an API with a /secure resource associated with an authorizer.
// Returns (handler, echoEngine, apiID).
func setupAuthorizerAPI(
	t *testing.T,
	authType string,
) (*apigateway.Handler, *echo.Echo, string) {
	t.Helper()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	e := echo.New()

	// Create REST API.
	createRec := postWithHandler(t, h, e, "CreateRestApi", `{"name":"auth-api","description":"test"}`)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	apiID := createResp["id"].(string)

	// Get root resource.
	listRec := postWithHandler(t, h, e, "GetResources", `{"restApiId":"`+apiID+`"}`)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	rootID := listResp["item"].([]any)[0].(map[string]any)["id"].(string)

	// Create /secure resource.
	childRec := postWithHandler(t, h, e, "CreateResource",
		`{"restApiId":"`+apiID+`","parentId":"`+rootID+`","pathPart":"secure"}`)
	require.Equal(t, http.StatusCreated, childRec.Code)

	var childResp map[string]any
	require.NoError(t, json.Unmarshal(childRec.Body.Bytes(), &childResp))
	childID := childResp["id"].(string)

	// Create authorizer.
	authBody := `{
"restApiId":"` + apiID + `",
"name":"test-auth",
"type":"` + authType + `",
"identitySource":"method.request.header.Authorization",
"authorizerUri":"arn:aws:lambda:us-east-1:123:function:authFn"
}`
	authRec := postWithHandler(t, h, e, "CreateAuthorizer", authBody)
	require.Equal(t, http.StatusCreated, authRec.Code)

	var authResp map[string]any
	require.NoError(t, json.Unmarshal(authRec.Body.Bytes(), &authResp))
	authID := authResp["id"].(string)

	// PutMethod with authorizerId.
	methodBody := `{
"restApiId":"` + apiID + `",
"resourceId":"` + childID + `",
"httpMethod":"GET",
"authorizationType":"CUSTOM",
"authorizerId":"` + authID + `"
}`
	methodRec := postWithHandler(t, h, e, "PutMethod", methodBody)
	require.Equal(t, http.StatusCreated, methodRec.Code)

	// PutIntegration (MOCK so we can test the authorizer in isolation).
	integBody := `{
"restApiId":"` + apiID + `",
"resourceId":"` + childID + `",
"httpMethod":"GET",
"type":"MOCK"
}`
	integRec := postWithHandler(t, h, e, "PutIntegration", integBody)
	require.Equal(t, http.StatusCreated, integRec.Code)

	// CreateDeployment.
	deplRec := postWithHandler(t, h, e, "CreateDeployment",
		`{"restApiId":"`+apiID+`","stageName":"prod","description":"v1"}`)
	require.Equal(t, http.StatusCreated, deplRec.Code)

	return h, e, apiID
}

// allowPolicy returns a standard "Allow" IAM policy response from a Lambda authorizer.
func allowPolicy() []byte {
	return []byte(`{"principalId":"u","policyDocument":{"Statement":[` +
		`{"Effect":"Allow","Action":"execute-api:Invoke","Resource":"arn:*"}]}}`)
}

// denyPolicy returns a standard "Deny" IAM policy response from a Lambda authorizer.
func denyPolicy() []byte {
	return []byte(`{"principalId":"u","policyDocument":{"Statement":[` +
		`{"Effect":"Deny","Action":"execute-api:Invoke","Resource":"arn:*"}]}}`)
}

// authReq sends a GET request with an optional Authorization header to /restapis/.../prod/_user_request_/secure.
func authReq(
	t *testing.T,
	h *apigateway.Handler,
	e *echo.Echo,
	apiID, token string,
) *httptest.ResponseRecorder {
	t.Helper()

	url := "/restapis/" + apiID + "/prod/_user_request_/secure"
	req := httptest.NewRequest(http.MethodGet, url, nil)

	if token != "" {
		req.Header.Set("Authorization", token)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestRunAuthorizer_TOKEN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		invokerErr  error
		name        string
		invokerResp []byte
		wantStatus  int
		setInvoker  bool
	}{
		{
			name:        "allow_returns_200",
			invokerResp: allowPolicy(),
			setInvoker:  true,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "deny_returns_403",
			invokerResp: denyPolicy(),
			setInvoker:  true,
			wantStatus:  http.StatusForbidden,
		},
		{
			name:       "no_lambda_invoker_returns_503",
			setInvoker: false,
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:       "invocation_failure_returns_401",
			invokerErr: errLambdaError,
			setInvoker: true,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:        "malformed_response_returns_401",
			invokerResp: []byte("not json"),
			setInvoker:  true,
			wantStatus:  http.StatusUnauthorized,
		},
		{
			name:        "empty_policy_document_returns_403",
			invokerResp: []byte(`{"principalId":"u","policyDocument":{"Statement":[]}}`),
			setInvoker:  true,
			wantStatus:  http.StatusForbidden,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e, apiID := setupAuthorizerAPI(t, "TOKEN")

			if tt.setInvoker {
				h.SetLambdaInvoker(&captureAuthInvoker{
					response:    tt.invokerResp,
					returnError: tt.invokerErr,
				})
			}

			rec := authReq(t, h, e, apiID, "Bearer test-token")
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestRunAuthorizer_REQUEST(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		invokerResp []byte
		wantStatus  int
	}{
		{
			name:        "allow_returns_200",
			invokerResp: allowPolicy(),
			wantStatus:  http.StatusOK,
		},
		{
			name:        "deny_returns_403",
			invokerResp: denyPolicy(),
			wantStatus:  http.StatusForbidden,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e, apiID := setupAuthorizerAPI(t, "REQUEST")
			h.SetLambdaInvoker(&captureAuthInvoker{response: tt.invokerResp})

			rec := authReq(t, h, e, apiID, "Bearer test-token")
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestRunAuthorizer_MethodArn_ResourcePath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "method_arn_uses_stripped_resource_path"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var capturedPayload []byte

			// Use a captureInvoker to record the authorizer event payload, but return allow.
			capture := &captureAuthInvokerWithCapture{
				capturedPayload: &capturedPayload,
				response:        allowPolicy(),
			}

			h, e, apiID := setupAuthorizerAPI(t, "TOKEN")
			h.SetLambdaInvoker(capture)

			rec := authReq(t, h, e, apiID, "Bearer token")
			assert.Equal(t, http.StatusOK, rec.Code)

			// Verify the methodArn in the captured event uses the stripped resource path ("/secure"),
			// not the full proxy path ("/restapis/.../prod/_user_request_/secure").
			var event map[string]any
			require.NoError(t, json.Unmarshal(capturedPayload, &event))
			methodArn, _ := event["methodArn"].(string)
			assert.Contains(t, methodArn, "/secure", "methodArn should contain the stripped path")
			assert.NotContains(t, methodArn, "_user_request_", "methodArn must not contain internal proxy prefix")
		})
	}
}

func TestRunAuthorizer_Cache(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "second_request_uses_cache"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			invoker := &captureAuthInvoker{response: allowPolicy()}
			h, e, apiID := setupAuthorizerAPI(t, "TOKEN")

			calls := 0
			h.SetLambdaInvoker(&trackingInvoker{inner: invoker, calls: &calls})

			// First request - should call Lambda.
			rec1 := authReq(t, h, e, apiID, "Bearer cached-token")
			assert.Equal(t, http.StatusOK, rec1.Code)
			assert.Equal(t, 1, calls, "first request should call Lambda once")

			// Second request with same token - should hit cache.
			rec2 := authReq(t, h, e, apiID, "Bearer cached-token")
			assert.Equal(t, http.StatusOK, rec2.Code)
			assert.Equal(t, 1, calls, "second request should use cache, not call Lambda again")
		})
	}
}

// trackingInvoker wraps an invoker and counts calls.
type trackingInvoker struct {
	inner apigateway.LambdaInvoker
	calls *int
}

func (t *trackingInvoker) InvokeFunction(ctx context.Context, fn, invType string, payload []byte) ([]byte, int, error) {
	*t.calls++

	return t.inner.InvokeFunction(ctx, fn, invType, payload)
}

// captureAuthInvokerWithCapture records the event payload and returns a configurable response.
type captureAuthInvokerWithCapture struct {
	capturedPayload *[]byte
	response        []byte
}

func (m *captureAuthInvokerWithCapture) InvokeFunction(
	_ context.Context,
	_, _ string,
	payload []byte,
) ([]byte, int, error) {
	if m.capturedPayload != nil {
		*m.capturedPayload = make([]byte, len(payload))
		copy(*m.capturedPayload, payload)
	}

	return m.response, http.StatusOK, nil
}
