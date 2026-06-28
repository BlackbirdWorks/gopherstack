package apigatewayv2_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

var errShouldNotBeCalled = errors.New("should not be called")

// mockLambdaInvoker is a stub LambdaInvoker for tests.
type mockLambdaInvoker struct {
	fn func(ctx context.Context, name, invocationType string, payload []byte) ([]byte, int, error)
}

func (m *mockLambdaInvoker) InvokeFunction(
	ctx context.Context,
	name, invocationType string,
	payload []byte,
) ([]byte, int, error) {
	if m.fn != nil {
		return m.fn(ctx, name, invocationType, payload)
	}

	resp := map[string]any{"statusCode": 200, "body": "ok"}
	b, _ := json.Marshal(resp)

	return b, 200, nil
}

// doProxyRequest sends an HTTP request to the v2proxy data plane.
func doProxyRequest(
	t *testing.T,
	h *apigatewayv2.Handler,
	method, apiID, path string,
	headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	proxyPath := fmt.Sprintf("/v2proxy/%s/$default%s", apiID, path)
	req := httptest.NewRequest(method, proxyPath, strings.NewReader(""))

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	rr := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rr)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rr
}

// buildHTTPAPI creates an HTTP API with one route pointing to an AWS_PROXY Lambda integration.
// Returns apiID.
func buildHTTPAPIWithLambda(
	t *testing.T,
	h *apigatewayv2.Handler,
	routeKey string,
	lambdaURI string,
) string {
	t.Helper()

	// Create API.
	rr := doRequest(t, h, http.MethodPost, "/v2/apis", map[string]any{
		"name":         "test-api",
		"protocolType": "HTTP",
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	var api apigatewayv2.API
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &api))

	theAPIID := api.APIID

	// Create integration.
	rr = doRequest(t, h, http.MethodPost, "/v2/apis/"+theAPIID+"/integrations", map[string]any{
		"integrationType":      "AWS_PROXY",
		"integrationUri":       lambdaURI,
		"payloadFormatVersion": "2.0",
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	var integ apigatewayv2.Integration
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &integ))

	theIntegrationID := integ.IntegrationID

	// Create route.
	rr = doRequest(t, h, http.MethodPost, "/v2/apis/"+theAPIID+"/routes", map[string]any{
		"routeKey": routeKey,
		"target":   "integrations/" + theIntegrationID,
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	return theAPIID
}

// TestHTTPAPIProxy_RouteMatching verifies that the HTTP API data plane correctly
// matches routes, including literal paths, path params, $default fallback, and ANY method.
func TestHTTPAPIProxy_RouteMatching(t *testing.T) {
	t.Parallel()

	const lambdaURI = "arn:aws:lambda:us-east-1:123456789012:function:my-fn/invocations"

	tests := []struct {
		name          string
		requestMethod string
		requestPath   string
		wantRouteKey  string
		setupRoutes   []string
		wantStatus    int
	}{
		{
			name:          "literal_route_exact_match",
			setupRoutes:   []string{"GET /hello"},
			requestMethod: http.MethodGet,
			requestPath:   "/hello",
			wantStatus:    http.StatusOK,
			wantRouteKey:  "GET /hello",
		},
		{
			name:          "path_param_route",
			setupRoutes:   []string{"GET /users/{id}"},
			requestMethod: http.MethodGet,
			requestPath:   "/users/42",
			wantStatus:    http.StatusOK,
			wantRouteKey:  "GET /users/{id}",
		},
		{
			name:          "default_fallback",
			setupRoutes:   []string{"$default"},
			requestMethod: http.MethodPost,
			requestPath:   "/anything/goes",
			wantStatus:    http.StatusOK,
			wantRouteKey:  "$default",
		},
		{
			name:          "any_method_wildcard",
			setupRoutes:   []string{"ANY /items"},
			requestMethod: http.MethodDelete,
			requestPath:   "/items",
			wantStatus:    http.StatusOK,
			wantRouteKey:  "ANY /items",
		},
		{
			name:          "literal_beats_param",
			setupRoutes:   []string{"GET /users/{id}", "GET /users/me"},
			requestMethod: http.MethodGet,
			requestPath:   "/users/me",
			wantStatus:    http.StatusOK,
			wantRouteKey:  "GET /users/me",
		},
		{
			name:          "no_match_no_default",
			setupRoutes:   []string{"GET /specific"},
			requestMethod: http.MethodGet,
			requestPath:   "/other",
			wantStatus:    http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Capture which routeKey Lambda receives.
			var capturedRouteKey string

			lambda := &mockLambdaInvoker{
				fn: func(_ context.Context, _, _ string, payload []byte) ([]byte, int, error) {
					var ev map[string]any
					if err := json.Unmarshal(payload, &ev); err == nil {
						capturedRouteKey, _ = ev["routeKey"].(string)
					}

					resp := map[string]any{"statusCode": 200, "body": "routed"}
					b, _ := json.Marshal(resp)

					return b, 200, nil
				},
			}

			h.SetLambdaInvoker(lambda)

			apiID := buildHTTPAPIWithLambda(t, h, tt.setupRoutes[0], lambdaURI)

			// Create extra routes if provided.
			for _, rk := range tt.setupRoutes[1:] {
				rr := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/integrations", map[string]any{
					"integrationType":      "AWS_PROXY",
					"integrationUri":       lambdaURI,
					"payloadFormatVersion": "2.0",
				})
				require.Equal(t, http.StatusCreated, rr.Code)

				var integ apigatewayv2.Integration
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &integ))

				rr = doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/routes", map[string]any{
					"routeKey": rk,
					"target":   "integrations/" + integ.IntegrationID,
				})
				require.Equal(t, http.StatusCreated, rr.Code)
			}

			rr := doProxyRequest(t, h, tt.requestMethod, apiID, tt.requestPath, nil)

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantRouteKey != "" {
				assert.Equal(t, tt.wantRouteKey, capturedRouteKey)
			}
		})
	}
}

// TestHTTPAPIProxy_PayloadFormat verifies both format 1.0 and 2.0 payloads are built correctly.
func TestHTTPAPIProxy_PayloadFormat(t *testing.T) {
	t.Parallel()

	const lambdaURI = "arn:aws:lambda:us-east-1:123456789012:function:pf-fn/invocations"

	tests := []struct {
		name         string
		pfv          string
		wantVersion  string
		wantEventKey string
	}{
		{
			name:         "format_2_0",
			pfv:          "2.0",
			wantVersion:  "2.0",
			wantEventKey: "routeKey",
		},
		{
			name:         "format_1_0",
			pfv:          "1.0",
			wantEventKey: "httpMethod",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			var capturedPayload map[string]any
			lambda := &mockLambdaInvoker{
				fn: func(_ context.Context, _, _ string, payload []byte) ([]byte, int, error) {
					_ = json.Unmarshal(payload, &capturedPayload)

					resp := map[string]any{"statusCode": 200, "body": "ok"}
					b, _ := json.Marshal(resp)

					return b, 200, nil
				},
			}
			h.SetLambdaInvoker(lambda)

			// Create API + integration with specific PFV.
			rr := doRequest(t, h, http.MethodPost, "/v2/apis", map[string]any{
				"name": "pf-api", "protocolType": "HTTP",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			var api apigatewayv2.API
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &api))

			rr = doRequest(t, h, http.MethodPost, "/v2/apis/"+api.APIID+"/integrations", map[string]any{
				"integrationType":      "AWS_PROXY",
				"integrationUri":       lambdaURI,
				"payloadFormatVersion": tt.pfv,
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			var integ apigatewayv2.Integration
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &integ))

			rr = doRequest(t, h, http.MethodPost, "/v2/apis/"+api.APIID+"/routes", map[string]any{
				"routeKey": "GET /test",
				"target":   "integrations/" + integ.IntegrationID,
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			doProxyRequest(t, h, http.MethodGet, api.APIID, "/test", nil)

			require.NotNil(t, capturedPayload)
			_, hasKey := capturedPayload[tt.wantEventKey]
			assert.True(t, hasKey, "expected event key %q in payload", tt.wantEventKey)

			if tt.wantVersion != "" {
				assert.Equal(t, tt.wantVersion, capturedPayload["version"])
			}
		})
	}
}

// TestHTTPAPIProxy_CORSPreflight verifies that OPTIONS requests return CORS headers
// without hitting the Lambda integration.
func TestHTTPAPIProxy_CORSPreflight(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	lambdaCalled := false
	h.SetLambdaInvoker(&mockLambdaInvoker{
		fn: func(_ context.Context, _, _ string, _ []byte) ([]byte, int, error) {
			lambdaCalled = true

			return nil, 0, errShouldNotBeCalled
		},
	})

	// Create API with CORS.
	rr := doRequest(t, h, http.MethodPost, "/v2/apis", map[string]any{
		"name":         "cors-api",
		"protocolType": "HTTP",
		"corsConfiguration": map[string]any{
			"allowOrigins": []string{"https://example.com"},
			"allowMethods": []string{"GET", "POST"},
			"allowHeaders": []string{"Content-Type"},
		},
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	var api apigatewayv2.API
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &api))

	// Create a route (even though OPTIONS won't use it).
	rr = doRequest(t, h, http.MethodPost, "/v2/apis/"+api.APIID+"/integrations", map[string]any{
		"integrationType": "AWS_PROXY",
		"integrationUri":  "arn:aws:lambda:us-east-1:123456789012:function:fn/invocations",
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	var integ apigatewayv2.Integration
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &integ))

	doRequest(t, h, http.MethodPost, "/v2/apis/"+api.APIID+"/routes", map[string]any{
		"routeKey": "GET /items",
		"target":   "integrations/" + integ.IntegrationID,
	})

	// OPTIONS preflight.
	rr = doProxyRequest(t, h, http.MethodOptions, api.APIID, "/items",
		map[string]string{"Origin": "https://example.com"})

	assert.Equal(t, http.StatusNoContent, rr.Code)
	assert.Contains(t, rr.Header().Get("Access-Control-Allow-Origin"), "example.com")
	assert.False(t, lambdaCalled, "Lambda must not be called for CORS preflight")
}

// TestHTTPAPIProxy_JWTAuthorizer verifies that JWT authorization blocks requests
// without a valid token.
func TestHTTPAPIProxy_JWTAuthorizer(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	h.SetLambdaInvoker(&mockLambdaInvoker{})

	// Create API.
	rr := doRequest(t, h, http.MethodPost, "/v2/apis", map[string]any{
		"name": "jwt-api", "protocolType": "HTTP",
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	var api apigatewayv2.API
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &api))

	// Create JWT authorizer.
	rr = doRequest(t, h, http.MethodPost, "/v2/apis/"+api.APIID+"/authorizers", map[string]any{
		"name":           "jwt-auth",
		"authorizerType": "JWT",
		"identitySource": []string{"$request.header.Authorization"},
		"jwtConfiguration": map[string]any{
			"issuer":   "https://example.com/",
			"audience": []string{"my-audience"},
		},
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	var authorizer apigatewayv2.Authorizer
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &authorizer))

	// Create integration + route with JWT auth.
	rr = doRequest(t, h, http.MethodPost, "/v2/apis/"+api.APIID+"/integrations", map[string]any{
		"integrationType": "AWS_PROXY",
		"integrationUri":  "arn:aws:lambda:us-east-1:123456789012:function:fn/invocations",
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	var integ apigatewayv2.Integration
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &integ))

	rr = doRequest(t, h, http.MethodPost, "/v2/apis/"+api.APIID+"/routes", map[string]any{
		"routeKey":          "GET /secure",
		"authorizationType": "JWT",
		"authorizerId":      authorizer.AuthorizerID,
		"target":            "integrations/" + integ.IntegrationID,
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	// Request without Authorization header → 401.
	rr = doProxyRequest(t, h, http.MethodGet, api.APIID, "/secure", nil)
	assert.Equal(t, http.StatusUnauthorized, rr.Code)

	// Request with a garbage token → 401.
	rr = doProxyRequest(t, h, http.MethodGet, api.APIID, "/secure",
		map[string]string{"Authorization": "Bearer not-a-jwt"})
	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

// TestHTTPAPIProxy_LambdaResponse verifies that Lambda response fields (status code,
// headers, body) are correctly forwarded to the HTTP client.
func TestHTTPAPIProxy_LambdaResponse(t *testing.T) {
	t.Parallel()

	const lambdaURI = "arn:aws:lambda:us-east-1:123456789012:function:resp-fn/invocations"

	tests := []struct {
		name       string
		lambdaBody map[string]any
		wantBody   string
		wantHeader string
		wantHdrVal string
		wantStatus int
	}{
		{
			name:       "200_with_body",
			lambdaBody: map[string]any{"statusCode": 200, "body": `{"ok":true}`},
			wantStatus: http.StatusOK,
			wantBody:   `{"ok":true}`,
		},
		{
			name:       "404_response",
			lambdaBody: map[string]any{"statusCode": 404, "body": "not found"},
			wantStatus: http.StatusNotFound,
			wantBody:   "not found",
		},
		{
			name: "custom_header",
			lambdaBody: map[string]any{
				"statusCode": 200,
				"body":       "ok",
				"headers":    map[string]any{"X-Custom": "hello"},
			},
			wantStatus: http.StatusOK,
			wantHeader: "X-Custom",
			wantHdrVal: "hello",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			lambda := &mockLambdaInvoker{
				fn: func(_ context.Context, _, _ string, _ []byte) ([]byte, int, error) {
					b, _ := json.Marshal(tt.lambdaBody)

					return b, 200, nil
				},
			}
			h.SetLambdaInvoker(lambda)

			apiID := buildHTTPAPIWithLambda(t, h, "GET /check", lambdaURI)
			rr := doProxyRequest(t, h, http.MethodGet, apiID, "/check", nil)

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantBody != "" {
				assert.Equal(t, tt.wantBody, rr.Body.String())
			}

			if tt.wantHeader != "" {
				assert.Equal(t, tt.wantHdrVal, rr.Header().Get(tt.wantHeader))
			}
		})
	}
}

// TestHTTPAPIProxy_PathParameters verifies that path parameters extracted during
// route matching are forwarded in the Lambda event.
func TestHTTPAPIProxy_PathParameters(t *testing.T) {
	t.Parallel()

	const lambdaURI = "arn:aws:lambda:us-east-1:123456789012:function:pp-fn/invocations"

	h := newTestHandler()

	var capturedPathParams map[string]any

	h.SetLambdaInvoker(&mockLambdaInvoker{
		fn: func(_ context.Context, _, _ string, payload []byte) ([]byte, int, error) {
			var ev map[string]any
			if err := json.Unmarshal(payload, &ev); err == nil {
				if pp, ok := ev["pathParameters"].(map[string]any); ok {
					capturedPathParams = pp
				}
			}

			b, _ := json.Marshal(map[string]any{"statusCode": 200, "body": "ok"})

			return b, 200, nil
		},
	})

	apiID := buildHTTPAPIWithLambda(t, h, "GET /orgs/{orgId}/users/{userId}", lambdaURI)
	rr := doProxyRequest(t, h, http.MethodGet, apiID, "/orgs/acme/users/99", nil)

	require.Equal(t, http.StatusOK, rr.Code)
	require.NotNil(t, capturedPathParams)
	assert.Equal(t, "acme", capturedPathParams["orgId"])
	assert.Equal(t, "99", capturedPathParams["userId"])
}
