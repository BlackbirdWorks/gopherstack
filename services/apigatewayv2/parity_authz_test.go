package apigatewayv2_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

const (
	authFnName   = "arn:aws:lambda:us-east-1:123456789012:function:auth-fn"
	backendFnURI = "arn:aws:lambda:us-east-1:123456789012:function:backend-fn/invocations"
	// errTypeHeaderKey is the canonical form of the x-amzn-ErrorType header
	// (Go's http.Header canonicalises header keys to this casing).
	errTypeHeaderKey = "X-Amzn-Errortype"
	sigV4AuthHeader  = "AWS4-HMAC-SHA256 Credential=AKID/20230101/us-east-1/execute-api/aws4_request"
)

// authScenario describes a Lambda authorizer configuration and its response.
type authScenario struct {
	authResponse      map[string]any
	payloadVersion    string
	identitySource    []string
	ttlSeconds        int
	enableSimple      bool
	authorizerMissing bool
}

// setupRequestAuthAPI creates an HTTP API with an AWS_PROXY integration, a
// REQUEST authorizer, and a CUSTOM-authorized route. It returns the api id and
// an atomic counter that records how many times the authorizer Lambda is
// invoked (integration invocations are not counted).
func setupRequestAuthAPI(
	t *testing.T,
	h *apigatewayv2.Handler,
	sc authScenario,
) (string, *atomic.Int64) {
	t.Helper()

	const routeKey = "GET /secure"

	var authCalls atomic.Int64

	respBytes, _ := json.Marshal(sc.authResponse)

	h.SetLambdaInvoker(&mockLambdaInvoker{
		fn: func(_ context.Context, name, _ string, _ []byte) ([]byte, int, error) {
			if strings.Contains(name, "auth-fn") {
				authCalls.Add(1)

				return respBytes, 200, nil
			}

			// Integration backend function.
			b, _ := json.Marshal(map[string]any{"statusCode": 200, "body": "ok"})

			return b, 200, nil
		},
	})

	apiID := createAPI(t, h, "authz-api")

	// Integration.
	rr := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/integrations", map[string]any{
		"integrationType":      "AWS_PROXY",
		"integrationUri":       backendFnURI,
		"payloadFormatVersion": "2.0",
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	var integ apigatewayv2.Integration
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &integ))

	// Authorizer.
	identity := sc.identitySource
	if identity == nil {
		identity = []string{"$request.header.Authorization"}
	}

	authBody := map[string]any{
		"name":                         "req-authorizer",
		"authorizerType":               "REQUEST",
		"authorizerUri":                authFnName,
		"identitySource":               identity,
		"authorizerResultTtlInSeconds": sc.ttlSeconds,
		"enableSimpleResponses":        sc.enableSimple,
	}
	if sc.payloadVersion != "" {
		authBody["authorizerPayloadFormatVersion"] = sc.payloadVersion
	}

	rr = doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/authorizers", authBody)
	require.Equal(t, http.StatusCreated, rr.Code)

	var auth apigatewayv2.Authorizer
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &auth))

	authorizerID := auth.AuthorizerID
	if sc.authorizerMissing {
		authorizerID = "does-not-exist"
	}

	// Route with CUSTOM authorization.
	rr = doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/routes", map[string]any{
		"routeKey":          routeKey,
		"target":            "integrations/" + integ.IntegrationID,
		"authorizationType": "CUSTOM",
		"authorizerId":      authorizerID,
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	return apiID, &authCalls
}

func TestRequestAuthorizer_SimpleResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		isAuth     bool
		wantStatus int
	}{
		{name: "simple_allow", isAuth: true, wantStatus: http.StatusOK},
		{name: "simple_deny", isAuth: false, wantStatus: http.StatusForbidden},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID, authCalls := setupRequestAuthAPI(t, h, authScenario{
				payloadVersion: "2.0",
				enableSimple:   true,
				ttlSeconds:     0,
				authResponse:   map[string]any{"isAuthorized": tt.isAuth},
			})

			rr := doProxyRequest(t, h, http.MethodGet, apiID, "/secure", map[string]string{
				"Authorization": "Bearer token-123",
			})

			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Equal(t, int64(1), authCalls.Load())

			if tt.wantStatus == http.StatusForbidden {
				assert.Equal(t, "AccessDeniedException", rr.Header().Get(errTypeHeaderKey))
			}
		})
	}
}

func TestRequestAuthorizer_IAMPolicyResponse(t *testing.T) {
	t.Parallel()

	allowStmt := func(effect, resource string) map[string]any {
		return map[string]any{
			"principalId": "user-1",
			"policyDocument": map[string]any{
				"Version": "2012-10-17",
				"Statement": []any{
					map[string]any{
						"Effect":   effect,
						"Action":   "execute-api:Invoke",
						"Resource": resource,
					},
				},
			},
		}
	}

	tests := []struct {
		response   map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "policy_allow_wildcard",
			response:   allowStmt("Allow", "*"),
			wantStatus: http.StatusOK,
		},
		{
			name:       "policy_explicit_deny",
			response:   allowStmt("Deny", "*"),
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "policy_no_matching_resource_implicit_deny",
			response:   allowStmt("Allow", "arn:aws:execute-api:us-east-1:123456789012:other/*/GET/nope"),
			wantStatus: http.StatusForbidden,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID, authCalls := setupRequestAuthAPI(t, h, authScenario{
				payloadVersion: "1.0",
				enableSimple:   false,
				authResponse:   tt.response,
			})

			rr := doProxyRequest(t, h, http.MethodGet, apiID, "/secure", map[string]string{
				"Authorization": "Bearer token-123",
			})

			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Equal(t, int64(1), authCalls.Load())
		})
	}
}

func TestRequestAuthorizer_MissingIdentitySource(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	apiID, authCalls := setupRequestAuthAPI(t, h, authScenario{
		payloadVersion: "2.0",
		enableSimple:   true,
		identitySource: []string{"$request.header.Authorization"},
		authResponse:   map[string]any{"isAuthorized": true},
	})

	// No Authorization header → required identity source missing → 401, and the
	// authorizer Lambda must not be invoked.
	rr := doProxyRequest(t, h, http.MethodGet, apiID, "/secure", nil)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
	assert.Equal(t, "UnauthorizedException", rr.Header().Get(errTypeHeaderKey))
	assert.Equal(t, int64(0), authCalls.Load())
}

func TestRequestAuthorizer_CachingTTL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ttl        int
		wantCalls  int64
		numRequest int
	}{
		{name: "cache_hit_ttl_positive", ttl: 300, numRequest: 3, wantCalls: 1},
		{name: "no_cache_ttl_zero", ttl: 0, numRequest: 3, wantCalls: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID, authCalls := setupRequestAuthAPI(t, h, authScenario{
				payloadVersion: "2.0",
				enableSimple:   true,
				ttlSeconds:     tt.ttl,
				authResponse:   map[string]any{"isAuthorized": true},
			})

			for range tt.numRequest {
				rr := doProxyRequest(t, h, http.MethodGet, apiID, "/secure", map[string]string{
					"Authorization": "Bearer token-abc",
				})
				require.Equal(t, http.StatusOK, rr.Code)
			}

			assert.Equal(t, tt.wantCalls, authCalls.Load())
		})
	}
}

func TestRequestAuthorizer_MissingAuthorizerRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	apiID, _ := setupRequestAuthAPI(t, h, authScenario{
		payloadVersion:    "2.0",
		enableSimple:      true,
		authorizerMissing: true,
		authResponse:      map[string]any{"isAuthorized": true},
	})

	rr := doProxyRequest(t, h, http.MethodGet, apiID, "/secure", map[string]string{
		"Authorization": "Bearer token-123",
	})

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestRouteAuth_AWSIAM(t *testing.T) {
	t.Parallel()

	tests := []struct {
		headers    map[string]string
		query      string
		name       string
		wantStatus int
	}{
		{
			name:       "unsigned_request_rejected",
			headers:    nil,
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "sigv4_signed_allowed",
			headers:    map[string]string{"Authorization": sigV4AuthHeader},
			wantStatus: http.StatusOK,
		},
		{
			name:       "presigned_query_allowed",
			query:      "?X-Amz-Algorithm=AWS4-HMAC-SHA256",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			h.SetLambdaInvoker(&mockLambdaInvoker{})

			apiID := createAPI(t, h, "iam-api")

			rr := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/integrations", map[string]any{
				"integrationType":      "AWS_PROXY",
				"integrationUri":       backendFnURI,
				"payloadFormatVersion": "2.0",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			var integ apigatewayv2.Integration
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &integ))

			rr = doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/routes", map[string]any{
				"routeKey":          "GET /iam",
				"target":            "integrations/" + integ.IntegrationID,
				"authorizationType": "AWS_IAM",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			rr = doProxyRequest(t, h, http.MethodGet, apiID, "/iam"+tt.query, tt.headers)
			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusForbidden {
				assert.Equal(t, "AccessDeniedException", rr.Header().Get(errTypeHeaderKey))
			}
		})
	}
}

func TestCreateRoute_AuthorizationTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		authType   string
		wantStatus int
	}{
		{name: "none_ok", authType: "NONE", wantStatus: http.StatusCreated},
		{name: "aws_iam_ok", authType: "AWS_IAM", wantStatus: http.StatusCreated},
		{name: "invalid_rejected", authType: "BOGUS", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "route-auth-api")

			rr := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/routes", map[string]any{
				"routeKey":          "GET /r",
				"authorizationType": tt.authType,
			})

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusBadRequest {
				assert.Equal(t, "BadRequestException", rr.Header().Get(errTypeHeaderKey))
			}
		})
	}
}
