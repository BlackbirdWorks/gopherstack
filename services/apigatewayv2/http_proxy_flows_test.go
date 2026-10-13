package apigatewayv2_test

import (
	"context"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

const flowsLambdaURI = "arn:aws:lambda:us-east-1:123456789012:function:flow-fn/invocations"

type flowAPI struct {
	h     *apigatewayv2.Handler
	apiID string
}

func newFlowAPI(t *testing.T, cors map[string]any) *flowAPI {
	t.Helper()

	h := newTestHandler()
	body := map[string]any{"name": "flow-api", "protocolType": "HTTP"}

	if cors != nil {
		body["corsConfiguration"] = cors
	}

	rr := doRequest(t, h, http.MethodPost, "/v2/apis", body)
	require.Equal(t, http.StatusCreated, rr.Code)

	var api apigatewayv2.API
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &api))

	return &flowAPI{h: h, apiID: api.APIID}
}

func (f *flowAPI) integration(t *testing.T, body map[string]any) string {
	t.Helper()

	rr := doRequest(t, f.h, http.MethodPost, "/v2/apis/"+f.apiID+"/integrations", body)
	require.Equal(t, http.StatusCreated, rr.Code, rr.Body.String())

	var integ apigatewayv2.Integration
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &integ))

	return integ.IntegrationID
}

func (f *flowAPI) route(t *testing.T, body map[string]any) {
	t.Helper()

	rr := doRequest(t, f.h, http.MethodPost, "/v2/apis/"+f.apiID+"/routes", body)
	require.Equal(t, http.StatusCreated, rr.Code, rr.Body.String())
}

func (f *flowAPI) lambdaRoute(t *testing.T, routeKey, payloadVersion string, extra map[string]any) {
	t.Helper()

	id := f.integration(t, map[string]any{
		"integrationType": "AWS_PROXY", "integrationUri": flowsLambdaURI, "payloadFormatVersion": payloadVersion,
	})
	body := map[string]any{"routeKey": routeKey, "target": "integrations/" + id}

	maps.Copy(body, extra)

	f.route(t, body)
}

type recordedEvents struct {
	events []map[string]any
	mu     sync.Mutex
}

func (r *recordedEvents) last(t *testing.T) map[string]any {
	t.Helper()

	r.mu.Lock()
	defer r.mu.Unlock()

	require.NotEmpty(t, r.events)

	return r.events[len(r.events)-1]
}

func recordingInvoker(rec *recordedEvents, response string) *mockLambdaInvoker {
	return &mockLambdaInvoker{fn: func(_ context.Context, _, _ string, payload []byte) ([]byte, int, error) {
		var ev map[string]any
		if err := json.Unmarshal(payload, &ev); err == nil {
			rec.mu.Lock()
			rec.events = append(rec.events, ev)
			rec.mu.Unlock()
		}

		return []byte(response), http.StatusOK, nil
	}}
}

func TestHTTPAPIProxy_LambdaResponseShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		version     string
		response    string
		wantBody    string
		wantType    string
		wantStatus  int
		wantCookies int
	}{
		{
			name: "v2 object without status code", version: "2.0", response: `{"hello":"world"}`,
			wantStatus: http.StatusOK, wantBody: `{"hello":"world"}`, wantType: "application/json",
		},
		{
			name: "v2 json string", version: "2.0", response: `"plain"`,
			wantStatus: http.StatusOK, wantBody: `"plain"`, wantType: "application/json",
		},
		{
			name: "v2 envelope defaults content type", version: "2.0", response: `{"statusCode":201,"body":"made"}`,
			wantStatus: http.StatusCreated, wantBody: "made", wantType: "application/json",
		},
		{
			name: "v2 envelope keeps content type", version: "2.0",
			response:   `{"statusCode":200,"headers":{"Content-Type":"text/plain"},"body":"hi"}`,
			wantStatus: http.StatusOK, wantBody: "hi", wantType: "text/plain",
		},
		{
			name: "v2 cookies", version: "2.0",
			response:   `{"statusCode":200,"cookies":["a=1","b=2"],"body":"x"}`,
			wantStatus: http.StatusOK, wantBody: "x", wantType: "application/json", wantCookies: 2,
		},
		{
			name: "base64 body", version: "2.0",
			response:   `{"statusCode":200,"isBase64Encoded":true,"body":"aGVsbG8="}`,
			wantStatus: http.StatusOK, wantBody: "hello", wantType: "application/json",
		},
		{
			name: "v1 multi value headers", version: "1.0",
			response:   `{"statusCode":200,"multiValueHeaders":{"Set-Cookie":["a=1","b=2"]},"body":"x"}`,
			wantStatus: http.StatusOK, wantBody: "x", wantType: "application/json", wantCookies: 2,
		},
		{
			name: "v1 without status code", version: "1.0", response: `{"hello":"world"}`,
			wantStatus: http.StatusInternalServerError, wantBody: `{"message":"Internal Server Error"}`,
			wantType: "application/json",
		},
		{
			name: "function error", version: "2.0",
			response:   `{"errorMessage":"boom","errorType":"Error"}`,
			wantStatus: http.StatusInternalServerError, wantBody: `{"message":"Internal Server Error"}`,
			wantType: "application/json",
		},
		{
			name: "invalid status code", version: "2.0", response: `{"statusCode":42,"body":"x"}`,
			wantStatus: http.StatusInternalServerError, wantBody: `{"message":"Internal Server Error"}`,
			wantType: "application/json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFlowAPI(t, nil)
			f.h.SetLambdaInvoker(recordingInvoker(&recordedEvents{}, tt.response))
			f.lambdaRoute(t, "GET /thing", tt.version, nil)

			rr := doProxyRequest(t, f.h, http.MethodGet, f.apiID, "/thing", nil)

			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Equal(t, tt.wantBody, rr.Body.String())
			assert.Equal(t, tt.wantType, rr.Header().Get("Content-Type"))
			assert.Len(t, rr.Header().Values("Set-Cookie"), tt.wantCookies)
		})
	}
}

func TestHTTPAPIProxy_LambdaEventShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check    func(t *testing.T, ev map[string]any)
		name     string
		version  string
		routeKey string
		path     string
		stage    string
	}{
		{
			name: "v1 event", version: "1.0", routeKey: "GET /items/{id}", path: "/items/5", stage: "$default",
			check: func(t *testing.T, ev map[string]any) {
				t.Helper()

				assert.Equal(t, "1.0", ev["version"])
				assert.Equal(t, "/items/{id}", ev["resource"])
				assert.Equal(t, "/items/5", ev["path"])
				assert.Equal(t, false, ev["isBase64Encoded"])
				assert.Contains(t, ev, "multiValueHeaders")
				assert.Equal(t, map[string]any{"id": "5"}, ev["pathParameters"])

				rc, _ := ev["requestContext"].(map[string]any)
				assert.Equal(t, "/items/{id}", rc["resourcePath"])
				assert.Contains(t, rc, "identity")
				assert.NotEmpty(t, rc["requestTime"])
			},
		},
		{
			name: "v2 named stage prefixes raw path", version: "2.0", routeKey: "GET /items/{id}",
			path: "/items/5", stage: "prod",
			check: func(t *testing.T, ev map[string]any) {
				t.Helper()

				assert.Equal(t, "/prod/items/5", ev["rawPath"])
				assert.Equal(t, false, ev["isBase64Encoded"])

				rc, _ := ev["requestContext"].(map[string]any)
				httpCtx, _ := rc["http"].(map[string]any)
				assert.Equal(t, "/prod/items/5", httpCtx["path"])
			},
		},
		{
			name: "v2 greedy parameter has no leading slash", version: "2.0", routeKey: "ANY /files/{proxy+}",
			path: "/files/a/b.txt", stage: "$default",
			check: func(t *testing.T, ev map[string]any) {
				t.Helper()

				assert.Equal(t, map[string]any{"proxy": "a/b.txt"}, ev["pathParameters"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFlowAPI(t, nil)
			rec := &recordedEvents{}
			f.h.SetLambdaInvoker(recordingInvoker(rec, `{"statusCode":200}`))
			f.lambdaRoute(t, tt.routeKey, tt.version, nil)

			rr := doRequest(t, f.h, http.MethodPost, "/v2/apis/"+f.apiID+"/stages",
				map[string]any{"stageName": tt.stage})
			require.Equal(t, http.StatusCreated, rr.Code)

			hdrs := map[string]string{"X-Test": "1"}
			rr = doProxyRequestToStage(t, f.h, http.MethodGet, f.apiID, tt.stage, tt.path, hdrs)
			require.Equal(t, http.StatusOK, rr.Code)

			tt.check(t, rec.last(t))
		})
	}
}

type cognitoFixture struct {
	backend  *cognitoidp.InMemoryBackend
	idToken  string
	access   string
	clientID string
	poolID   string
}

func newCognitoFixture(t *testing.T) *cognitoFixture {
	t.Helper()

	backend := cognitoidp.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")

	pool, err := backend.CreateUserPool("flow-pool")
	require.NoError(t, err)

	client, err := backend.CreateUserPoolClient(pool.ID, "flow-client")
	require.NoError(t, err)

	_, err = backend.SignUp(client.ClientID, "flowuser", "Password1!", nil)
	require.NoError(t, err)
	require.NoError(t, backend.AdminConfirmSignUp(pool.ID, "flowuser"))

	res, err := backend.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "flowuser", "Password1!")
	require.NoError(t, err)

	return &cognitoFixture{
		backend: backend, idToken: res.Tokens.IDToken, access: res.Tokens.AccessToken,
		clientID: client.ClientID, poolID: pool.ID,
	}
}

func TestHTTPAPIProxy_JWTAuthorizerCognitoFlow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		issuer     func(poolID string) string
		name       string
		scopes     []string
		wantStatus int
		useAccess  bool
	}{
		{
			name:       "emulator issuer id token",
			issuer:     func(p string) string { return "http://localhost:8000/" + p },
			wantStatus: http.StatusOK,
		},
		{
			name:       "aws style issuer id token",
			issuer:     func(p string) string { return "https://cognito-idp.us-east-1.amazonaws.com/" + p },
			wantStatus: http.StatusOK,
		},
		{
			name:       "access token audience is client id",
			issuer:     func(p string) string { return "https://cognito-idp.us-east-1.amazonaws.com/" + p },
			useAccess:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "route scope present",
			issuer:     func(p string) string { return "http://localhost:8000/" + p },
			useAccess:  true,
			scopes:     []string{"aws.cognito.signin.user.admin"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "route scope missing",
			issuer:     func(p string) string { return "http://localhost:8000/" + p },
			useAccess:  true,
			scopes:     []string{"orders/write"},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "other pool issuer",
			issuer:     func(string) string { return "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_Other" },
			wantStatus: http.StatusUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cog := newCognitoFixture(t)
			f := newFlowAPI(t, nil)
			f.h.SetJWKSProvider(cog.backend)

			rec := &recordedEvents{}
			f.h.SetLambdaInvoker(recordingInvoker(rec, `{"statusCode":200}`))

			rr := doRequest(t, f.h, http.MethodPost, "/v2/apis/"+f.apiID+"/authorizers", map[string]any{
				"name": "cog", "authorizerType": "JWT",
				"identitySource":   []string{"$request.header.Authorization"},
				"jwtConfiguration": map[string]any{"issuer": tt.issuer(cog.poolID), "audience": []string{cog.clientID}},
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			var authz apigatewayv2.Authorizer
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &authz))

			extra := map[string]any{"authorizationType": "JWT", "authorizerId": authz.AuthorizerID}
			if tt.scopes != nil {
				extra["authorizationScopes"] = tt.scopes
			}

			f.lambdaRoute(t, "GET /secure", "2.0", extra)

			token := cog.idToken
			if tt.useAccess {
				token = cog.access
			}

			hdrs := map[string]string{"Authorization": "Bearer " + token}
			rr = doProxyRequest(t, f.h, http.MethodGet, f.apiID, "/secure", hdrs)
			require.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus != http.StatusOK {
				return
			}

			rc, _ := rec.last(t)["requestContext"].(map[string]any)
			authzCtx, _ := rc["authorizer"].(map[string]any)
			jwtCtx, _ := authzCtx["jwt"].(map[string]any)
			claims, _ := jwtCtx["claims"].(map[string]any)

			assert.NotEmpty(t, claims["sub"])
			assert.Contains(t, jwtCtx, "scopes")
		})
	}
}

func TestHTTPAPIProxy_LambdaAuthorizerContext(t *testing.T) {
	t.Parallel()

	f := newFlowAPI(t, nil)
	rec := &recordedEvents{}

	integrationInvoker := recordingInvoker(rec, `{"statusCode":200}`)
	f.h.SetLambdaInvoker(&mockLambdaInvoker{fn: func(
		ctx context.Context, name, kind string, payload []byte,
	) ([]byte, int, error) {
		if strings.Contains(name, "authz") {
			return []byte(`{"isAuthorized":true,"context":{"tenant":"acme","level":3}}`), http.StatusOK, nil
		}

		return integrationInvoker.fn(ctx, name, kind, payload)
	}})

	rr := doRequest(t, f.h, http.MethodPost, "/v2/apis/"+f.apiID+"/authorizers", map[string]any{
		"name": "lam", "authorizerType": "REQUEST",
		"authorizerUri":                  "arn:aws:lambda:us-east-1:123456789012:function:authz/invocations",
		"authorizerPayloadFormatVersion": "2.0", "enableSimpleResponses": true,
		"identitySource": []string{"$request.header.X-Token"},
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	var authz apigatewayv2.Authorizer
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &authz))

	f.lambdaRoute(t, "GET /secure", "2.0",
		map[string]any{"authorizationType": "CUSTOM", "authorizerId": authz.AuthorizerID})

	rr = doProxyRequest(t, f.h, http.MethodGet, f.apiID, "/secure", map[string]string{"X-Token": "t"})
	require.Equal(t, http.StatusOK, rr.Code)

	rc, _ := rec.last(t)["requestContext"].(map[string]any)
	authzCtx, _ := rc["authorizer"].(map[string]any)
	assert.Equal(t, map[string]any{"tenant": "acme", "level": float64(3)}, authzCtx["lambda"])
}

func TestHTTPAPIProxy_CORSHeaders(t *testing.T) {
	t.Parallel()

	cors := map[string]any{
		"allowOrigins": []string{"https://app.example.com"}, "allowMethods": []string{"GET", "POST"},
		"allowHeaders": []string{"content-type"}, "exposeHeaders": []string{"x-req"}, "maxAge": 300,
	}

	tests := []struct {
		name        string
		method      string
		origin      string
		wantOrigin  string
		wantMethods string
		wantExpose  string
		wantStatus  int
	}{
		{name: "no origin", method: http.MethodGet, wantStatus: http.StatusOK},
		{
			name: "allowed origin", method: http.MethodGet, origin: "https://app.example.com",
			wantStatus: http.StatusOK, wantOrigin: "https://app.example.com", wantExpose: "x-req",
		},
		{
			name: "disallowed origin", method: http.MethodGet, origin: "https://evil.example.com",
			wantStatus: http.StatusOK,
		},
		{
			name: "preflight allowed", method: http.MethodOptions, origin: "https://app.example.com",
			wantStatus: http.StatusNoContent, wantOrigin: "https://app.example.com", wantMethods: "GET,POST",
			wantExpose: "x-req",
		},
		{
			name: "preflight disallowed", method: http.MethodOptions, origin: "https://evil.example.com",
			wantStatus: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFlowAPI(t, cors)
			f.h.SetLambdaInvoker(recordingInvoker(&recordedEvents{}, `{"statusCode":200}`))
			f.lambdaRoute(t, "GET /x", "2.0", nil)

			hdrs := map[string]string{}
			if tt.origin != "" {
				hdrs["Origin"] = tt.origin
			}

			rr := doProxyRequest(t, f.h, tt.method, f.apiID, "/x", hdrs)

			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Equal(t, tt.wantOrigin, rr.Header().Get("Access-Control-Allow-Origin"))
			assert.Equal(t, tt.wantMethods, rr.Header().Get("Access-Control-Allow-Methods"))
			assert.Equal(t, tt.wantExpose, rr.Header().Get("Access-Control-Expose-Headers"))

			if tt.wantMethods == "" {
				assert.Empty(t, rr.Header().Get("Access-Control-Max-Age"))
			}
		})
	}
}

func TestHTTPAPIProxy_HTTPIntegrationForwarding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		routeKey     string
		uriSuffix    string
		integMethod  string
		requestPath  string
		requestVerb  string
		wantPath     string
		wantUpstream string
	}{
		{
			name: "any keeps client method", routeKey: "ANY /echo", uriSuffix: "/fixed",
			integMethod: "ANY", requestPath: "/echo", requestVerb: http.MethodPut,
			wantPath: "/fixed", wantUpstream: http.MethodPut,
		},
		{
			name: "named param", routeKey: "GET /items/{id}", uriSuffix: "/backend/items/{id}",
			integMethod: "GET", requestPath: "/items/42", requestVerb: http.MethodGet,
			wantPath: "/backend/items/42", wantUpstream: http.MethodGet,
		},
		{
			name: "greedy proxy", routeKey: "ANY /files/{proxy+}", uriSuffix: "/{proxy}",
			integMethod: "ANY", requestPath: "/files/a/b/c.txt", requestVerb: http.MethodPost,
			wantPath: "/a/b/c.txt", wantUpstream: http.MethodPost,
		},
		{
			name: "explicit integration method", routeKey: "ANY /echo", uriSuffix: "/fixed",
			integMethod: "POST", requestPath: "/echo", requestVerb: http.MethodGet,
			wantPath: "/fixed", wantUpstream: http.MethodPost,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var gotMethod, gotPath string

			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotMethod, gotPath = r.Method, r.URL.Path
				_, _ = io.WriteString(w, "upstream")
			}))
			t.Cleanup(upstream.Close)

			f := newFlowAPI(t, nil)
			id := f.integration(t, map[string]any{
				"integrationType": "HTTP_PROXY", "integrationMethod": tt.integMethod,
				"integrationUri": upstream.URL + tt.uriSuffix, "payloadFormatVersion": "1.0",
			})
			f.route(t, map[string]any{"routeKey": tt.routeKey, "target": "integrations/" + id})

			rr := doProxyRequest(t, f.h, tt.requestVerb, f.apiID, tt.requestPath, nil)

			require.Equal(t, http.StatusOK, rr.Code)
			assert.Equal(t, "upstream", rr.Body.String())
			assert.Equal(t, tt.wantUpstream, gotMethod)
			assert.Equal(t, tt.wantPath, gotPath)
		})
	}
}

func TestHTTPAPIProxy_GatewayErrorBodies(t *testing.T) {
	t.Parallel()

	f := newFlowAPI(t, nil)
	f.h.SetLambdaInvoker(recordingInvoker(&recordedEvents{}, `{"statusCode":200}`))
	f.lambdaRoute(t, "GET /only", "2.0", nil)

	rr := doProxyRequest(t, f.h, http.MethodGet, f.apiID, "/missing", nil)

	assert.Equal(t, http.StatusNotFound, rr.Code)
	assert.JSONEq(t, `{"message":"Not Found"}`, rr.Body.String())
	assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))
}
