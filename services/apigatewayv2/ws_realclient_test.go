package apigatewayv2_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	mgmtsdk "github.com/aws/aws-sdk-go-v2/service/apigatewaymanagementapi"
	mgmttypes "github.com/aws/aws-sdk-go-v2/service/apigatewaymanagementapi/types"
	apigatewayv2sdk "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	apigatewayv2types "github.com/aws/aws-sdk-go-v2/service/apigatewayv2/types"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/apigatewaymanagementapi"
	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

type wsLambdaCall struct {
	fn    string
	event map[string]any
}

// wsLambda is a function-name keyed fake Lambda that records every invocation.
type wsLambda struct {
	handlers map[string]func(event map[string]any) []byte
	calls    chan wsLambdaCall
}

func (l *wsLambda) InvokeFunction(_ context.Context, fn, _ string, payload []byte) ([]byte, int, error) {
	var ev map[string]any
	_ = json.Unmarshal(payload, &ev)
	l.calls <- wsLambdaCall{fn: fn, event: ev}

	if h := l.handlers[fn]; h != nil {
		return h(ev), http.StatusOK, nil
	}

	return []byte(`{"statusCode":200}`), http.StatusOK, nil
}

// next waits for the next invocation of fn.
func (l *wsLambda) next(t *testing.T, fn string) map[string]any {
	t.Helper()

	timeout := time.After(10 * time.Second)

	for {
		select {
		case c := <-l.calls:
			if c.fn == fn {
				return c.event
			}
		case <-timeout:
			require.FailNow(t, "no lambda invocation", fn)
		}
	}
}

type wsEnv struct {
	lambda *wsLambda
	api    *apigatewayv2sdk.Client
	mgmt   *mgmtsdk.Client
	srv    *httptest.Server
	apiID  string
}

func lambdaURI(fn string) *string {
	return aws.String("arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/" +
		"arn:aws:lambda:us-east-1:000000000000:function:" + fn + "/invocations")
}

func newWSEnv(t *testing.T, lambda *wsLambda) *wsEnv {
	t.Helper()

	v2 := apigatewayv2.NewHandler(apigatewayv2.NewInMemoryBackend())
	v2.SetLambdaInvoker(lambda)

	mgmtBackend := apigatewaymanagementapi.NewInMemoryBackend()
	v2.SetManagementAPIBackend(mgmtBackend)

	registry := service.NewRegistry()
	require.NoError(t, registry.Register(v2))
	require.NoError(t, registry.Register(apigatewaymanagementapi.NewHandler(mgmtBackend)))

	e := echo.New()
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	require.NoError(t, err)

	return &wsEnv{
		lambda: lambda,
		srv:    srv,
		api: apigatewayv2sdk.NewFromConfig(cfg, func(o *apigatewayv2sdk.Options) {
			o.BaseEndpoint = aws.String(srv.URL)
		}),
		mgmt: mgmtsdk.NewFromConfig(cfg, func(o *mgmtsdk.Options) {
			o.BaseEndpoint = aws.String(srv.URL + "/prod")
		}),
	}
}

func (e *wsEnv) route(t *testing.T, key, fn string, mod func(*apigatewayv2sdk.CreateRouteInput)) string {
	t.Helper()

	integ, err := e.api.CreateIntegration(t.Context(), &apigatewayv2sdk.CreateIntegrationInput{
		ApiId:             aws.String(e.apiID),
		IntegrationType:   apigatewayv2types.IntegrationTypeAwsProxy,
		IntegrationMethod: aws.String("POST"),
		IntegrationUri:    lambdaURI(fn),
	})
	require.NoError(t, err)

	in := &apigatewayv2sdk.CreateRouteInput{
		ApiId:    aws.String(e.apiID),
		RouteKey: aws.String(key),
		Target:   aws.String("integrations/" + aws.ToString(integ.IntegrationId)),
	}
	if mod != nil {
		mod(in)
	}

	out, err := e.api.CreateRoute(t.Context(), in)
	require.NoError(t, err)

	return aws.ToString(out.RouteId)
}

// dial connects to the API through its execute-api host name.
func (e *wsEnv) dial(t *testing.T, query string, hdr http.Header) (*websocket.Conn, *http.Response, error) {
	t.Helper()

	u, err := url.Parse(e.srv.URL)
	require.NoError(t, err)

	if hdr == nil {
		hdr = http.Header{}
	}

	hdr.Set("Host", e.apiID+".execute-api.localhost:"+u.Port())

	target := "ws://" + u.Host + "/prod"
	if query != "" {
		target += "?" + query
	}

	conn, resp, err := websocket.DefaultDialer.DialContext(t.Context(), target, hdr)
	if conn != nil {
		t.Cleanup(func() { _ = conn.Close() })
	}

	return conn, resp, err
}

func readText(t *testing.T, conn *websocket.Conn) string {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(10*time.Second)))

	_, msg, err := conn.ReadMessage()
	require.NoError(t, err)

	return string(msg)
}

func rcField(ev map[string]any, key string) any {
	rc, _ := ev["requestContext"].(map[string]any)

	return rc[key]
}

const allowPolicy = `{"principalId":"user-1","policyDocument":{"Version":"2012-10-17","Statement":[` +
	`{"Action":"execute-api:Invoke","Effect":"Allow","Resource":"*"}]},"context":{"tier":"gold"}}`

const denyPolicy = `{"principalId":"user-1","policyDocument":{"Version":"2012-10-17","Statement":[` +
	`{"Action":"execute-api:Invoke","Effect":"Deny","Resource":"*"}]}}`

func buildWSAPI(t *testing.T, lambda *wsLambda) *wsEnv {
	t.Helper()

	env := newWSEnv(t, lambda)

	api, err := env.api.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
		Name:                     aws.String("ws"),
		ProtocolType:             apigatewayv2types.ProtocolTypeWebsocket,
		RouteSelectionExpression: aws.String("$request.body.action"),
	})
	require.NoError(t, err)

	env.apiID = aws.ToString(api.ApiId)

	auth, err := env.api.CreateAuthorizer(t.Context(), &apigatewayv2sdk.CreateAuthorizerInput{
		ApiId:          api.ApiId,
		Name:           aws.String("req"),
		AuthorizerType: apigatewayv2types.AuthorizerTypeRequest,
		AuthorizerUri:  lambdaURI("auth-fn"),
		IdentitySource: []string{"route.request.querystring.token", "route.request.header.Auth"},
	})
	require.NoError(t, err)

	env.route(t, "$connect", "connect-fn", func(in *apigatewayv2sdk.CreateRouteInput) {
		in.AuthorizationType = apigatewayv2types.AuthorizationTypeCustom
		in.AuthorizerId = auth.AuthorizerId
	})
	env.route(t, "$disconnect", "disconnect-fn", nil)
	env.route(t, "$default", "default-fn", nil)
	env.route(t, "echo", "echo-fn", func(in *apigatewayv2sdk.CreateRouteInput) {
		in.RouteResponseSelectionExpression = aws.String("$default")
	})

	routes, err := env.api.GetRoutes(t.Context(), &apigatewayv2sdk.GetRoutesInput{ApiId: api.ApiId})
	require.NoError(t, err)

	for _, r := range routes.Items {
		if aws.ToString(r.RouteKey) == "echo" {
			assert.Equal(t, "$default", aws.ToString(r.RouteResponseSelectionExpression))

			_, rrErr := env.api.CreateRouteResponse(t.Context(), &apigatewayv2sdk.CreateRouteResponseInput{
				ApiId: api.ApiId, RouteId: r.RouteId, RouteResponseKey: aws.String("$default"),
			})
			require.NoError(t, rrErr)
		}
	}

	_, err = env.api.CreateStage(t.Context(), &apigatewayv2sdk.CreateStageInput{
		ApiId: api.ApiId, StageName: aws.String("prod"),
	})
	require.NoError(t, err)
	_, err = env.api.CreateDeployment(t.Context(), &apigatewayv2sdk.CreateDeploymentInput{
		ApiId: api.ApiId, StageName: aws.String("prod"),
	})
	require.NoError(t, err)

	return env
}

func authHandler(t *testing.T) func(ev map[string]any) []byte {
	t.Helper()

	return func(ev map[string]any) []byte {
		q, _ := ev["queryStringParameters"].(map[string]any)
		if q["token"] == "good" {
			return []byte(allowPolicy)
		}

		return []byte(denyPolicy)
	}
}

func TestWebSocket_ConnectAuthorizer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		query      string
		header     string
		wantStatus int
		wantAuth   bool
	}{
		{name: "allow", query: "token=good", header: "h", wantAuth: true},
		{name: "deny", query: "token=bad", header: "h", wantStatus: http.StatusForbidden},
		{name: "missing query identity", header: "h", wantStatus: http.StatusUnauthorized},
		{name: "missing header identity", query: "token=good", wantStatus: http.StatusUnauthorized},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			lambda := &wsLambda{
				calls:    make(chan wsLambdaCall, 64),
				handlers: map[string]func(map[string]any) []byte{},
			}
			lambda.handlers["auth-fn"] = authHandler(t)
			env := buildWSAPI(t, lambda)

			hdr := http.Header{}
			if tc.header != "" {
				hdr.Set("Auth", tc.header)
			}

			conn, resp, err := env.dial(t, tc.query, hdr)

			if tc.wantStatus != 0 {
				require.ErrorIs(t, err, websocket.ErrBadHandshake)
				assert.Equal(t, tc.wantStatus, resp.StatusCode)

				for len(lambda.calls) > 0 {
					assert.NotEqual(t, "connect-fn", (<-lambda.calls).fn, "denied connect must not reach the integration")
				}

				return
			}

			require.NoError(t, err)
			require.NotNil(t, conn)

			authEv := lambda.next(t, "auth-fn")
			assert.Equal(t, "REQUEST", authEv["type"])
			assert.True(t, strings.HasSuffix(authEv["methodArn"].(string), "/prod/$connect"))

			ev := lambda.next(t, "connect-fn")
			assert.Equal(t, "$connect", rcField(ev, "routeKey"))
			assert.Equal(t, "CONNECT", rcField(ev, "eventType"))
			assert.Equal(t, "prod", rcField(ev, "stage"))
			assert.Equal(t, env.apiID, rcField(ev, "apiId"))
			assert.Contains(t, rcField(ev, "domainName"), ".execute-api.")
			assert.NotEmpty(t, rcField(ev, "connectionId"))
			authz, _ := rcField(ev, "authorizer").(map[string]any)
			assert.Equal(t, "user-1", authz["principalId"])
			assert.Equal(t, "gold", authz["tier"])
		})
	}
}

func TestWebSocket_RoutingAndResponses(t *testing.T) {
	t.Parallel()

	lambda := &wsLambda{calls: make(chan wsLambdaCall, 64), handlers: map[string]func(map[string]any) []byte{}}
	lambda.handlers["auth-fn"] = authHandler(t)
	lambda.handlers["echo-fn"] = func(ev map[string]any) []byte { return []byte(`"echo:` + ev["body"].(string) + `"`) }
	env := buildWSAPI(t, lambda)

	hdr := http.Header{}
	hdr.Set("Auth", "h")

	conn, _, err := env.dial(t, "token=good", hdr)
	require.NoError(t, err)

	connEv := lambda.next(t, "connect-fn")
	connID := rcField(connEv, "connectionId").(string)

	tests := []struct {
		name      string
		send      string
		wantFn    string
		wantRoute string
		wantReply string
	}{
		{name: "custom route", send: `{"action":"echo","d":1}`, wantFn: "echo-fn", wantRoute: "echo",
			wantReply: `"echo:{"action":"echo","d":1}"`},
		{name: "unknown action falls to default", send: `{"action":"nope"}`, wantFn: "default-fn", wantRoute: "$default"},
		{name: "no action falls to default", send: `{"x":1}`, wantFn: "default-fn", wantRoute: "$default"},
		{name: "non json falls to default", send: `hello`, wantFn: "default-fn", wantRoute: "$default"},
	}

	for _, tc := range tests {
		require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(tc.send)), tc.name)

		ev := lambda.next(t, tc.wantFn)
		assert.Equal(t, tc.wantRoute, rcField(ev, "routeKey"), tc.name)
		assert.Equal(t, "MESSAGE", rcField(ev, "eventType"), tc.name)
		assert.Equal(t, connID, rcField(ev, "connectionId"), tc.name)
		assert.Equal(t, "prod", rcField(ev, "stage"), tc.name)
		assert.NotEmpty(t, rcField(ev, "messageId"), tc.name)
		assert.Equal(t, tc.send, ev["body"], tc.name)

		if tc.wantReply != "" {
			assert.Equal(t, tc.wantReply, readText(t, conn), tc.name)
		}
	}

	// default-fn returned a payload but $default has no route response: nothing is delivered.
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(200*time.Millisecond)))
	_, _, readErr := conn.ReadMessage()
	require.Error(t, readErr)
}

func TestWebSocket_ManagementConnections(t *testing.T) {
	t.Parallel()

	lambda := &wsLambda{calls: make(chan wsLambdaCall, 64), handlers: map[string]func(map[string]any) []byte{}}
	lambda.handlers["auth-fn"] = authHandler(t)
	env := buildWSAPI(t, lambda)

	hdr := http.Header{}
	hdr.Set("Auth", "h")
	hdr.Set("User-Agent", "ws-test-agent")

	conn, _, err := env.dial(t, "token=good", hdr)
	require.NoError(t, err)

	connID := rcField(lambda.next(t, "connect-fn"), "connectionId").(string)

	_, err = env.mgmt.PostToConnection(t.Context(), &mgmtsdk.PostToConnectionInput{
		ConnectionId: aws.String(connID), Data: []byte("pushed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "pushed", readText(t, conn))

	got, err := env.mgmt.GetConnection(t.Context(), &mgmtsdk.GetConnectionInput{ConnectionId: aws.String(connID)})
	require.NoError(t, err)
	assert.NotNil(t, got.ConnectedAt)
	assert.NotNil(t, got.LastActiveAt)
	require.NotNil(t, got.Identity)
	assert.NotEmpty(t, aws.ToString(got.Identity.SourceIp))
	assert.Equal(t, "ws-test-agent", aws.ToString(got.Identity.UserAgent))

	_, err = env.mgmt.DeleteConnection(t.Context(), &mgmtsdk.DeleteConnectionInput{ConnectionId: aws.String(connID)})
	require.NoError(t, err)

	disc := lambda.next(t, "disconnect-fn")
	assert.Equal(t, "DISCONNECT", rcField(disc, "eventType"))
	assert.Equal(t, "$disconnect", rcField(disc, "routeKey"))
	assert.Equal(t, connID, rcField(disc, "connectionId"))

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(10*time.Second)))
	_, _, readErr := conn.ReadMessage()
	require.Error(t, readErr)

	_, err = env.mgmt.GetConnection(t.Context(), &mgmtsdk.GetConnectionInput{ConnectionId: aws.String(connID)})

	var gone *mgmttypes.GoneException
	require.ErrorAs(t, err, &gone)
}

func TestWebSocket_NoMatchingRouteSendsForbidden(t *testing.T) {
	t.Parallel()

	lambda := &wsLambda{calls: make(chan wsLambdaCall, 64), handlers: map[string]func(map[string]any) []byte{}}
	env := newWSEnv(t, lambda)

	api, err := env.api.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
		Name:                     aws.String("ws2"),
		ProtocolType:             apigatewayv2types.ProtocolTypeWebsocket,
		RouteSelectionExpression: aws.String("$request.body.action"),
	})
	require.NoError(t, err)

	env.apiID = aws.ToString(api.ApiId)
	env.route(t, "only", "only-fn", nil)

	_, err = env.api.CreateStage(t.Context(), &apigatewayv2sdk.CreateStageInput{
		ApiId: api.ApiId, StageName: aws.String("prod"),
	})
	require.NoError(t, err)

	conn, _, err := env.dial(t, "", nil)
	require.NoError(t, err)
	require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"x"}`)))

	var frame map[string]string
	require.NoError(t, json.Unmarshal([]byte(readText(t, conn)), &frame))
	assert.Equal(t, "Forbidden", frame["message"])
	assert.NotEmpty(t, frame["connectionId"])
}
