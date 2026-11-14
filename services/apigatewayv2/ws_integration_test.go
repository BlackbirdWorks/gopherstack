package apigatewayv2_test

import (
	"encoding/json/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigatewayv2sdk "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	apigatewayv2types "github.com/aws/aws-sdk-go-v2/service/apigatewayv2/types"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

type wsIntegrationResponse struct {
	templates map[string]string
	key       string
	selector  string
}

type wsIntegrationSpec struct {
	templates       map[string]string
	selector        string
	integrationType apigatewayv2types.IntegrationType
	passthrough     string
	responses       []wsIntegrationResponse
	routeResponse   bool
}

// wsUpstream echoes "<method>:<body>" with the status taken from the JSON "status" field of the body.
func wsUpstream(t *testing.T) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)

		var in struct {
			Status int `json:"status"`
		}
		_ = json.Unmarshal(body, &in, jsonx.V1())

		w.Header().Set("X-Upstream", "yes")

		if in.Status != 0 {
			w.WriteHeader(in.Status)
		}

		_, _ = w.Write([]byte(r.Method + ":" + string(body)))
	}))
	t.Cleanup(srv.Close)

	return srv
}

func wsIntegrationAPI(t *testing.T, upstreamURL string, spec wsIntegrationSpec) *wsEnv {
	t.Helper()

	env := newWSEnv(t, &wsLambda{calls: make(chan wsLambdaCall, 8)})

	api, err := env.api.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
		Name:                     aws.String("ws"),
		ProtocolType:             apigatewayv2types.ProtocolTypeWebsocket,
		RouteSelectionExpression: aws.String("$request.body.action"),
	})
	require.NoError(t, err)

	env.apiID = aws.ToString(api.ApiId)

	in := &apigatewayv2sdk.CreateIntegrationInput{
		ApiId:                       api.ApiId,
		IntegrationType:             spec.integrationType,
		IntegrationMethod:           aws.String("POST"),
		IntegrationUri:              aws.String(upstreamURL),
		RequestTemplates:            spec.templates,
		TemplateSelectionExpression: aws.String(spec.selector),
	}
	if spec.passthrough != "" {
		in.PassthroughBehavior = apigatewayv2types.PassthroughBehavior(spec.passthrough)
	}

	integ, err := env.api.CreateIntegration(t.Context(), in)
	require.NoError(t, err)

	for _, r := range spec.responses {
		_, irErr := env.api.CreateIntegrationResponse(t.Context(), &apigatewayv2sdk.CreateIntegrationResponseInput{
			ApiId:                       api.ApiId,
			IntegrationId:               integ.IntegrationId,
			IntegrationResponseKey:      aws.String(r.key),
			ResponseTemplates:           r.templates,
			TemplateSelectionExpression: aws.String(r.selector),
		})
		require.NoError(t, irErr)
	}

	route := &apigatewayv2sdk.CreateRouteInput{
		ApiId:    api.ApiId,
		RouteKey: aws.String("echo"),
		Target:   aws.String("integrations/" + aws.ToString(integ.IntegrationId)),
	}
	if spec.routeResponse {
		route.RouteResponseSelectionExpression = aws.String("$default")
	}

	created, err := env.api.CreateRoute(t.Context(), route)
	require.NoError(t, err)

	if spec.routeResponse {
		_, rrErr := env.api.CreateRouteResponse(t.Context(), &apigatewayv2sdk.CreateRouteResponseInput{
			ApiId: api.ApiId, RouteId: created.RouteId, RouteResponseKey: aws.String("$default"),
		})
		require.NoError(t, rrErr)
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

func TestWebSocket_HTTPIntegrations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		send      string
		wantReply string
		spec      wsIntegrationSpec
		noReply   bool
	}{
		{
			name: "http proxy passthrough",
			spec: wsIntegrationSpec{
				integrationType: apigatewayv2types.IntegrationTypeHttpProxy,
				routeResponse:   true,
			},
			send:      `{"action":"echo","n":1}`,
			wantReply: `POST:{"action":"echo","n":1}`,
		},
		{
			name: "http proxy passes non 2xx body through",
			spec: wsIntegrationSpec{
				integrationType: apigatewayv2types.IntegrationTypeHttpProxy,
				routeResponse:   true,
			},
			send:      `{"action":"echo","status":503}`,
			wantReply: `POST:{"action":"echo","status":503}`,
		},
		{
			name: "http proxy without route response sends nothing",
			spec: wsIntegrationSpec{
				integrationType: apigatewayv2types.IntegrationTypeHttpProxy,
			},
			send:    `{"action":"echo"}`,
			noReply: true,
		},
		{
			name: "request template chosen by selection expression",
			spec: wsIntegrationSpec{
				integrationType: apigatewayv2types.IntegrationTypeHttp,
				routeResponse:   true,
				selector:        "${request.body.kind}",
				templates: map[string]string{
					"a":        `A[$input.json('$.msg')]`,
					"$default": `D[$context.connectionId.length()]`,
				},
				responses: []wsIntegrationResponse{{key: "$default"}},
			},
			send:      `{"action":"echo","kind":"a","msg":"hi"}`,
			wantReply: `POST:A["hi"]`,
		},
		{
			name: "request template falls back to default key",
			spec: wsIntegrationSpec{
				integrationType: apigatewayv2types.IntegrationTypeHttp,
				routeResponse:   true,
				selector:        "${request.body.kind}",
				templates:       map[string]string{"a": `A`, "$default": `D-$context.routeKey`},
				responses:       []wsIntegrationResponse{{key: "$default"}},
			},
			send:      `{"action":"echo","kind":"zzz"}`,
			wantReply: `POST:D-echo`,
		},
		{
			name: "passthrough never without template errors",
			spec: wsIntegrationSpec{
				integrationType: apigatewayv2types.IntegrationTypeHttp,
				routeResponse:   true,
				passthrough:     "NEVER",
				responses:       []wsIntegrationResponse{{key: "$default"}},
			},
			send:      `{"action":"echo"}`,
			wantReply: "Internal server error",
		},
		{
			name: "response template by status regex",
			spec: wsIntegrationSpec{
				integrationType: apigatewayv2types.IntegrationTypeHttp,
				routeResponse:   true,
				responses: []wsIntegrationResponse{
					{key: "/2\\d\\d/", templates: map[string]string{"$default": `ok:$input.body`}},
					{key: "/4\\d\\d/", templates: map[string]string{"$default": `bad:$input.body`}},
					{key: "$default", templates: map[string]string{"$default": `other`}},
				},
			},
			send:      `{"action":"echo"}`,
			wantReply: `ok:POST:{"action":"echo"}`,
		},
		{
			name: "response template for client error status",
			spec: wsIntegrationSpec{
				integrationType: apigatewayv2types.IntegrationTypeHttp,
				routeResponse:   true,
				responses: []wsIntegrationResponse{
					{key: "/2\\d\\d/", templates: map[string]string{"$default": `ok`}},
					{key: "/4\\d\\d/", templates: map[string]string{"$default": `bad`}},
					{key: "$default", templates: map[string]string{"$default": `other`}},
				},
			},
			send:      `{"action":"echo","status":404}`,
			wantReply: `bad`,
		},
		{
			name: "default integration response for unmatched status",
			spec: wsIntegrationSpec{
				integrationType: apigatewayv2types.IntegrationTypeHttp,
				routeResponse:   true,
				responses: []wsIntegrationResponse{
					{key: "/2\\d\\d/", templates: map[string]string{"$default": `ok`}},
					{key: "$default", templates: map[string]string{"$default": `other`}},
				},
			},
			send:      `{"action":"echo","status":500}`,
			wantReply: `other`,
		},
		{
			name: "response template selected by status code expression",
			spec: wsIntegrationSpec{
				integrationType: apigatewayv2types.IntegrationTypeHttp,
				routeResponse:   true,
				responses: []wsIntegrationResponse{{
					key:      "$default",
					selector: "${integration.response.statuscode}",
					templates: map[string]string{
						"418":      `teapot`,
						"$default": `plain`,
					},
				}},
			},
			send:      `{"action":"echo","status":418}`,
			wantReply: `teapot`,
		},
		{
			name: "response template selected by response header",
			spec: wsIntegrationSpec{
				integrationType: apigatewayv2types.IntegrationTypeHttp,
				routeResponse:   true,
				responses: []wsIntegrationResponse{{
					key:       "$default",
					selector:  "${integration.response.header.X-Upstream}",
					templates: map[string]string{"yes": `hdr`, "$default": `plain`},
				}},
			},
			send:      `{"action":"echo"}`,
			wantReply: `hdr`,
		},
		{
			name: "no matching integration response is an error",
			spec: wsIntegrationSpec{
				integrationType: apigatewayv2types.IntegrationTypeHttp,
				routeResponse:   true,
				responses: []wsIntegrationResponse{
					{key: "/2\\d\\d/", templates: map[string]string{"$default": `ok`}},
				},
			},
			send:      `{"action":"echo","status":500}`,
			wantReply: "Internal server error",
		},
		{
			name: "response without route response is not delivered",
			spec: wsIntegrationSpec{
				integrationType: apigatewayv2types.IntegrationTypeHttp,
				responses:       []wsIntegrationResponse{{key: "$default"}},
			},
			send:    `{"action":"echo"}`,
			noReply: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			upstream := wsUpstream(t)
			env := wsIntegrationAPI(t, upstream.URL, tc.spec)

			conn, _, err := env.dial(t, "", nil)
			require.NoError(t, err)

			require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(tc.send)))

			if tc.noReply {
				require.NoError(t, conn.SetReadDeadline(time.Now().Add(300*time.Millisecond)))
				_, _, readErr := conn.ReadMessage()
				require.Error(t, readErr)

				return
			}

			reply := readText(t, conn)
			if tc.wantReply == "Internal server error" {
				var got map[string]string

				require.NoError(t, json.Unmarshal([]byte(reply), &got, jsonx.V1()))
				assert.Equal(t, "Internal server error", got["message"])
				assert.NotEmpty(t, got["connectionId"])

				return
			}

			assert.Equal(t, tc.wantReply, reply)
		})
	}
}

func TestWebSocket_MockIntegrationResponseSelection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		template  string
		wantReply string
	}{
		{name: "status 200", template: `{"statusCode":200}`, wantReply: "ok"},
		{name: "status 404", template: `{"statusCode":404}`, wantReply: "missing"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := wsIntegrationAPI(t, "", wsIntegrationSpec{
				integrationType: apigatewayv2types.IntegrationTypeMock,
				routeResponse:   true,
				selector:        "$default",
				templates:       map[string]string{"$default": tc.template},
				responses: []wsIntegrationResponse{
					{key: "/2\\d\\d/", templates: map[string]string{"$default": "ok"}},
					{key: "$default", templates: map[string]string{"$default": "missing"}},
				},
			})

			conn, _, err := env.dial(t, "", nil)
			require.NoError(t, err)
			require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"echo"}`)))
			assert.Equal(t, tc.wantReply, readText(t, conn))
		})
	}
}

func TestWebSocket_LambdaNonProxyIntegrationResponses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		lambdaOut string
		wantReply string
	}{
		{name: "success", lambdaOut: `{"v":1}`, wantReply: `done:{"v":1}`},
		{name: "function error", lambdaOut: `{"errorMessage":"Boom happened"}`, wantReply: `failed`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			lambda := &wsLambda{
				calls: make(chan wsLambdaCall, 8),
				handlers: map[string]func(map[string]any) []byte{
					"fn": func(map[string]any) []byte { return []byte(tc.lambdaOut) },
				},
			}

			env := newWSEnv(t, lambda)
			api, err := env.api.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
				Name:                     aws.String("ws"),
				ProtocolType:             apigatewayv2types.ProtocolTypeWebsocket,
				RouteSelectionExpression: aws.String("$request.body.action"),
			})
			require.NoError(t, err)

			env.apiID = aws.ToString(api.ApiId)

			integ, err := env.api.CreateIntegration(t.Context(), &apigatewayv2sdk.CreateIntegrationInput{
				ApiId:           api.ApiId,
				IntegrationType: apigatewayv2types.IntegrationTypeAws,
				IntegrationUri:  lambdaURI("fn"),
				RequestTemplates: map[string]string{
					"$default": `{"in":$input.json('$.action')}`,
				},
				TemplateSelectionExpression: aws.String("$default"),
			})
			require.NoError(t, err)

			for key, tpl := range map[string]string{"/Boom.*/": "failed", "$default": "done:$input.body"} {
				_, irErr := env.api.CreateIntegrationResponse(
					t.Context(),
					&apigatewayv2sdk.CreateIntegrationResponseInput{
						ApiId: api.ApiId, IntegrationId: integ.IntegrationId, IntegrationResponseKey: aws.String(key),
						ResponseTemplates: map[string]string{"$default": tpl},
					},
				)
				require.NoError(t, irErr)
			}

			route, err := env.api.CreateRoute(t.Context(), &apigatewayv2sdk.CreateRouteInput{
				ApiId:                            api.ApiId,
				RouteKey:                         aws.String("echo"),
				Target:                           aws.String("integrations/" + aws.ToString(integ.IntegrationId)),
				RouteResponseSelectionExpression: aws.String("$default"),
			})
			require.NoError(t, err)

			_, err = env.api.CreateRouteResponse(t.Context(), &apigatewayv2sdk.CreateRouteResponseInput{
				ApiId: api.ApiId, RouteId: route.RouteId, RouteResponseKey: aws.String("$default"),
			})
			require.NoError(t, err)

			_, err = env.api.CreateStage(t.Context(), &apigatewayv2sdk.CreateStageInput{
				ApiId: api.ApiId, StageName: aws.String("prod"),
			})
			require.NoError(t, err)
			_, err = env.api.CreateDeployment(t.Context(), &apigatewayv2sdk.CreateDeploymentInput{
				ApiId: api.ApiId, StageName: aws.String("prod"),
			})
			require.NoError(t, err)

			conn, _, err := env.dial(t, "", nil)
			require.NoError(t, err)
			require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"echo"}`)))

			ev := lambda.next(t, "fn")
			assert.Equal(t, "echo", ev["in"])
			assert.Equal(t, tc.wantReply, readText(t, conn))
		})
	}
}
