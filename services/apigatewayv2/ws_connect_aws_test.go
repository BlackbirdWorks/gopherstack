package apigatewayv2_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	apigatewayv2types "github.com/aws/aws-sdk-go-v2/service/apigatewayv2/types"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

type wsFakeAWS struct {
	deadline chan time.Duration
	reqs     chan apigateway.AWSServiceRequest
	body     string
	status   int
}

func (f *wsFakeAWS) Supports(svc string) bool { return svc == "dynamodb" }

func (f *wsFakeAWS) InvokeAWSService(
	ctx context.Context, r apigateway.AWSServiceRequest,
) (apigateway.AWSServiceResponse, error) {
	if f.deadline != nil {
		dl, ok := ctx.Deadline()
		if !ok {
			dl = time.Now().Add(time.Hour)
		}

		f.deadline <- time.Until(dl)
	}

	f.reqs <- r

	return apigateway.AWSServiceResponse{Body: []byte(f.body), Status: f.status}, nil
}

func wsFixedUpstream(t *testing.T, status int, bodies chan<- string) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if bodies != nil {
			bodies <- string(b)
		}

		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)

	return srv
}

func TestWebSocket_ConnectUpstreamStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		typ        apigatewayv2types.IntegrationType
		status     int
		wantStatus int
	}{
		{name: "proxy 503 rejects", typ: apigatewayv2types.IntegrationTypeHttpProxy, status: 503, wantStatus: 503},
		{name: "proxy 403 rejects", typ: apigatewayv2types.IntegrationTypeHttpProxy, status: 403, wantStatus: 403},
		{name: "proxy 204 admits", typ: apigatewayv2types.IntegrationTypeHttpProxy, status: 204},
		{name: "http 500 rejects", typ: apigatewayv2types.IntegrationTypeHttp, status: 500, wantStatus: 500},
		{name: "http 302 maps to 500", typ: apigatewayv2types.IntegrationTypeHttp, status: 302, wantStatus: 500},
		{name: "http 200 admits", typ: apigatewayv2types.IntegrationTypeHttp, status: 200},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			upstream := wsFixedUpstream(t, tc.status, nil)
			env := wsIntegrationAPI(t, upstream.URL, wsIntegrationSpec{
				integrationType: tc.typ,
				routeKey:        "$connect",
				responses:       []wsIntegrationResponse{{key: "$default"}},
			})

			conn, resp, err := env.dial(t, "", nil)
			if tc.wantStatus == 0 {
				require.NoError(t, err)
				require.NotNil(t, conn)

				return
			}

			require.Error(t, err)
			require.NotNil(t, resp)
			assert.Equal(t, tc.wantStatus, resp.StatusCode)
		})
	}
}

func TestWebSocket_DisconnectContext(t *testing.T) {
	t.Parallel()

	bodies := make(chan string, 4)
	upstream := wsFixedUpstream(t, http.StatusOK, bodies)
	env := wsIntegrationAPI(t, upstream.URL, wsIntegrationSpec{
		integrationType: apigatewayv2types.IntegrationTypeHttp,
		routeKey:        "$disconnect",
		templates: map[string]string{
			"$default": `$context.eventType:$context.disconnectStatusCode:$context.routeKey`,
		},
		responses: []wsIntegrationResponse{{key: "$default"}},
	})

	conn, _, err := env.dial(t, "", nil)
	require.NoError(t, err)
	require.NoError(t, conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "bye"), time.Now().Add(5*time.Second)))

	select {
	case got := <-bodies:
		assert.Equal(t, "DISCONNECT:1000:$disconnect", got)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "no $disconnect upstream call")
	}
}

func TestWebSocket_AWSIntegrationService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		uri       string
		body      string
		respKey   string
		respTpl   string
		wantReply string
		wantSpec  string
		status    int
	}{
		{
			name: "dynamodb action success", uri: "arn:aws:apigateway:us-east-1:dynamodb:action/PutItem",
			body: `{"ok":1}`, status: 200, respKey: "/2\\d\\d/", respTpl: `ok:$input.body`,
			wantReply: `ok:{"ok":1}`, wantSpec: "PutItem",
		},
		{
			name: "dynamodb status selects response", uri: "arn:aws:apigateway:us-east-1:dynamodb:action/GetItem",
			body: `{}`, status: 400, respKey: "/4\\d\\d/", respTpl: `bad`,
			wantReply: `bad`, wantSpec: "GetItem",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fake := &wsFakeAWS{reqs: make(chan apigateway.AWSServiceRequest, 2), body: tc.body, status: tc.status}

			env := wsIntegrationAPI(t, tc.uri, wsIntegrationSpec{
				integrationType: apigatewayv2types.IntegrationTypeAws,
				routeResponse:   true,
				templates:       map[string]string{"$default": `{"msg":"$input.path('$.msg')"}`},
				responses: []wsIntegrationResponse{
					{key: tc.respKey, templates: map[string]string{"$default": tc.respTpl}},
				},
			}, func(h *apigatewayv2.Handler) { h.SetAWSServiceInvoker(fake) })

			conn, _, err := env.dial(t, "", nil)
			require.NoError(t, err)
			require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"echo","msg":"hi"}`)))

			assert.Equal(t, tc.wantReply, readText(t, conn))

			select {
			case req := <-fake.reqs:
				assert.Equal(t, "dynamodb", req.Service)
				assert.Equal(t, tc.wantSpec, req.Spec)
				assert.JSONEq(t, `{"msg":"hi"}`, string(req.Body))
			default:
				require.FailNow(t, "service not invoked")
			}
		})
	}
}

func TestWebSocket_AWSIntegrationBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		timeout      int32
		bodyLen      int
		wantWithin   time.Duration
		wantReplyLen int
	}{
		{name: "timeout bounds call", timeout: 2500, bodyLen: 4, wantWithin: 2500 * time.Millisecond, wantReplyLen: 4},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fake := &wsFakeAWS{
				reqs: make(chan apigateway.AWSServiceRequest, 2), deadline: make(chan time.Duration, 2),
				body: strings.Repeat("x", tc.bodyLen), status: 200,
			}

			env := wsIntegrationAPI(t, "arn:aws:apigateway:us-east-1:dynamodb:action/PutItem", wsIntegrationSpec{
				integrationType: apigatewayv2types.IntegrationTypeAws,
				routeResponse:   true,
				timeoutMillis:   tc.timeout,
				responses:       []wsIntegrationResponse{{key: "/2\\d\\d/"}},
			}, func(h *apigatewayv2.Handler) { h.SetAWSServiceInvoker(fake) })

			conn, _, err := env.dial(t, "", nil)
			require.NoError(t, err)
			require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"echo"}`)))

			assert.Len(t, readText(t, conn), tc.wantReplyLen)
			remaining := <-fake.deadline
			if tc.wantWithin > 0 {
				assert.Positive(t, remaining)
				assert.LessOrEqual(t, remaining, tc.wantWithin)
			}
		})
	}
}
