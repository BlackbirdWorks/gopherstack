package gcpapigateway_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	apigateway "cloud.google.com/go/apigateway/apiv1"
	"cloud.google.com/go/apigateway/apiv1/apigatewaypb"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/gcpapigateway"
)

const (
	project = "test-project"
	region  = "us-central1"
	apiID   = "my-api"
	parent  = "projects/" + project + "/locations/global"
	gwParen = "projects/" + project + "/locations/" + region
)

type fakeClock struct{ ns atomic.Int64 }

func newClock() *fakeClock {
	c := &fakeClock{}
	c.ns.Store(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())

	return c
}

func (c *fakeClock) Now() time.Time { return time.Unix(0, c.ns.Load()).UTC() }

func (c *fakeClock) Advance(d time.Duration) { c.ns.Add(int64(d)) }

type env struct {
	handler *gcpapigateway.Handler
	clock   *fakeClock
	client  *apigateway.Client
	conn    *grpc.ClientConn
	http    *http.Client
}

func newEnv(t *testing.T) *env {
	t.Helper()

	clk := newClock()
	b := gcpapigateway.NewInMemoryBackend()
	b.SetClock(clk.Now)
	b.SetTiming(gcpapigateway.Timing{})

	h := gcpapigateway.NewHandler(b)
	h.Port = 0
	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	conn, err := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	client, err := apigateway.NewClient(t.Context(), option.WithGRPCConn(conn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return &env{handler: h, clock: clk, client: client, conn: conn, http: testclient.New(t)}
}

func (e *env) createAPI(t *testing.T, id string) *apigatewaypb.Api {
	t.Helper()

	op, err := e.client.CreateApi(t.Context(), &apigatewaypb.CreateApiRequest{
		Parent: parent, ApiId: id, Api: &apigatewaypb.Api{DisplayName: id},
	})
	require.NoError(t, err)

	api, err := op.Wait(t.Context())
	require.NoError(t, err)

	return api
}

func configRequest(id, doc string) *apigatewaypb.CreateApiConfigRequest {
	return &apigatewaypb.CreateApiConfigRequest{
		Parent: parent + "/apis/" + apiID, ApiConfigId: id,
		ApiConfig: &apigatewaypb.ApiConfig{OpenapiDocuments: []*apigatewaypb.ApiConfig_OpenApiDocument{{
			Document: &apigatewaypb.ApiConfig_File{Path: "openapi.yaml", Contents: []byte(doc)},
		}}},
	}
}

func (e *env) createConfig(t *testing.T, id, doc string) *apigatewaypb.ApiConfig {
	t.Helper()

	op, err := e.client.CreateApiConfig(t.Context(), configRequest(id, doc))
	require.NoError(t, err)

	cfg, err := op.Wait(t.Context())
	require.NoError(t, err)

	return cfg
}

func (e *env) createGateway(t *testing.T, id, cfg string) *apigatewaypb.Gateway {
	t.Helper()

	op, err := e.client.CreateGateway(t.Context(), &apigatewaypb.CreateGatewayRequest{
		Parent: gwParen, GatewayId: id, Gateway: &apigatewaypb.Gateway{ApiConfig: cfg},
	})
	require.NoError(t, err)

	gw, err := op.Wait(t.Context())
	require.NoError(t, err)

	return gw
}

// deploy creates an API, one config from doc and a gateway serving it.
func (e *env) deploy(t *testing.T, doc string) *apigatewaypb.Gateway {
	t.Helper()

	e.createAPI(t, apiID)

	cfg := e.createConfig(t, "cfg-1", doc)

	return e.createGateway(t, "my-gw", cfg.GetName())
}

type reply struct {
	body   map[string]any
	raw    string
	status int
}

// call sends a request to the gateway by default hostname.
func (e *env) call(t *testing.T, gw *apigatewaypb.Gateway, method, target string, hdr map[string]string) reply {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, "http://"+e.handler.Addr()+target, nil)
	require.NoError(t, err)

	req.Host = gw.GetDefaultHostname()

	for k, v := range hdr {
		req.Header.Set(k, v)
	}

	return e.do(t, req)
}

func (e *env) do(t *testing.T, req *http.Request) reply {
	t.Helper()

	resp, err := e.http.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	r := reply{status: resp.StatusCode, raw: string(raw)}
	_ = json.Unmarshal(raw, &r.body)

	return r
}

// echoBackend answers every request with a JSON description of what it received.
func echoBackend(t *testing.T) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hdr := map[string]string{}
		for _, k := range []string{"Authorization", "X-Forwarded-Authorization", "X-Apigateway-Api-Userinfo"} {
			hdr[k] = r.Header.Get(k)
		}

		body, _ := io.ReadAll(r.Body)

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Backend", "echo")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"method": r.Method, "path": r.URL.Path, "query": r.URL.RawQuery, "headers": hdr, "body": string(body),
		})
	}))
	t.Cleanup(srv.Close)

	return srv
}

const specHeader = "swagger: \"2.0\"\ninfo:\n  title: test\n  version: \"1.0\"\n"

// opSpec renders a Swagger document with the given root backend address and extra top-level and path YAML.
func opSpec(root, top, paths string) string {
	var sb strings.Builder

	sb.WriteString(specHeader)

	if root != "" {
		sb.WriteString("x-google-backend:\n  address: " + root + "\n")
	}

	sb.WriteString(top)
	sb.WriteString("paths:\n")
	sb.WriteString(paths)

	return sb.String()
}

const okResponses = "      responses:\n        \"200\":\n          description: ok\n"

func deleteGatewayRequest(name string) *apigatewaypb.DeleteGatewayRequest {
	return &apigatewaypb.DeleteGatewayRequest{Name: name}
}

func (e *env) managedService(t *testing.T) string {
	t.Helper()

	api, err := e.client.GetApi(t.Context(), &apigatewaypb.GetApiRequest{Name: parent + "/apis/" + apiID})
	require.NoError(t, err)

	return api.GetManagedService()
}

func gatewayRequest(id, parentName, cfg string) *apigatewaypb.CreateGatewayRequest {
	return &apigatewaypb.CreateGatewayRequest{
		Parent: parentName, GatewayId: id, Gateway: &apigatewaypb.Gateway{ApiConfig: cfg},
	}
}
