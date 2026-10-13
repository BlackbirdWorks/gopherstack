package apigatewayv2

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
)

var errInvoke = errors.New("boom")

type failingInvoker struct{}

func (failingInvoker) InvokeFunction(_ context.Context, _, _ string, _ []byte) ([]byte, int, error) {
	return nil, 0, errInvoke
}

func TestInvokeWSRoute_DeploymentSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		deploy  bool
		wantErr bool
	}{
		{name: "pinned snapshot keeps route", deploy: true, wantErr: false},
		{name: "undeployed stage uses live routes", deploy: false, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := NewHandler(NewInMemoryBackend())
			h.SetLambdaInvoker(&stubLambdaInvoker{})

			api, err := h.Backend.CreateAPI(context.Background(), CreateAPIInput{
				Name: "ws-snap", ProtocolType: protocolTypeWebSocket,
			})
			require.NoError(t, err)

			integ, err := h.Backend.CreateIntegration(api.APIID, CreateIntegrationInput{
				IntegrationType: integrationTypeMock,
			})
			require.NoError(t, err)

			route, err := h.Backend.CreateRoute(api.APIID, CreateRouteInput{
				RouteKey: "chat", Target: "integrations/" + integ.IntegrationID,
			})
			require.NoError(t, err)

			_, err = h.Backend.CreateStage(api.APIID, CreateStageInput{StageName: "prod"})
			require.NoError(t, err)

			if tt.deploy {
				_, err = h.Backend.CreateDeployment(api.APIID, CreateDeploymentInput{StageName: "prod"})
				require.NoError(t, err)
			}

			require.NoError(t, h.Backend.DeleteRoute(api.APIID, route.RouteID))

			c := echo.New().NewContext(httptest.NewRequest(http.MethodGet, "/", nil), httptest.NewRecorder())
			err = h.invokeWSRoute(c, api.APIID, "prod", "chat", "conn-1", nil)

			if tt.wantErr {
				require.ErrorIs(t, err, ErrRouteNotFound)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInvokeWSRoute_LambdaFailureIsIntegrationInvoke(t *testing.T) {
	t.Parallel()

	h := NewHandler(NewInMemoryBackend())
	h.SetLambdaInvoker(failingInvoker{})

	api, err := h.Backend.CreateAPI(context.Background(), CreateAPIInput{
		Name: "ws-fail", ProtocolType: protocolTypeWebSocket,
	})
	require.NoError(t, err)

	integ, err := h.Backend.CreateIntegration(api.APIID, CreateIntegrationInput{
		IntegrationType: IntegrationTypeAWSProxy,
		IntegrationURI:  "arn:aws:lambda:us-east-1:123456789012:function:fn/invocations",
	})
	require.NoError(t, err)

	_, err = h.Backend.CreateRoute(api.APIID, CreateRouteInput{
		RouteKey: "chat", Target: "integrations/" + integ.IntegrationID,
	})
	require.NoError(t, err)

	c := echo.New().NewContext(httptest.NewRequest(http.MethodGet, "/", nil), httptest.NewRecorder())
	err = h.invokeWSRoute(c, api.APIID, "$default", "chat", "conn-1", nil)

	require.ErrorIs(t, err, ErrIntegrationInvoke)
}

func TestWSMetrics_RoutedDimensionsAndErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err       error
		name      string
		wantNames []string
		detailed  bool
	}{
		{name: "ok", wantNames: []string{"IntegrationLatency"}},
		{
			name: "integration error", err: ErrIntegrationInvoke,
			wantNames: []string{"IntegrationLatency", "ExecutionError", "IntegrationError"},
		},
		{name: "routing error", err: ErrRouteNotFound, wantNames: []string{"IntegrationLatency", "ExecutionError"}},
		{name: "detailed route", detailed: true, wantNames: []string{"IntegrationLatency"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := NewHandler(NewInMemoryBackend())

			var (
				mu     sync.Mutex
				points []cwmetric.Point
			)

			h.SetMetricEmitter(cwmetric.EmitterFunc(func(p cwmetric.Point) error {
				mu.Lock()
				defer mu.Unlock()

				points = append(points, p)

				return nil
			}))

			api, err := h.Backend.CreateAPI(context.Background(), CreateAPIInput{
				Name: "ws-metrics", ProtocolType: protocolTypeWebSocket,
			})
			require.NoError(t, err)

			_, err = h.Backend.CreateStage(api.APIID, CreateStageInput{
				StageName:            "prod",
				DefaultRouteSettings: &RouteSettings{DetailedMetricsEnabled: tt.detailed},
			})
			require.NoError(t, err)

			h.wsMetrics(api.APIID, "prod").forRoute("chat").routed(tt.err, time.Now())

			mu.Lock()
			defer mu.Unlock()

			seen := map[string]bool{}
			routeDim := false

			for _, p := range points {
				seen[p.Name] = true

				for _, d := range p.Dimensions {
					if d.Name == "Route" && d.Value == "chat" {
						routeDim = true
					}
				}
			}

			for _, n := range tt.wantNames {
				assert.True(t, seen[n], "missing metric %s", n)
			}

			assert.Len(t, seen, len(tt.wantNames), "unexpected extra metrics: %v", seen)
			assert.Equal(t, tt.detailed, routeDim)
		})
	}
}
