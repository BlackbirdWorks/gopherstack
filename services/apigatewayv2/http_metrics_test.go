package apigatewayv2_test

import (
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

func TestHTTPAPIProxy_Metrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		detailed  bool
		wantRoute bool
	}{
		{name: "default dimensions", detailed: false, wantRoute: false},
		{name: "detailed route dimension", detailed: true, wantRoute: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			h.SetLambdaInvoker(&mockLambdaInvoker{})

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

			apiID := buildHTTPAPIWithLambda(t, h, "GET /items", throttleLambdaURI)
			ensureDefaultStage(t, h, apiID)

			_, err := h.Backend.UpdateStage(apiID, "$default", apigatewayv2.UpdateStageInput{
				RouteSettings: map[string]apigatewayv2.RouteSettings{
					"GET /items": {DetailedMetricsEnabled: tt.detailed},
				},
			})
			require.NoError(t, err)

			rr := doProxyRequest(t, h, http.MethodGet, apiID, "/items", nil)
			require.Equal(t, http.StatusOK, rr.Code)

			mu.Lock()
			defer mu.Unlock()

			var dataProcessed float64

			routeDim := false

			for _, p := range points {
				for _, d := range p.Dimensions {
					if d.Name == "Route" && d.Value == "GET /items" {
						routeDim = true
					}
				}

				if p.Name == "DataProcessed" {
					assert.Equal(t, "Bytes", p.Unit)

					dataProcessed = max(dataProcessed, p.Value)
				}
			}

			assert.Positive(t, dataProcessed, "DataProcessed must count the response body")
			assert.Equal(t, tt.wantRoute, routeDim)
		})
	}
}
