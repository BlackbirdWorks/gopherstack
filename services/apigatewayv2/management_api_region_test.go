package apigatewayv2_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/apigatewaymanagementapi"
)

type noopInvoker struct{}

func (noopInvoker) InvokeFunction(context.Context, string, string, []byte) ([]byte, int, error) {
	return nil, http.StatusOK, nil
}

func TestHandler_WebSocketConnectionsLandInAPIRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
	}{
		{name: "home-region", region: mrHome},
		{name: "other-region", region: "eu-west-1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler()
			h.SetLambdaInvoker(noopInvoker{})

			mgmt := apigatewaymanagementapi.NewHandler(apigatewaymanagementapi.NewInMemoryBackend())
			mgmt.EnableRegions(mrHome)
			t.Cleanup(func() { mgmt.Shutdown(context.Background()) })
			h.SetManagementAPIResolver(mgmt.BackendFor)

			api := regionCall(t, h, tc.region, http.MethodPost, "/v2/apis",
				`{"name":"ws","protocolType":"WEBSOCKET","routeSelectionExpression":"$request.body.action"}`)
			apiID, _ := api["apiId"].(string)
			require.NotEmpty(t, apiID)

			regionCall(t, h, tc.region, http.MethodPost, "/v2/apis/"+apiID+"/stages", `{"stageName":"prod"}`)

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				r = r.WithContext(awsmeta.Set(r.Context(), &awsmeta.Metadata{Region: mrHome, Account: "000000000000"}))
				_ = h.Handler()(echo.New().NewContext(r, w))
			}))
			t.Cleanup(srv.Close)

			conn, resp, err := websocket.DefaultDialer.DialContext(
				t.Context(), "ws"+strings.TrimPrefix(srv.URL, "http")+"/v2proxy/"+apiID+"/prod", nil)
			require.NoError(t, err)

			defer func() { _ = resp.Body.Close() }()
			defer func() { _ = conn.Close() }()

			require.Eventually(t, func() bool {
				return len(mgmt.BackendFor(tc.region).ListConnections()) == 1
			}, 5*time.Second, 10*time.Millisecond)

			for _, r := range []string{mrHome, "eu-west-1", "ap-south-1"} {
				want := 0
				if r == tc.region {
					want = 1
				}

				assert.Len(t, mgmt.BackendFor(r).ListConnections(), want, r)
			}
		})
	}
}
