package apigateway_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

func TestInvokeStage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		req        apigateway.StageRequest
		wantStatus int
		wantEvent  bool
	}{
		{
			name: "deployed route",
			req: apigateway.StageRequest{
				Stage: "prod", Method: http.MethodPost, Path: "/items",
				Headers: map[string]string{"X-Probe": "1"}, Query: map[string]string{"q": "v"}, Body: []byte(`{"a":1}`),
			},
			wantStatus: http.StatusOK,
			wantEvent:  true,
		},
		{
			name:       "unknown stage",
			req:        apigateway.StageRequest{Stage: "nope", Method: http.MethodPost, Path: "/items"},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "unknown path",
			req:        apigateway.StageRequest{Stage: "prod", Method: http.MethodPost, Path: "/other"},
			wantStatus: http.StatusForbidden,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _, apiID := setupProxyAPIViaHandler(t, "AWS_PROXY",
				"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/"+
					"arn:aws:lambda:us-east-1:000000000000:function:fn/invocations")

			var captured []byte

			h.SetLambdaInvoker(&captureInvoker{capture: &captured})

			tt.req.APIID = apiID
			resp := h.InvokeStage(t.Context(), tt.req)

			assert.Equal(t, tt.wantStatus, resp.Status)

			if !tt.wantEvent {
				return
			}

			var ev apigateway.LambdaProxyEvent

			require.NoError(t, json.Unmarshal(captured, &ev))
			assert.Equal(t, "/items", ev.Resource)
			assert.Equal(t, `{"a":1}`, ev.Body)
			assert.Equal(t, "v", ev.QueryStringParameters["q"])
			assert.Equal(t, "1", ev.Headers["x-probe"])
		})
	}
}
