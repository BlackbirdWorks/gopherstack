package app

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apigwbackend "github.com/blackbirdworks/gopherstack/services/apigateway"
)

func TestAPIGatewayInvoker_S3PathBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		spec       string
		wantStatus int
		wantServed bool
	}{
		{name: "bucket key", spec: "bucket/key.txt", wantStatus: http.StatusOK, wantServed: true},
		{name: "admin path", spec: "_gopherstack/reset", wantStatus: http.StatusBadRequest},
		{name: "leading slash admin", spec: "/_gopherstack/reset", wantStatus: http.StatusBadRequest},
		{name: "dot dot", spec: "bucket/../../x", wantStatus: http.StatusBadRequest},
		{name: "query", spec: "bucket/key?x=1", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			served := false
			inv := &apigwServiceInvoker{handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				served = true
				w.WriteHeader(http.StatusOK)
			})}

			resp, err := inv.InvokeAWSService(t.Context(), apigwbackend.AWSServiceRequest{
				Region: "us-east-1", Service: "s3", Kind: "path", Spec: tt.spec, Method: http.MethodPost,
			})
			require.NoError(t, err)

			assert.Equal(t, tt.wantStatus, resp.Status)
			assert.Equal(t, tt.wantServed, served)
		})
	}
}
