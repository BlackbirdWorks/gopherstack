package apprunner_test

import (
	"encoding/json/v2"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func TestCreateVpcIngressConnection_ServiceMustExist(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantType string
		wantCode int
		dangling bool
	}{
		{name: "existing", wantCode: http.StatusOK},
		{name: "dangling", dangling: true, wantCode: http.StatusBadRequest, wantType: "InvalidRequestException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			svcArn := createTestService(t, h)

			if tt.dangling {
				svcArn += "-gone"
			}

			rec := doRequest(t, h, "CreateVpcIngressConnection", map[string]any{
				"VpcIngressConnectionName": "vic",
				"ServiceArn":               svcArn,
				"IngressVpcConfiguration":  map[string]any{"VpcId": "vpc-1", "VpcEndpointId": "vpce-1"},
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantType != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp, jsonx.V1()))
				assert.Contains(t, resp["__type"], tt.wantType)
			}
		})
	}
}
