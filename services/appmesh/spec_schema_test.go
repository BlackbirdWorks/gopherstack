package appmesh_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/appmesh"
)

const (
	httpPortMapping = `{"portMapping":{"port":80,"protocol":"http"}}`
	oneTarget       = `{"weightedTargets":[{"virtualNode":"n","weight":1}]}`
	svcTarget       = `{"target":{"virtualService":{"virtualServiceName":"s"}}}`
)

func validGatewaySpecBody() map[string]any {
	return map[string]any{"listeners": []any{map[string]any{
		"portMapping": map[string]any{"port": 8080, "protocol": "http"},
	}}}
}

func TestBackend_DeepSpecValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		kind    string
		spec    string
		wantErr bool
	}{
		{
			name: "node_ok", kind: "node",
			spec: `{"listeners":[{"portMapping":{"port":80,"protocol":"http"},` +
				`"timeout":{"http":{"idle":{"unit":"s","value":5}}}}],"serviceDiscovery":{"dns":{"hostname":"a"}}}`,
		},
		{
			name: "node_bad_protocol", kind: "node", wantErr: true,
			spec: `{"listeners":[{"portMapping":{"port":80,"protocol":"udp"}}]}`,
		},
		{name: "node_missing_port_mapping", kind: "node", spec: `{"listeners":[{}]}`, wantErr: true},
		{
			name: "node_health_check_missing_threshold", kind: "node", wantErr: true,
			spec: `{"listeners":[{"portMapping":{"port":80,"protocol":"http"},` +
				`"healthCheck":{"protocol":"http","intervalMillis":5000,"timeoutMillis":2000,"unhealthyThreshold":2}}]}`,
		},
		{
			name: "node_two_timeouts", kind: "node", wantErr: true,
			spec: `{"listeners":[{"portMapping":{"port":80,"protocol":"http"},"timeout":{"http":{},"tcp":{}}}]}`,
		},
		{name: "node_dns_without_hostname", kind: "node", spec: `{"serviceDiscovery":{"dns":{}}}`, wantErr: true},
		{
			name: "node_bad_tls_mode", kind: "node", wantErr: true,
			spec: `{"listeners":[{"portMapping":{"port":80,"protocol":"http"},` +
				`"tls":{"mode":"LOOSE","certificate":{"sds":{"secretName":"s"}}}}]}`,
		},
		{
			name: "route_ok", kind: "route",
			spec: `{"httpRoute":{"action":` + oneTarget + `,"match":{"prefix":"/"}}}`,
		},
		{
			name: "route_weight_out_of_range", kind: "route", wantErr: true,
			spec: `{"httpRoute":{"action":{"weightedTargets":[{"virtualNode":"n","weight":101}]},"match":{}}}`,
		},
		{name: "route_missing_action", kind: "route", spec: `{"tcpRoute":{}}`, wantErr: true},
		{
			name: "route_bad_method", kind: "route", wantErr: true,
			spec: `{"httpRoute":{"action":` + oneTarget + `,"match":{"method":"FETCH"}}}`,
		},
		{
			name: "route_header_two_matchers", kind: "route", wantErr: true,
			spec: `{"httpRoute":{"action":` + oneTarget +
				`,"match":{"headers":[{"name":"h","match":{"exact":"a","prefix":"b"}}]}}}`,
		},
		{name: "gateway_ok", kind: "gateway", spec: `{"listeners":[` + httpPortMapping + `]}`},
		{
			name: "gateway_tcp_protocol", kind: "gateway", wantErr: true,
			spec: `{"listeners":[{"portMapping":{"port":80,"protocol":"tcp"}}]}`,
		},
		{name: "gateway_missing_listeners", kind: "gateway", spec: `{}`, wantErr: true},
		{
			name: "gateway_route_ok", kind: "gwroute",
			spec: `{"httpRoute":{"action":` + svcTarget + `,"match":{"prefix":"/"}}}`,
		},
		{
			name: "gateway_route_missing_target", kind: "gwroute", wantErr: true,
			spec: `{"httpRoute":{"action":{},"match":{"prefix":"/"}}}`,
		},
		{
			name: "gateway_route_bad_rewrite", kind: "gwroute", wantErr: true,
			spec: `{"grpcRoute":{"action":{"target":{"virtualService":{"virtualServiceName":"s"}},` +
				`"rewrite":{"hostname":{"defaultTargetHostname":"MAYBE"}}},"match":{}}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := appmesh.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateMesh("m", nil, nil)
			require.NoError(t, err)
			_, err = b.CreateVirtualRouter("m", "r", nil, nil)
			require.NoError(t, err)
			_, err = b.CreateVirtualGateway("m", "g", json.RawMessage(`{"listeners":[`+httpPortMapping+`]}`), nil)
			require.NoError(t, err)

			spec := json.RawMessage(tt.spec)

			switch tt.kind {
			case "node":
				_, err = b.CreateVirtualNode("m", "n", spec, nil)
			case "route":
				_, err = b.CreateRoute("m", "r", "rt", spec, nil)
			case "gateway":
				_, err = b.CreateVirtualGateway("m", "g2", spec, nil)
			case "gwroute":
				_, err = b.CreateGatewayRoute("m", "g", "gr", spec, nil)
			}

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrInvalidParameter)

				return
			}

			require.NoError(t, err)
		})
	}
}
