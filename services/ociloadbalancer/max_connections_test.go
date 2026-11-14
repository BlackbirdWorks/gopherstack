package ociloadbalancer_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/loadbalancer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaxConnectionsBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   int
		wantErr bool
	}{
		{"unlimited", 0, false},
		{"below minimum", 255, true},
		{"minimum", 256, false},
		{"negative", -1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			id := e.mkLB(t, baseDetails("maxconn"))
			b := startBackend(t, "a")

			_, err := e.lb.CreateBackendSet(t.Context(), loadbalancer.CreateBackendSetRequest{
				LoadBalancerId: &id,
				Name:           new("web"), Policy: new("ROUND_ROBIN"), HealthChecker: hc(),
				BackendMaxConnections: new(tt.value),
			})

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

				return
			}

			require.NoError(t, err)
			e.settle()

			_, err = e.lb.CreateBackend(t.Context(), loadbalancer.CreateBackendRequest{
				LoadBalancerId: &id, BackendSetName: new("web"),
				IpAddress: new(b.ip()), Port: new(b.port()), MaxConnections: new(tt.value),
			})
			require.NoError(t, err)
		})
	}
}
