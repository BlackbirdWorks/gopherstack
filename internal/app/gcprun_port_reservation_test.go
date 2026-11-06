package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	gcprunbackend "github.com/blackbirdworks/gopherstack/services/gcprun"
)

func TestReserveFixedServicePorts_GCPRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		port      int
		wantTaken bool
	}{
		{"grpc port inside pool range", gcprunbackend.DefaultPort, true},
		{"grpc port outside pool range", gcprunbackend.DefaultPort, false},
		{"ingress port inside pool range", gcprunbackend.DefaultIngressPort, true},
		{"ingress port outside pool range", gcprunbackend.DefaultIngressPort, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, ports := fixedPortPools(t, tt.wantTaken, gcprunbackend.DefaultPort, gcprunbackend.DefaultIngressPort)

			cli := CLI{GCPRun: gcprunbackend.Settings{Port: ports[0], IngressPort: ports[1]}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			port := ports[0]
			if tt.port == gcprunbackend.DefaultIngressPort {
				port = ports[1]
			}

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
		})
	}
}
