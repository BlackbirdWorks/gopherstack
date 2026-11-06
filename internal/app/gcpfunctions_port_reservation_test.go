package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	gcpfunctionsbackend "github.com/blackbirdworks/gopherstack/services/gcpfunctions"
)

func TestReserveFixedServicePorts_GCPFunctions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		port      int
		wantTaken bool
	}{
		{"grpc port inside pool range", gcpfunctionsbackend.DefaultPort, true},
		{"grpc port outside pool range", gcpfunctionsbackend.DefaultPort, false},
		{"ingress port inside pool range", gcpfunctionsbackend.DefaultIngressPort, true},
		{"ingress port outside pool range", gcpfunctionsbackend.DefaultIngressPort, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, ports := fixedPortPools(
				t, tt.wantTaken, gcpfunctionsbackend.DefaultPort, gcpfunctionsbackend.DefaultIngressPort,
			)

			cli := CLI{GCPFunctions: gcpfunctionsbackend.Settings{Port: ports[0], IngressPort: ports[1]}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			port := ports[0]
			if tt.port == gcpfunctionsbackend.DefaultIngressPort {
				port = ports[1]
			}

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
		})
	}
}
