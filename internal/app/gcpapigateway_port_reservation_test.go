package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	gcpapigatewaybackend "github.com/blackbirdworks/gopherstack/services/gcpapigateway"
)

func TestReserveFixedServicePorts_GCPAPIGateway(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		port      int
		wantTaken bool
	}{
		{"port inside pool range", gcpapigatewaybackend.DefaultPort, true},
		{"port outside pool range", gcpapigatewaybackend.DefaultPort, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, port := fixedPortPool(t, tt.wantTaken, tt.port)

			cli := CLI{GCPAPIGateway: gcpapigatewaybackend.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
		})
	}
}
