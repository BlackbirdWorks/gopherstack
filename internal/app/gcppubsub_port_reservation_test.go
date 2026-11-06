package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	gcppubsubbackend "github.com/blackbirdworks/gopherstack/services/gcppubsub"
)

func TestReserveFixedServicePorts_GCPPubSub(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		port      int
		wantTaken bool
	}{
		{"port inside pool range", gcppubsubbackend.DefaultPort, true},
		{"port outside pool range", gcppubsubbackend.DefaultPort, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, port := fixedPortPool(t, tt.wantTaken, tt.port)

			cli := CLI{GCPPubSub: gcppubsubbackend.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
		})
	}
}
