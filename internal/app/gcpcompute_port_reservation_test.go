package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	gcpcomputebackend "github.com/blackbirdworks/gopherstack/services/gcpcompute"
)

func TestReserveFixedServicePorts_GCPCompute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		port      int
		wantTaken bool
	}{
		{"port inside pool range", gcpcomputebackend.DefaultPort, true},
		{"port outside pool range", gcpcomputebackend.DefaultPort, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, port := fixedPortPool(t, tt.wantTaken, tt.port)

			cli := CLI{GCPCompute: gcpcomputebackend.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
		})
	}
}
