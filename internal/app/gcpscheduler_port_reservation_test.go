package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	gcpschedulerbackend "github.com/blackbirdworks/gopherstack/services/gcpscheduler"
)

func TestReserveFixedServicePorts_GCPScheduler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		port      int
		wantTaken bool
	}{
		{"port inside pool range", gcpschedulerbackend.DefaultPort, true},
		{"port outside pool range", gcpschedulerbackend.DefaultPort, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, port := fixedPortPool(t, tt.wantTaken, tt.port)

			cli := CLI{GCPScheduler: gcpschedulerbackend.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
		})
	}
}
