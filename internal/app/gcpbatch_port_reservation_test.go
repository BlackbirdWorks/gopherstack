package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	gcpbatchbackend "github.com/blackbirdworks/gopherstack/services/gcpbatch"
)

func TestReserveFixedServicePorts_GCPBatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		port      int
		wantTaken bool
	}{
		{"port inside pool range", gcpbatchbackend.DefaultPort, true},
		{"port outside pool range", gcpbatchbackend.DefaultPort, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, port := fixedPortPool(t, tt.wantTaken, tt.port)

			cli := CLI{GCPBatch: gcpbatchbackend.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
		})
	}
}
