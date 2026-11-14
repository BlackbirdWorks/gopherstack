package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	gcpeventarcbackend "github.com/blackbirdworks/gopherstack/services/gcpeventarc"
)

func TestReserveFixedServicePorts_GCPEventarc(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		port      int
		wantTaken bool
	}{
		{"port inside pool range", gcpeventarcbackend.DefaultPort, true},
		{"port outside pool range", gcpeventarcbackend.DefaultPort, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, port := fixedPortPool(t, tt.wantTaken, tt.port)

			cli := CLI{GCPEventarc: gcpeventarcbackend.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
		})
	}
}
