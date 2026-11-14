package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	gcpbigtablebackend "github.com/blackbirdworks/gopherstack/services/gcpbigtable"
)

func TestReserveFixedServicePorts_GCPBigtable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		port      int
		wantTaken bool
	}{
		{"port inside pool range", gcpbigtablebackend.DefaultPort, true},
		{"port outside pool range", gcpbigtablebackend.DefaultPort, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, port := fixedPortPool(t, tt.wantTaken, tt.port)

			cli := CLI{GCPBigtable: gcpbigtablebackend.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
		})
	}
}
