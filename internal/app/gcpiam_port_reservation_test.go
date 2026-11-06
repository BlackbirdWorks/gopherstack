package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	gcpiambackend "github.com/blackbirdworks/gopherstack/services/gcpiam"
)

func TestReserveFixedServicePorts_GCPIAM(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		port      int
		wantTaken bool
	}{
		{"port inside pool range", gcpiambackend.DefaultPort, true},
		{"port outside pool range", gcpiambackend.DefaultPort, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, port := fixedPortPool(t, tt.wantTaken, tt.port)

			cli := CLI{GCPIAM: gcpiambackend.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
		})
	}
}
