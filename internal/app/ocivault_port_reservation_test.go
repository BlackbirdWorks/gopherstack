package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	ocivaultbackend "github.com/blackbirdworks/gopherstack/services/ocivault"
)

func TestReserveFixedServicePorts_OCIVault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		port      int
		wantTaken bool
	}{
		{"port inside pool range", ocivaultbackend.DefaultPort, true},
		{"port outside pool range", ocivaultbackend.DefaultPort, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, port := fixedPortPool(t, tt.wantTaken, tt.port)

			cli := CLI{OCIVault: ocivaultbackend.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
		})
	}
}
