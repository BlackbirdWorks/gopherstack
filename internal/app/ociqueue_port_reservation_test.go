package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	ociqueuebackend "github.com/blackbirdworks/gopherstack/services/ociqueue"
)

func TestReserveFixedServicePorts_OCIQueue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		port      int
		wantTaken bool
	}{
		{"port inside pool range", ociqueuebackend.DefaultPort, true},
		{"port outside pool range", ociqueuebackend.DefaultPort, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, port := fixedPortPool(t, tt.wantTaken, tt.port)

			cli := CLI{OCIQueue: ociqueuebackend.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
		})
	}
}
