package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	gcploggingbackend "github.com/blackbirdworks/gopherstack/services/gcplogging"
)

func TestReserveFixedServicePorts_GCPLogging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantTaken bool
	}{
		{"port inside pool range", true},
		{"port outside pool range", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, port := fixedPortPool(t, tt.wantTaken, gcploggingbackend.DefaultPort)

			cli := CLI{GCPLogging: gcploggingbackend.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
		})
	}
}
