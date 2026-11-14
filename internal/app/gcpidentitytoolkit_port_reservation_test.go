package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	gcpidtkbackend "github.com/blackbirdworks/gopherstack/services/gcpidentitytoolkit"
)

func TestReserveFixedServicePorts_GCPIdentityToolkit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		port      int
		wantTaken bool
	}{
		{"port inside pool range", gcpidtkbackend.DefaultPort, true},
		{"port outside pool range", gcpidtkbackend.DefaultPort, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, port := fixedPortPool(t, tt.wantTaken, tt.port)

			cli := CLI{GCPIdentityToolkit: gcpidtkbackend.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
		})
	}
}
