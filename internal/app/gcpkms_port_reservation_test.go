package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	gcpkmsbackend "github.com/blackbirdworks/gopherstack/services/gcpkms"
)

func TestReserveFixedServicePorts_GCPKMS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		port      int
		wantTaken bool
	}{
		{"port inside pool range", gcpkmsbackend.DefaultPort, true},
		{"port outside pool range", gcpkmsbackend.DefaultPort, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, port := fixedPortPool(t, tt.wantTaken, tt.port)

			cli := CLI{GCPKMS: gcpkmsbackend.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
		})
	}
}
