package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	gcpstoragebackend "github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

func TestReserveFixedServicePorts_GCPStorage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		port      int
		wantTaken bool
	}{
		{"port inside pool range", gcpstoragebackend.DefaultPort, true},
		{"port outside pool range", gcpstoragebackend.DefaultPort, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, port := fixedPortPool(t, tt.wantTaken, tt.port)

			cli := CLI{GCPStorage: gcpstoragebackend.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
		})
	}
}
