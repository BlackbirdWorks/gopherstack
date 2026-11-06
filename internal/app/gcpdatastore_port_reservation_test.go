package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	gcpdatastorebackend "github.com/blackbirdworks/gopherstack/services/gcpdatastore"
)

func TestReserveFixedServicePorts_GCPDatastore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		port      int
		wantTaken bool
	}{
		{"port inside pool range", gcpdatastorebackend.DefaultPort, true},
		{"port outside pool range", gcpdatastorebackend.DefaultPort, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, port := fixedPortPool(t, tt.wantTaken, tt.port)

			cli := CLI{GCPDatastore: gcpdatastorebackend.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
		})
	}
}
