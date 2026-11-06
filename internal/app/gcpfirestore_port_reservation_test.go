package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	gcpfirestorebackend "github.com/blackbirdworks/gopherstack/services/gcpfirestore"
)

func TestReserveFixedServicePorts_GCPFirestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		port      int
		wantTaken bool
	}{
		{"port inside pool range", gcpfirestorebackend.DefaultPort, true},
		{"port outside pool range", gcpfirestorebackend.DefaultPort, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, port := fixedPortPool(t, tt.wantTaken, tt.port)

			cli := CLI{GCPFirestore: gcpfirestorebackend.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
		})
	}
}
