package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	azuregraphbackend "github.com/blackbirdworks/gopherstack/services/azuregraph"
)

func TestReserveFixedServicePorts_AzureGraph(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		port                int
		wantBlockedFromPool bool
	}{
		{"default port collides with default pool range", azuregraphbackend.DefaultPort, true},
		{"custom port outside the pool range", 9996, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, port := fixedPortPool(t, tt.wantBlockedFromPool, tt.port)

			cli := CLI{AzureGraph: azuregraphbackend.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantBlockedFromPool, alloc.IsAllocated(port))
		})
	}
}
