package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
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

			alloc, err := portalloc.New(10000, 10100)
			require.NoError(t, err)

			cli := CLI{AzureGraph: azuregraphbackend.Settings{Port: tt.port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantBlockedFromPool, alloc.IsAllocated(tt.port))
		})
	}
}
