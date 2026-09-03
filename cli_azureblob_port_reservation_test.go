package main

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	azureblobbackend "github.com/blackbirdworks/gopherstack/services/azureblob"
)

// TestReserveFixedServicePorts is a regression test: services/azureblob binds
// its dedicated listener directly via net.Listen, not through PortAlloc, but
// its default port (10000) sits inside PortRangeStart/PortRangeEnd's own
// default range (10000-10100). Without reserving it, PortAlloc could still
// hand that same port number to an unrelated caller (e.g. ElastiCache),
// which would only surface later as a confusing address-in-use failure. See
// AZURE.md section 4 and pkgs/portalloc.Allocator.Reserve's doc comment.
func TestReserveFixedServicePorts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		azurePort           int
		rangeStart          int
		rangeEnd            int
		wantBlockedFromPool bool
	}{
		{
			name:      "default azure port collides with default pool range",
			azurePort: azureblobbackend.DefaultPort, rangeStart: 10000, rangeEnd: 10100,
			wantBlockedFromPool: true,
		},
		{
			name:      "custom azure port outside a custom pool range",
			azurePort: 9999, rangeStart: 10000, rangeEnd: 10100,
			wantBlockedFromPool: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, err := portalloc.New(tt.rangeStart, tt.rangeEnd)
			require.NoError(t, err)

			cli := CLI{AzureBlob: azureblobbackend.Settings{Port: tt.azurePort}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantBlockedFromPool, alloc.IsAllocated(tt.azurePort), tt.name)
		})
	}
}

// TestReserveFixedServicePorts_NilAllocatorIsNoop covers the disabled-pool
// path (setupPortAllocator returns nil for an invalid range): nothing to
// reserve against, must not panic.
func TestReserveFixedServicePorts_NilAllocatorIsNoop(t *testing.T) {
	t.Parallel()

	cli := CLI{AzureBlob: azureblobbackend.Settings{Port: azureblobbackend.DefaultPort}}

	assert.NotPanics(t, func() {
		reserveFixedServicePorts(t.Context(), slog.Default(), nil, cli)
	})
}
