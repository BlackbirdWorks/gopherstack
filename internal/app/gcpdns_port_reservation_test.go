package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	gcpdnsbackend "github.com/blackbirdworks/gopherstack/services/gcpdns"
)

func TestReserveFixedServicePorts_GCPDNS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		port       int
		dnsPort    int
		rangeStart int
		rangeEnd   int
		wantTaken  bool
	}{
		{"ports inside pool range", gcpdnsbackend.DefaultPort, gcpdnsbackend.DefaultDNSPort, 8000, 9000, true},
		{"ports outside pool range", gcpdnsbackend.DefaultPort, gcpdnsbackend.DefaultDNSPort, 10000, 10100, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, err := portalloc.New(tt.rangeStart, tt.rangeEnd)
			require.NoError(t, err)

			cli := CLI{GCPDNS: gcpdnsbackend.Settings{Port: tt.port, DNSPort: tt.dnsPort}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(tt.port))
			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(tt.dnsPort))
		})
	}
}
