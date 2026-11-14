package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	gcpdnsbackend "github.com/blackbirdworks/gopherstack/services/gcpdns"
)

func TestReserveFixedServicePorts_GCPDNS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		port      int
		dnsPort   int
		wantTaken bool
	}{
		{"ports inside pool range", gcpdnsbackend.DefaultPort, gcpdnsbackend.DefaultDNSPort, true},
		{"ports outside pool range", gcpdnsbackend.DefaultPort, gcpdnsbackend.DefaultDNSPort, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, ports := fixedPortPools(t, tt.wantTaken, tt.port, tt.dnsPort)

			cli := CLI{GCPDNS: gcpdnsbackend.Settings{Port: ports[0], DNSPort: ports[1]}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(ports[0]))
			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(ports[1]))
		})
	}
}
