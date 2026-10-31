package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ociidentitybackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
	ociloadbalancer "github.com/blackbirdworks/gopherstack/services/ociloadbalancer"
)

func TestReserveFixedServicePorts_OCILoadBalancer(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 8165, ociloadbalancer.DefaultPort)

	tests := []struct {
		name       string
		rangeStart int
		rangeEnd   int
		wantTaken  bool
	}{
		{"port inside pool range", 8100, 8200, true},
		{"port outside pool range", 10000, 10100, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, err := portalloc.New(tt.rangeStart, tt.rangeEnd)
			require.NoError(t, err)

			cli := CLI{OCILoadBalancer: ociloadbalancer.Settings{Port: ociloadbalancer.DefaultPort}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(ociloadbalancer.DefaultPort))
		})
	}
}

func TestWireOCILoadBalancerSettingsAndLinking(t *testing.T) {
	t.Parallel()

	idH := ociidentitybackend.NewHandler(ociidentitybackend.NewInMemoryBackend())
	lbH := ociloadbalancer.NewHandler(ociloadbalancer.NewInMemoryBackend())

	assert.NotPanics(t, func() {
		wireOCILoadBalancer(map[string]service.Registerable{"OCIIdentity": idH, "OCILoadBalancer": lbH})
		wireOCILoadBalancer(map[string]service.Registerable{"OCILoadBalancer": lbH})
	})

	cli := CLI{OCILoadBalancer: ociloadbalancer.Settings{Port: 8165}}
	assert.Equal(t, 8165, cli.GetOCILoadBalancerSettings().Port)
}
