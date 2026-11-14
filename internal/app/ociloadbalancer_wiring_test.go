package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ociidentitybackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
	ociloadbalancer "github.com/blackbirdworks/gopherstack/services/ociloadbalancer"
)

func TestReserveFixedServicePorts_OCILoadBalancer(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 8165, ociloadbalancer.DefaultPort)

	tests := []struct {
		name      string
		wantTaken bool
	}{
		{"port inside pool range", true},
		{"port outside pool range", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, port := fixedPortPool(t, tt.wantTaken, ociloadbalancer.DefaultPort)

			cli := CLI{OCILoadBalancer: ociloadbalancer.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
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
