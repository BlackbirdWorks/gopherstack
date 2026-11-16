package app

import (
	"log/slog"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ocicorebackend "github.com/blackbirdworks/gopherstack/services/ocicore"
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

func TestWireOCILoadBalancerPoolClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wire       bool
		wantStatus int
	}{
		{name: "wired", wire: true, wantStatus: http.StatusNotFound},
		{name: "unwired", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			coreH := ocicorebackend.NewHandler(ocicorebackend.NewInMemoryBackend())
			lbH := ociloadbalancer.NewHandler(ociloadbalancer.NewInMemoryBackend())

			if tt.wire {
				wireOCILoadBalancer(map[string]service.Registerable{"OCICore": coreH, "OCILoadBalancer": lbH})
			}

			port := 80

			_, _, aerr := coreH.Backend.AttachLoadBalancer(
				"ocid1.instancepool.oc1.iad.x",
				"",
				ocicorebackend.AttachLoadBalancerIn{
					LoadBalancerID: "ocid1.loadbalancer.oc1.iad.x", BackendSetName: "web",
					VnicSelection: "PrimaryVnic", Port: &port,
				},
			)
			require.NotNil(t, aerr)
			assert.Equal(t, tt.wantStatus, aerr.Status)
		})
	}
}

func TestPoolLoadBalancersAdapter(t *testing.T) {
	t.Parallel()

	lb := ociloadbalancer.NewInMemoryBackend()
	a := poolLoadBalancers{b: lb}

	assert.False(t, a.HasBackendSet("ocid1.loadbalancer.oc1.iad.x", "web"))

	err := a.ApplyBackends("ocid1.loadbalancer.oc1.iad.x", []ocicorebackend.BackendChange{{
		Set: "web", Add: []ocicorebackend.BackendEndpoint{{IP: "10.0.0.1", Port: 80}},
	}})
	require.ErrorIs(t, err, ocicorebackend.ErrTargetGone)
}
