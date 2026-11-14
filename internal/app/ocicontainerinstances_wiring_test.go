package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ocicontainerinstances "github.com/blackbirdworks/gopherstack/services/ocicontainerinstances"
	ociidentitybackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
)

func TestReserveFixedServicePorts_OCIContainerInstances(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 8167, ocicontainerinstances.DefaultPort)

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

			alloc, port := fixedPortPool(t, tt.wantTaken, ocicontainerinstances.DefaultPort)

			cli := CLI{OCIContainerInstances: ocicontainerinstances.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
		})
	}
}

func TestWireOCIContainerInstancesSettingsAndLinking(t *testing.T) {
	t.Parallel()

	idH := ociidentitybackend.NewHandler(ociidentitybackend.NewInMemoryBackend())
	h := ocicontainerinstances.NewHandler(ocicontainerinstances.NewInMemoryBackend())

	assert.NotPanics(t, func() {
		wireOCIContainerInstances(map[string]service.Registerable{"OCIIdentity": idH, "OCIContainerInstances": h})
		wireOCIContainerInstances(map[string]service.Registerable{"OCIContainerInstances": h})
	})

	cli := CLI{OCIContainerInstances: ocicontainerinstances.Settings{Port: 8167, Engine: "docker"}}
	assert.Equal(t, 8167, cli.GetOCIContainerInstancesSettings().Port)
	assert.Equal(t, "docker", cli.GetOCIContainerInstancesSettings().Engine)
}
