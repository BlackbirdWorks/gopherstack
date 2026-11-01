package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ocicontainerinstances "github.com/blackbirdworks/gopherstack/services/ocicontainerinstances"
	ociidentitybackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
)

func TestReserveFixedServicePorts_OCIContainerInstances(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 8167, ocicontainerinstances.DefaultPort)

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

			cli := CLI{OCIContainerInstances: ocicontainerinstances.Settings{Port: ocicontainerinstances.DefaultPort}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(ocicontainerinstances.DefaultPort))
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
