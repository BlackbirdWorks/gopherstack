package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ociartifacts"
	ociidentitybackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
	ociobjectstorage "github.com/blackbirdworks/gopherstack/services/ociobjectstorage"
)

func TestReserveFixedServicePorts_OCIArtifacts(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 8166, ociartifacts.DefaultPort)

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

			alloc, port := fixedPortPool(t, tt.wantTaken, ociartifacts.DefaultPort)

			cli := CLI{OCIArtifacts: ociartifacts.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
		})
	}
}

func TestWireOCIArtifactsSettingsAndLinking(t *testing.T) {
	t.Parallel()

	idH := ociidentitybackend.NewHandler(ociidentitybackend.NewInMemoryBackend())
	osH := ociobjectstorage.NewHandler(ociobjectstorage.NewInMemoryBackend())
	osH.Backend.SetNamespace("tenantns")
	artH := ociartifacts.NewHandler(ociartifacts.NewInMemoryBackend())

	assert.NotPanics(t, func() {
		wireOCIArtifacts(
			map[string]service.Registerable{"OCIIdentity": idH, "OCIObjectStorage": osH, "OCIArtifacts": artH},
		)
		wireOCIArtifacts(map[string]service.Registerable{"OCIArtifacts": artH})
	})

	assert.Equal(t, "tenantns", artH.Backend.Namespace())

	cli := CLI{OCIArtifacts: ociartifacts.Settings{Port: 8166}}
	assert.Equal(t, 8166, cli.GetOCIArtifactsSettings().Port)
}
