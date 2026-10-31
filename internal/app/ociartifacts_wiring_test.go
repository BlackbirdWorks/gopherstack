package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ociartifacts"
	ociidentitybackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
	ociobjectstorage "github.com/blackbirdworks/gopherstack/services/ociobjectstorage"
)

func TestReserveFixedServicePorts_OCIArtifacts(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 8166, ociartifacts.DefaultPort)

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

			cli := CLI{OCIArtifacts: ociartifacts.Settings{Port: ociartifacts.DefaultPort}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(ociartifacts.DefaultPort))
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
