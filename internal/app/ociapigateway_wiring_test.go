package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ociapigateway "github.com/blackbirdworks/gopherstack/services/ociapigateway"
	ociidentitybackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
)

func TestReserveFixedServicePorts_OCIAPIGateway(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 8163, ociapigateway.DefaultPort)

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

			cli := CLI{OCIAPIGateway: ociapigateway.Settings{Port: ociapigateway.DefaultPort}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(ociapigateway.DefaultPort))
		})
	}
}

func TestWireOCIAPIGatewaySettingsAndLinking(t *testing.T) {
	t.Parallel()

	idH := ociidentitybackend.NewHandler(ociidentitybackend.NewInMemoryBackend())
	gwH := ociapigateway.NewHandler(ociapigateway.NewInMemoryBackend())

	assert.NotPanics(t, func() {
		wireOCIAPIGateway(map[string]service.Registerable{"OCIIdentity": idH, "OCIAPIGateway": gwH})
		wireOCIAPIGateway(map[string]service.Registerable{"OCIAPIGateway": gwH})
	})

	cli := CLI{OCIAPIGateway: ociapigateway.Settings{Port: 8163}}
	assert.Equal(t, 8163, cli.GetOCIAPIGatewaySettings().Port)
}
