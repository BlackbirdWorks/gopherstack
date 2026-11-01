package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ociidentitybackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
	ocinosql "github.com/blackbirdworks/gopherstack/services/ocinosql"
)

func TestReserveFixedServicePorts_OCINoSQL(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 8162, ocinosql.DefaultPort)

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

			cli := CLI{OCINoSQL: ocinosql.Settings{Port: ocinosql.DefaultPort}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(ocinosql.DefaultPort))
		})
	}
}

func TestWireOCINoSQLSettingsAndLinking(t *testing.T) {
	t.Parallel()

	idH := ociidentitybackend.NewHandler(ociidentitybackend.NewInMemoryBackend())
	nosqlH := ocinosql.NewHandler(ocinosql.NewInMemoryBackend())

	assert.NotPanics(t, func() {
		wireOCINoSQL(map[string]service.Registerable{"OCIIdentity": idH, "OCINoSQL": nosqlH})
		wireOCINoSQL(map[string]service.Registerable{"OCINoSQL": nosqlH})
	})

	cli := CLI{OCINoSQL: ocinosql.Settings{Port: 8162}}
	assert.Equal(t, 8162, cli.GetOCINoSQLSettings().Port)
}
