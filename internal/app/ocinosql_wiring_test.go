package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ociidentitybackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
	ocinosql "github.com/blackbirdworks/gopherstack/services/ocinosql"
)

func TestReserveFixedServicePorts_OCINoSQL(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 8162, ocinosql.DefaultPort)

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

			alloc, port := fixedPortPool(t, tt.wantTaken, ocinosql.DefaultPort)

			cli := CLI{OCINoSQL: ocinosql.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
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
