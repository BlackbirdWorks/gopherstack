package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ocicorebackend "github.com/blackbirdworks/gopherstack/services/ocicore"
	"github.com/blackbirdworks/gopherstack/services/ocidatabase"
	ociidentitybackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
)

func TestReserveFixedServicePorts_OCIDatabase(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 8170, ocidatabase.DefaultPort)

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

			alloc, port := fixedPortPool(t, tt.wantTaken, ocidatabase.DefaultPort)

			settings := ocidatabase.DefaultSettings()
			settings.Port = port
			cli := CLI{OCIDatabase: settings}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
		})
	}
}

func TestOCIDatabaseEngineFollowsGlobalSwitch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cli  func() *CLI
		name string
		want string
	}{
		{
			name: "stub-default", want: ocidatabase.EngineStub,
			cli: func() *CLI { return &CLI{OCIDatabase: ocidatabase.DefaultSettings()} },
		},
		{
			name: "explicit-docker", want: ocidatabase.EngineDocker,
			cli: func() *CLI { return &CLI{OCIDatabase: ocidatabase.Settings{Engine: ocidatabase.EngineDocker}} },
		},
		{
			name: "engines-switch", want: ocidatabase.EngineDocker,
			cli: func() *CLI {
				return &CLI{
					OCIDatabase: ocidatabase.DefaultSettings(),
					engineModes: map[string]string{"oci-database-engine": ocidatabase.EngineDocker},
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.cli().GetOCIDatabaseEngine())
		})
	}
}

func TestWireOCIDatabaseAgainstCoreAndIdentity(t *testing.T) {
	t.Parallel()

	idH := ociidentitybackend.NewHandler(ociidentitybackend.NewInMemoryBackend())
	coreH := ocicorebackend.NewHandler(ocicorebackend.NewInMemoryBackend())
	dbH := ocidatabase.NewHandler(ocidatabase.NewInMemoryBackend())

	assert.NotPanics(t, func() { wireOCIDatabase(map[string]service.Registerable{"OCIDatabase": dbH}) })
	assert.NotPanics(t, func() { wireOCIDatabase(map[string]service.Registerable{}) })

	wireOCIDatabase(map[string]service.Registerable{"OCIIdentity": idH, "OCICore": coreH, "OCIDatabase": dbH})

	_, err := dbH.Backend.ListBackups(
		ocidatabase.BackupFilter{Compartment: "ocid1.compartment.oc1..ghost"},
		ocidatabase.ListOpts{},
	)
	require.Error(t, err, "compartments are checked against OCI Identity")

	cli := CLI{OCIDatabase: ocidatabase.Settings{Port: 8170}}
	assert.Equal(t, 8170, cli.GetOCIDatabaseSettings().Port)
}
