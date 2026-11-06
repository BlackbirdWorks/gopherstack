package app

import (
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ocicorebackend "github.com/blackbirdworks/gopherstack/services/ocicore"
	"github.com/blackbirdworks/gopherstack/services/ocifilestorage"
	ociidentitybackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
)

func TestReserveFixedServicePorts_OCIFileStorage(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 8171, ocifilestorage.DefaultPort)

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

			alloc, port := fixedPortPool(t, tt.wantTaken, ocifilestorage.DefaultPort)

			settings := ocifilestorage.DefaultSettings()
			settings.Port = port
			cli := CLI{OCIFileStorage: settings}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
		})
	}
}

func TestOCIFileStorageEngineIgnoresGlobalSwitch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cli  *CLI
		name string
		want string
	}{
		{name: "default", want: ocifilestorage.EngineStub, cli: &CLI{OCIFileStorage: ocifilestorage.DefaultSettings()}},
		{
			name: "explicit",
			want: ocifilestorage.EngineNFS,
			cli:  &CLI{OCIFileStorage: ocifilestorage.Settings{Engine: ocifilestorage.EngineNFS}},
		},
		{
			name: "global-docker", want: ocifilestorage.EngineStub,
			cli: &CLI{
				OCIFileStorage: ocifilestorage.DefaultSettings(),
				engineModes:    map[string]string{"oci-filestorage-engine": "docker"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.cli.GetOCIFileStorageEngine())
		})
	}
}

func idOf(t *testing.T, v any) string {
	t.Helper()

	raw, err := json.Marshal(v)
	require.NoError(t, err)

	var out struct {
		ID string `json:"id"`
	}

	require.NoError(t, json.Unmarshal(raw, &out))

	return out.ID
}

func TestWireOCIFileStorageAgainstCoreAndIdentity(t *testing.T) {
	t.Parallel()

	idH := ociidentitybackend.NewHandler(ociidentitybackend.NewInMemoryBackend())
	coreH := ocicorebackend.NewHandler(ocicorebackend.NewInMemoryBackend())
	fsH := ocifilestorage.NewHandler(ocifilestorage.NewInMemoryBackend())

	assert.NotPanics(t, func() { wireOCIFileStorage(map[string]service.Registerable{"OCIFileStorage": fsH}) })
	assert.NotPanics(t, func() { wireOCIFileStorage(map[string]service.Registerable{}) })

	wireOCIFileStorage(map[string]service.Registerable{"OCICore": coreH, "OCIFileStorage": fsH})

	const comp = "ocid1.compartment.oc1..c"

	vcn, _, aerr := coreH.Backend.CreateVcn(
		ocicorebackend.CreateVcnIn{CompartmentID: comp, DisplayName: "v", CidrBlock: "10.0.0.0/16"},
	)
	require.Nil(t, aerr)

	sub, _, aerr := coreH.Backend.CreateSubnet(
		ocicorebackend.CreateSubnetIn{CompartmentID: comp, VcnID: idOf(t, vcn), CidrBlock: "10.0.1.0/24"},
	)
	require.Nil(t, aerr)

	create := func() error {
		_, _, err := fsH.Backend.CreateMountTarget(ocifilestorage.CreateMountTargetIn{
			AvailabilityDomain: "ad-1", CompartmentID: comp, SubnetID: idOf(t, sub), IPAddress: new("10.0.1.40"),
		}, "")

		return err
	}

	require.NoError(t, create())
	require.Error(t, create(), "the mount target holds a real private IP in OCI Core")

	mts, err := fsH.Backend.ListMountTargets(
		ocifilestorage.MTFilter{Compartment: comp, AD: "ad-1"},
		ocifilestorage.ListOpts{},
	)
	require.NoError(t, err)
	require.Len(t, mts.Data, 1)

	require.NoError(t, fsH.Backend.DeleteMountTarget(mts.Data[0]["id"].(string), "", false))
	require.NoError(t, create(), "deleting the mount target releases its VNIC")

	wireOCIFileStorage(map[string]service.Registerable{"OCIIdentity": idH, "OCICore": coreH, "OCIFileStorage": fsH})

	_, err = fsH.Backend.ListFileSystems(
		ocifilestorage.FSFilter{Compartment: "ocid1.compartment.oc1..ghost", AD: "ad-1"}, ocifilestorage.ListOpts{},
	)
	require.Error(t, err, "compartments are checked against OCI Identity")

	cli := CLI{OCIFileStorage: ocifilestorage.Settings{Port: 8171}}
	assert.Equal(t, 8171, cli.GetOCIFileStorageSettings().Port)
}
