package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ocioke "github.com/blackbirdworks/gopherstack/services/ocicontainerengine"
	ocicorebackend "github.com/blackbirdworks/gopherstack/services/ocicore"
	ocivaultbackend "github.com/blackbirdworks/gopherstack/services/ocivault"
)

const okeTestCompartment = "ocid1.compartment.oc1..c"

func TestReserveFixedServicePorts_OCIContainerEngine(t *testing.T) {
	t.Parallel()

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

			cli := CLI{OCIContainerEngine: ocioke.Settings{Port: ocioke.DefaultPort}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(ocioke.DefaultPort))
		})
	}
}

func TestWireOCIContainerEngineAgainstCore(t *testing.T) {
	t.Parallel()

	coreH := ocicorebackend.NewHandler(ocicorebackend.NewInMemoryBackend())
	okeH := ocioke.NewHandler(ocioke.NewInMemoryBackend())
	vaultH := ocivaultbackend.NewHandler(ocivaultbackend.NewInMemoryBackend())

	wireOCIContainerEngine(map[string]service.Registerable{
		"OCICore": coreH, "OCIContainerEngine": okeH, "OCIVault": vaultH,
	})

	okeH.Backend.SetTransition(0)

	vcn, _, aerr := coreH.Backend.CreateVcn(ocicorebackend.CreateVcnIn{
		CompartmentID: okeTestCompartment, CidrBlocks: []string{"10.0.0.0/16"},
	})
	require.Nil(t, aerr)

	vcnID := vcn.(*ocicorebackend.Vcn).ID //nolint:forcetypeassert // CreateVcn returns *Vcn

	sub, _, aerr := coreH.Backend.CreateSubnet(ocicorebackend.CreateSubnetIn{
		CompartmentID: okeTestCompartment, VcnID: vcnID, CidrBlock: "10.0.1.0/24",
	})
	require.Nil(t, aerr)

	subID := sub.(*ocicorebackend.Subnet).ID //nolint:forcetypeassert // CreateSubnet returns *Subnet

	cluster := func(name, subnet string) ocioke.CreateClusterIn {
		return ocioke.CreateClusterIn{
			Name: name, Compartment: okeTestCompartment, Version: "v1.34.2", VcnID: &vcnID,
			EndpointConfig: &ocioke.EndpointConfig{SubnetID: subnet},
		}
	}

	_, err := okeH.Backend.CreateCluster(cluster("ghost", "ocid1.subnet.oc1..ghost"))
	require.Error(t, err, "subnets must exist in OCI Core")

	_, err = okeH.Backend.CreateCluster(cluster("real", subID))
	require.NoError(t, err)

	list, err := okeH.Backend.ListClusters(ocioke.ClusterListIn{Compartment: okeTestCompartment})
	require.NoError(t, err)
	require.Len(t, list.Data, 1)

	opts, err := okeH.Backend.GetNodePoolOptions(ocioke.NodePoolOptionsIn{ID: "all"})
	require.NoError(t, err)
	assert.Contains(t, opts.Shapes, "VM.Standard.E4.Flex")
	assert.NotEmpty(t, opts.Images)
	require.NotEmpty(t, opts.Sources)

	img := ocicorebackend.CatalogImages()[0]
	pool := func(shape string) ocioke.CreateNodePoolIn {
		size := 1

		return ocioke.CreateNodePoolIn{
			Compartment: okeTestCompartment, ClusterID: list.Data[0].ID, Name: "p", Shape: shape,
			Source: &ocioke.NodeSource{ImageID: img.ID},
			NodeConfig: &ocioke.NodeConfigIn{
				Size:       &size,
				Placements: []ocioke.Placement{{AvailabilityDom: "Uocm:US-ASHBURN-AD-1", SubnetID: subID}},
			},
		}
	}

	_, err = okeH.Backend.CreateNodePool(pool("VM.Nope"))
	require.Error(t, err, "shapes come from the OCI Core catalogue")

	_, err = okeH.Backend.CreateNodePool(pool("VM.Standard2.1"))
	require.NoError(t, err)

	got, _, err := okeH.Backend.GetCluster(list.Data[0].ID)
	require.NoError(t, err)
	assert.Equal(t, "ACTIVE", got.LifecycleState)
	assert.Equal(t, "10.0.1.5:6443", got.Endpoints.PrivateEndpoint, "endpoint address comes from the subnet CIDR")
}
