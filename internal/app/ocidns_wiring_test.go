package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ocicorebackend "github.com/blackbirdworks/gopherstack/services/ocicore"
	"github.com/blackbirdworks/gopherstack/services/ocidns"
	ociidentitybackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
)

func TestReserveFixedServicePorts_OCIDNS(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 8168, ocidns.DefaultPort)
	assert.Equal(t, 8169, ocidns.DefaultDNSPort)

	tests := []struct {
		name       string
		rangeStart int
		rangeEnd   int
		wantTaken  bool
	}{
		{"ports inside pool range", 8100, 8200, true},
		{"ports outside pool range", 10000, 10100, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, err := portalloc.New(tt.rangeStart, tt.rangeEnd)
			require.NoError(t, err)

			cli := CLI{OCIDNS: ocidns.DefaultSettings()}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(ocidns.DefaultPort))
			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(ocidns.DefaultDNSPort))
		})
	}
}

func TestWireOCIDNSAgainstCoreAndIdentity(t *testing.T) {
	t.Parallel()

	idH := ociidentitybackend.NewHandler(ociidentitybackend.NewInMemoryBackend())
	coreH := ocicorebackend.NewHandler(ocicorebackend.NewInMemoryBackend())
	dnsH := ocidns.NewHandler(ocidns.NewInMemoryBackend())

	assert.NotPanics(t, func() { wireOCIDNS(map[string]service.Registerable{"OCIDNS": dnsH}) })
	assert.NotPanics(t, func() { wireOCIDNS(map[string]service.Registerable{}) })

	wireOCIDNS(map[string]service.Registerable{"OCIIdentity": idH, "OCICore": coreH, "OCIDNS": dnsH})

	_, _, err := dnsH.Backend.CreateView(ocidns.CreateViewIn{CompartmentID: "ocid1.compartment.oc1..ghost"})
	require.Error(t, err, "compartments are checked against OCI Identity")

	const compartment = "ocid1.compartment.oc1..c"

	vcn, _, aerr := coreH.Backend.CreateVcn(ocicorebackend.CreateVcnIn{
		CompartmentID: compartment, CidrBlocks: []string{"10.0.0.0/16"}, DisplayName: "core-vcn",
	})
	require.Nil(t, aerr)

	vcnID := vcn.(*ocicorebackend.Vcn).ID //nolint:forcetypeassert // CreateVcn returns *Vcn

	sub, _, aerr := coreH.Backend.CreateSubnet(ocicorebackend.CreateSubnetIn{
		CompartmentID: compartment, VcnID: vcnID, CidrBlock: "10.0.1.0/24",
	})
	require.Nil(t, aerr)

	subID := sub.(*ocicorebackend.Subnet).ID //nolint:forcetypeassert // CreateSubnet returns *Subnet

	dnsH.Backend.SyncVcns()

	got, err := dnsH.Backend.ListResolvers(ocidns.ResolverFilter{}, ocidns.ListOpts{})
	require.Error(t, err, "compartmentId is required")
	assert.Empty(t, got.Items)

	dnsH.Backend.SetCompartmentChecker(nil)

	got, err = dnsH.Backend.ListResolvers(ocidns.ResolverFilter{Compart: compartment}, ocidns.ListOpts{})
	require.NoError(t, err)
	require.Len(t, got.Items, 1, "every OCI Core VCN gets a resolver")
	assert.Equal(t, vcnID, got.Items[0].AttachedVcnID)
	assert.Equal(t, "core-vcn", got.Items[0].DisplayName)

	yes, no := true, false

	ep, _, err := dnsH.Backend.CreateEndpoint(got.Items[0].ID, ocidns.CreateEndpointIn{
		Name: "ep", SubnetID: subID, IsForwarding: &yes, IsListening: &no,
	})
	require.NoError(t, err, "subnets resolve through OCI Core")
	assert.Equal(t, "10.0.1.2", *ep.ForwardingAddress)

	_, _, err = dnsH.Backend.CreateEndpoint(got.Items[0].ID, ocidns.CreateEndpointIn{
		Name: "ghost", SubnetID: "ocid1.subnet.oc1..ghost", IsForwarding: &yes, IsListening: &no,
	})
	require.Error(t, err)

	cli := CLI{OCIDNS: ocidns.Settings{Port: 8168, DNSPort: 8169}}
	assert.Equal(t, 8168, cli.GetOCIDNSSettings().Port)
	assert.Equal(t, 8169, cli.GetOCIDNSSettings().DNSPort)
}
