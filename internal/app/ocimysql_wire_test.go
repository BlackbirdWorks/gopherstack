package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ocicorebackend "github.com/blackbirdworks/gopherstack/services/ocicore"
	ocimysqlbackend "github.com/blackbirdworks/gopherstack/services/ocimysql"
)

const mysqlTestCompartment = "ocid1.compartment.oc1..c"

func TestReserveFixedServicePorts_OCIMySQL(t *testing.T) {
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

			cli := CLI{OCIMySQL: ocimysqlbackend.Settings{Port: ocimysqlbackend.DefaultPort}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(ocimysqlbackend.DefaultPort))
		})
	}
}

func TestWireOCIMySQLAgainstCore(t *testing.T) {
	t.Parallel()

	coreH := ocicorebackend.NewHandler(ocicorebackend.NewInMemoryBackend())
	mysqlH := ocimysqlbackend.NewHandler(ocimysqlbackend.NewInMemoryBackend())

	wireOCIMySQL(map[string]service.Registerable{"OCICore": coreH, "OCIMySQL": mysqlH})

	vcn, _, aerr := coreH.Backend.CreateVcn(ocicorebackend.CreateVcnIn{
		CompartmentID: mysqlTestCompartment, CidrBlocks: []string{"10.0.0.0/16"},
	})
	require.Nil(t, aerr)

	vcnID := vcn.(*ocicorebackend.Vcn).ID //nolint:forcetypeassert // CreateVcn returns *Vcn

	sub, _, aerr := coreH.Backend.CreateSubnet(ocicorebackend.CreateSubnetIn{
		CompartmentID: mysqlTestCompartment, VcnID: vcnID, CidrBlock: "10.0.1.0/24",
	})
	require.Nil(t, aerr)

	subID := sub.(*ocicorebackend.Subnet).ID //nolint:forcetypeassert // CreateSubnet returns *Subnet

	create := func(subnet string) error {
		_, _, _, err := mysqlH.Backend.CreateDBSystem(ocimysqlbackend.CreateDBSystemIn{
			CompartmentID: mysqlTestCompartment, ShapeName: "MySQL.2", SubnetID: subnet,
			AdminUsername: "dbadmin", AdminPassword: "Passw0rd!x",
		})

		return err
	}

	require.Error(t, create("ocid1.subnet.oc1..ghost"), "subnets must exist in OCI Core")
	require.NoError(t, create(subID))

	list, err := mysqlH.Backend.ListDBSystems(ocimysqlbackend.DBSystemListIn{Compartment: mysqlTestCompartment})
	require.NoError(t, err)
	require.Len(t, list.Data, 1)

	d, _, err := mysqlH.Backend.GetDBSystem(list.Data[0]["id"].(string)) //nolint:forcetypeassert // id is a string
	require.NoError(t, err)
	assert.Equal(t, "10.0.1.5", d["ipAddress"], "the endpoint address is allocated from the subnet CIDR")
}
