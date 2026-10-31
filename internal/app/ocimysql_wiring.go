package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ocicorebackend "github.com/blackbirdworks/gopherstack/services/ocicore"
	ocimysqlbackend "github.com/blackbirdworks/gopherstack/services/ocimysql"
)

// ociMySQLNetwork adapts the OCI Core backend to the OCI MySQL Network interface.
type ociMySQLNetwork struct {
	core *ocicorebackend.InMemoryBackend
}

func (n ociMySQLNetwork) Subnet(id string) (ocimysqlbackend.SubnetInfo, bool) {
	s, ok := n.core.LookupSubnet(id)

	return ocimysqlbackend.SubnetInfo{
		ID: s.ID, VcnID: s.VcnID, AvailabilityDomain: s.AvailabilityDomain, CIDR: s.CidrBlock,
	}, ok
}

// wireOCIMySQL lets OCI MySQL validate subnets and allocate endpoint addresses against OCI Core.
func wireOCIMySQL(byName map[string]service.Registerable) {
	mysqlH, ok := byName["OCIMySQL"].(*ocimysqlbackend.Handler)
	if !ok {
		return
	}

	if coreH, found := byName["OCICore"].(*ocicorebackend.Handler); found {
		mysqlH.Backend.SetNetwork(ociMySQLNetwork{core: coreH.Backend})
	}
}
