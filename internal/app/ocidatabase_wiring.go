package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ocicorebackend "github.com/blackbirdworks/gopherstack/services/ocicore"
	"github.com/blackbirdworks/gopherstack/services/ocidatabase"
	ociidentitybackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
)

// GetOCIDatabaseSettings returns OCI Database settings (ocidatabase.ConfigProvider).
func (c *CLI) GetOCIDatabaseSettings() ocidatabase.Settings { return c.OCIDatabase }

// wireOCIDatabase shares OCI Identity keys and compartments and checks private endpoint subnets in OCI Core.
func wireOCIDatabase(byName map[string]service.Registerable) {
	h, ok := byName["OCIDatabase"].(*ocidatabase.Handler)
	if !ok {
		return
	}

	if identityH, found := byName["OCIIdentity"].(*ociidentitybackend.Handler); found {
		h.Backend.Auth.Link(identityH.Backend.Auth)
		h.Backend.SetCompartmentChecker(identityH.Backend.CompartmentActive)
	}

	if coreH, found := byName["OCICore"].(*ocicorebackend.Handler); found {
		h.Backend.SetSubnetResolver(func(id string) (ocidatabase.SubnetInfo, bool) {
			ref, live := coreH.Backend.LookupSubnet(id)

			return ocidatabase.SubnetInfo{CIDR: ref.CidrBlock}, live
		})
	}
}

// GetOCIDatabaseHandler returns the OCIDatabase handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetOCIDatabaseHandler() service.Registerable { return c.ociDatabaseHandler }
