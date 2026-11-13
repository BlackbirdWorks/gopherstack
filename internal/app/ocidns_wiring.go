package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ocicorebackend "github.com/blackbirdworks/gopherstack/services/ocicore"
	"github.com/blackbirdworks/gopherstack/services/ocidns"
	ociidentitybackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
)

// GetOCIDNSSettings returns OCI DNS settings (ocidns.ConfigProvider).
func (c *CLI) GetOCIDNSSettings() ocidns.Settings { return c.OCIDNS }

// wireOCIDNS shares OCI Identity keys and compartments and attaches private resolvers to OCI Core VCNs.
func wireOCIDNS(byName map[string]service.Registerable) {
	dnsH, ok := byName["OCIDNS"].(*ocidns.Handler)
	if !ok {
		return
	}

	if identityH, found := byName["OCIIdentity"].(*ociidentitybackend.Handler); found {
		dnsH.Backend.Auth.Link(identityH.Backend.Auth)
		dnsH.Backend.SetCompartmentChecker(identityH.Backend.CompartmentActive)
		dnsH.Backend.SetTagLookup(identityH.Backend.TagDefinition)
	}

	if coreH, found := byName["OCICore"].(*ocicorebackend.Handler); found {
		dnsH.Backend.SetVcnSource(func() []ocidns.VcnInfo {
			vcns := coreH.Backend.ListVcns()
			out := make([]ocidns.VcnInfo, 0, len(vcns))

			for _, v := range vcns {
				out = append(out, ocidns.VcnInfo{
					ID: v.ID, CompartmentID: v.CompartmentID, DisplayName: v.DisplayName, Created: v.TimeCreated,
				})
			}

			return out
		})
		coreH.Backend.SetDNSResolverLookup(dnsH.Backend.ResolverIDForVcn)
		dnsH.Backend.SetSubnetResolver(func(id string) (ocidns.SubnetInfo, bool) {
			ref, live := coreH.Backend.LookupSubnet(id)

			return ocidns.SubnetInfo{VcnID: ref.VcnID, Cidr: ref.CidrBlock}, live
		})
	}
}

// GetOCIDNSHandler returns the OCIDNS handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetOCIDNSHandler() service.Registerable { return c.ociDNSHandler }
