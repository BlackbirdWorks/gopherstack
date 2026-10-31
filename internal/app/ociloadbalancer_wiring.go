package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ocicorebackend "github.com/blackbirdworks/gopherstack/services/ocicore"
	ociidentitybackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
	ociloadbalancer "github.com/blackbirdworks/gopherstack/services/ociloadbalancer"
)

// GetOCILoadBalancerSettings returns OCI Load Balancer settings (ociloadbalancer.ConfigProvider).
func (c *CLI) GetOCILoadBalancerSettings() ociloadbalancer.Settings { return c.OCILoadBalancer }

// wireOCILoadBalancer shares OCI Identity keys and compartments and resolves subnets through OCI Core.
func wireOCILoadBalancer(byName map[string]service.Registerable) {
	lbH, ok := byName["OCILoadBalancer"].(*ociloadbalancer.Handler)
	if !ok {
		return
	}

	if identityH, found := byName["OCIIdentity"].(*ociidentitybackend.Handler); found {
		lbH.Backend.Auth.Link(identityH.Backend.Auth)
		lbH.Backend.SetCompartmentChecker(identityH.Backend.CompartmentActive)
	}

	if coreH, found := byName["OCICore"].(*ocicorebackend.Handler); found {
		lbH.Backend.SetSubnetResolver(func(id string) (string, bool) {
			ref, live := coreH.Backend.LookupSubnet(id)

			return ref.CidrBlock, live
		})
	}
}
