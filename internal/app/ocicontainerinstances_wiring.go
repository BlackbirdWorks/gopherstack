package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ocicontainerinstances "github.com/blackbirdworks/gopherstack/services/ocicontainerinstances"
	ocicorebackend "github.com/blackbirdworks/gopherstack/services/ocicore"
	ociidentitybackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
	ocivaultbackend "github.com/blackbirdworks/gopherstack/services/ocivault"
)

// GetOCIContainerInstancesSettings returns OCI Container Instances settings (ocicontainerinstances.ConfigProvider).
func (c *CLI) GetOCIContainerInstancesSettings() ocicontainerinstances.Settings {
	return c.OCIContainerInstances
}

// wireOCIContainerInstances shares OCI Identity keys and compartments, checks VNIC subnets in OCI Core and reads
// VAULT image pull secrets from OCI Vault.
func wireOCIContainerInstances(byName map[string]service.Registerable) {
	h, ok := byName["OCIContainerInstances"].(*ocicontainerinstances.Handler)
	if !ok {
		return
	}

	if identityH, found := byName["OCIIdentity"].(*ociidentitybackend.Handler); found {
		h.Backend.Auth.Link(identityH.Backend.Auth)
		h.Backend.SetCompartmentChecker(identityH.Backend.CompartmentActive)
	}

	if coreH, found := byName["OCICore"].(*ocicorebackend.Handler); found {
		h.Backend.SetSubnetChecker(func(id string) bool {
			_, live := coreH.Backend.LookupSubnet(id)

			return live
		})
	}

	if vaultH, found := byName["OCIVault"].(*ocivaultbackend.Handler); found {
		h.Backend.SetSecretReader(vaultH.Backend.SecretContent)
	}
}

// GetOCIContainerInstancesHandler returns the OCIContainerInstances handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetOCIContainerInstancesHandler() service.Registerable {
	return c.ociContainerInstancesHandler
}
