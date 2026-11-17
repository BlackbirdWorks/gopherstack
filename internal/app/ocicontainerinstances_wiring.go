package app

import (
	"errors"

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
		h.Backend.SetTagLookup(identityH.Backend.TagDefinition)
	}

	if coreH, found := byName["OCICore"].(*ocicorebackend.Handler); found {
		h.Backend.SetSubnetChecker(func(id string) bool {
			_, live := coreH.Backend.LookupSubnet(id)

			return live
		})
		h.Backend.SetNetwork(ociContainerInstancesNetwork(coreH.Backend))
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

func ociContainerInstancesNetwork(core *ocicorebackend.InMemoryBackend) ocicontainerinstances.Network {
	return ocicontainerinstances.Network{
		Allocate: func(r ocicontainerinstances.VnicRequest) (ocicontainerinstances.VnicAllocation, error) {
			nic, err := core.CreateServiceNic(ocicorebackend.ServiceNicIn{
				CompartmentID: r.CompartmentID, AD: r.AD, SubnetID: r.SubnetID, DisplayName: r.DisplayName,
				HostnameLabel: r.HostnameLabel, PrivateIP: r.PrivateIP, NsgIDs: r.NsgIDs, AssignPublicIP: r.IsPublic,
			})
			if err != nil {
				if ae, ok := errors.AsType[*ocicorebackend.APIError](err); ok {
					return ocicontainerinstances.VnicAllocation{}, &ocicontainerinstances.APIError{
						Status: ae.Status, Code: ae.Code, Message: ae.Message,
					}
				}

				return ocicontainerinstances.VnicAllocation{}, err
			}

			return ocicontainerinstances.VnicAllocation{
				VnicID: nic.VnicID, PrivateIP: nic.IPAddress, PublicIPs: nic.PublicIPs,
			}, nil
		},
		Release: core.DeleteServiceNic,
	}
}
