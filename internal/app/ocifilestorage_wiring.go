package app

import (
	"encoding/base64"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ocicorebackend "github.com/blackbirdworks/gopherstack/services/ocicore"
	"github.com/blackbirdworks/gopherstack/services/ocifilestorage"
	ociidentitybackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
	ocivaultbackend "github.com/blackbirdworks/gopherstack/services/ocivault"
)

// GetOCIFileStorageSettings returns OCI File Storage settings (ocifilestorage.ConfigProvider).
func (c *CLI) GetOCIFileStorageSettings() ocifilestorage.Settings { return c.OCIFileStorage }

// GetOCIFileStorageEngine returns the OCI File Storage engine mode (ocifilestorage.EngineConfigProvider). The NFS
// engine needs no container runtime, so the global --engines switch does not turn it on.
func (c *CLI) GetOCIFileStorageEngine() string { return c.OCIFileStorage.Engine }

// GetOCIFileStorageHandler returns the OCIFileStorage handler.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetOCIFileStorageHandler() service.Registerable { return c.ociFileStorageHandler }

// wireOCIFileStorage shares OCI Identity keys and compartments, backs mount targets with OCI Core VNICs and reads
// Kerberos keytabs and LDAP passwords from OCI Vault secrets.
func wireOCIFileStorage(byName map[string]service.Registerable) {
	h, ok := byName["OCIFileStorage"].(*ocifilestorage.Handler)
	if !ok {
		return
	}

	if identityH, found := byName["OCIIdentity"].(*ociidentitybackend.Handler); found {
		h.Backend.Auth.Link(identityH.Backend.Auth)
		h.Backend.SetCompartmentChecker(identityH.Backend.CompartmentActive)
	}

	if coreH, found := byName["OCICore"].(*ocicorebackend.Handler); found {
		h.Backend.SetNetwork(ocifilestorage.Network{
			Subnet: func(id string) (ocifilestorage.SubnetInfo, bool) {
				ref, live := coreH.Backend.LookupSubnet(id)

				return ocifilestorage.SubnetInfo{
					VcnID: ref.VcnID, CIDR: ref.CidrBlock, AD: ref.AvailabilityDomain, CompartmentID: ref.CompartmentID,
				}, live
			},
			CreateNic: func(r ocifilestorage.NicRequest) (ocifilestorage.Nic, error) {
				nic, err := coreH.Backend.CreateServiceNic(ocicorebackend.ServiceNicIn{
					CompartmentID: r.CompartmentID, AD: r.AD, SubnetID: r.SubnetID, DisplayName: r.DisplayName,
					HostnameLabel: r.Hostname, PrivateIP: r.IPAddress, NsgIDs: r.NsgIDs,
				})
				if err != nil {
					return ocifilestorage.Nic{}, err
				}

				return ocifilestorage.Nic{
					VnicID:      nic.VnicID,
					PrivateIPID: nic.PrivateIPID,
					IPAddress:   nic.IPAddress,
				}, nil
			},
			DeleteNic: coreH.Backend.DeleteServiceNic,
		})
	}

	if vaultH, found := byName["OCIVault"].(*ocivaultbackend.Handler); found {
		h.Backend.SetSecretLookup(func(id string, version *int) ([]byte, bool) {
			sel := ocivaultbackend.BundleSelector{Stage: "CURRENT"}

			if version != nil {
				v := int64(*version)
				sel = ocivaultbackend.BundleSelector{VersionNumber: &v}
			}

			bundle, _, err := vaultH.Backend.GetSecretBundle(id, sel)
			if err != nil || bundle.Content == nil {
				return nil, false
			}

			raw, derr := base64.StdEncoding.DecodeString(bundle.Content.Content)

			return raw, derr == nil
		})
	}
}
