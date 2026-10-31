package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ociartifacts"
	ociidentitybackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
	ociobjectstorage "github.com/blackbirdworks/gopherstack/services/ociobjectstorage"
)

// GetOCIArtifactsSettings returns OCI Artifacts settings (ociartifacts.ConfigProvider).
func (c *CLI) GetOCIArtifactsSettings() ociartifacts.Settings { return c.OCIArtifacts }

// wireOCIArtifacts shares OCI Identity keys, compartments and the tenancy root, and the Object Storage
// namespace used in registry paths.
func wireOCIArtifacts(byName map[string]service.Registerable) {
	artH, ok := byName["OCIArtifacts"].(*ociartifacts.Handler)
	if !ok {
		return
	}

	if identityH, found := byName["OCIIdentity"].(*ociidentitybackend.Handler); found {
		artH.Backend.Auth.Link(identityH.Backend.Auth)
		artH.Backend.SetCompartmentChecker(identityH.Backend.CompartmentActive)
		artH.Backend.SetCompartmentTree(identityH.Backend.CompartmentWithin)
		artH.Backend.SetTenancy(ociidentitybackend.TenancyID)
	}

	if osH, found := byName["OCIObjectStorage"].(*ociobjectstorage.Handler); found {
		artH.Backend.SetNamespace(osH.Backend.Namespace())
	}
}
