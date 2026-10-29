package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ociidentitybackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
	ocinosql "github.com/blackbirdworks/gopherstack/services/ocinosql"
)

// GetOCINoSQLSettings returns OCI NoSQL settings (ocinosql.ConfigProvider).
func (c *CLI) GetOCINoSQLSettings() ocinosql.Settings { return c.OCINoSQL }

// wireOCINoSQL shares OCI Identity API keys and compartment checks with OCI NoSQL.
func wireOCINoSQL(byName map[string]service.Registerable) {
	identityH, ok := byName["OCIIdentity"].(*ociidentitybackend.Handler)
	if !ok {
		return
	}

	if h, found := byName["OCINoSQL"].(*ocinosql.Handler); found {
		h.Backend.Auth.Link(identityH.Backend.Auth)
		h.Backend.SetCompartmentChecker(identityH.Backend.CompartmentActive)
	}
}
