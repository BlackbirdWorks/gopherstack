package redshift

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// ---------------------------------------------------------------------------
// UpdateLakehouseConfiguration
// ---------------------------------------------------------------------------

// UpdateLakehouseConfigurationSL applies p to namespaceName's lakehouse/Glue
// Data Catalog federation config. CatalogArn is derived from the client-
// supplied CatalogName the same way every other resource ARN in this backend
// is built (arn.Build against b.region/b.accountID) -- the "catalog/<name>"
// resource form matches Glue's own named-catalog ARN shape for federated
// catalogs. If p.DryRun is true and the namespace exists, no state is
// mutated and ErrServerlessDryRun is returned (real DryRunException,
// confirmed in service-2.json: "the request was successful, but dry run was
// enabled so no action was taken").
func (b *InMemoryBackend) UpdateLakehouseConfigurationSL(
	p UpdateLakehouseConfigParams,
) (*LakehouseConfigResult, error) {
	b.mu.Lock("UpdateLakehouseConfiguration")
	defer b.mu.Unlock()

	ns, ok := b.slNamespaces.Get(p.NamespaceName)
	if !ok {
		return nil, fmt.Errorf("%w: namespace %q not found", ErrNamespaceNotFound, p.NamespaceName)
	}

	catalogName := p.CatalogName
	idcArn := p.LakehouseIdcApplicationArn
	regStatus := ns.LakehouseRegistrationStatus

	if existing, exists := b.slLakehouseConfig.Get(p.NamespaceName); exists {
		if catalogName == "" {
			catalogName = existing.CatalogName
		}

		if p.LakehouseIdcRegistration == "" && idcArn == "" {
			idcArn = existing.LakehouseIdcApplicationArn
		}
	}

	switch p.LakehouseIdcRegistration {
	case lakehouseIdcAssociate:
		// idcArn already carries the request's LakehouseIdcApplicationArn.
	case lakehouseIdcDisassociate:
		idcArn = ""
	}

	switch p.LakehouseRegistration {
	case lakehouseRegister:
		regStatus = slLakehouseRegistered
	case lakehouseDeregister:
		regStatus = slLakehouseDeregistered
	}

	var catalogArn string
	if catalogName != "" {
		catalogArn = arn.Build("glue", b.region, b.accountID, "catalog/"+catalogName)
	}

	if p.DryRun {
		return nil, ErrServerlessDryRun
	}

	ns.CatalogArn = catalogArn
	ns.LakehouseRegistrationStatus = regStatus

	b.slLakehouseConfig.Put(&ServerlessLakehouseConfig{
		NamespaceName:              p.NamespaceName,
		CatalogName:                catalogName,
		LakehouseIdcApplicationArn: idcArn,
	})

	return &LakehouseConfigResult{
		NamespaceName:               p.NamespaceName,
		CatalogArn:                  catalogArn,
		LakehouseIdcApplicationArn:  idcArn,
		LakehouseRegistrationStatus: regStatus,
	}, nil
}
