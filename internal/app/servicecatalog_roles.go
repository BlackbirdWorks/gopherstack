package app

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	servicecatalogbackend "github.com/blackbirdworks/gopherstack/services/servicecatalog"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

// serviceCatalogRoles checks launch roles trust servicecatalog.amazonaws.com through the STS emulator.
type serviceCatalogRoles struct{ sts *stsbackend.InMemoryBackend }

func (r serviceCatalogRoles) CheckLaunchRole(_ context.Context, roleARN string) error {
	if err := r.sts.CheckServiceRoleTrust(servicecatalogbackend.ServicePrincipal(), roleARN); err != nil {
		return fmt.Errorf("sts assume launch role: %w", err)
	}

	return nil
}

// wireServiceCatalogRoles makes Service Catalog assume launch roles under --enforce-iam.
func wireServiceCatalogRoles(cli *CLI, byName map[string]service.Registerable) {
	if cli == nil || !cli.EnforceIAM {
		return
	}

	scH, ok := byName["ServiceCatalog"].(*servicecatalogbackend.Handler)
	if !ok {
		return
	}

	if stsH, isSTS := byName["STS"].(*stsbackend.Handler); isSTS {
		if bk, isBk := stsH.Backend.(*stsbackend.InMemoryBackend); isBk {
			scH.Backend.SetRoleChecker(serviceCatalogRoles{sts: bk})
		}
	}
}
