package lakeformation

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// CreateLakeFormationIdentityCenterConfiguration creates or replaces the IAM Identity Center
// integration for the given catalog and returns a synthetic application ARN.
func (b *InMemoryBackend) CreateLakeFormationIdentityCenterConfiguration(
	catalogID, instanceArn string,
	externalFiltering *ExternalFilteringConfiguration,
	shareRecipients []DataLakePrincipal,
) (string, error) {
	b.mu.Lock("CreateLakeFormationIdentityCenterConfiguration")
	defer b.mu.Unlock()

	if b.identityCenterConfigs.Has(catalogID) {
		return "", awserr.New(
			"identity center configuration already exists for catalog: "+catalogID,
			awserr.ErrAlreadyExists,
		)
	}

	appArn := fmt.Sprintf(
		"arn:aws:sso::%s:application/ssoins-0000000000000000/apl-%s",
		catalogID,
		catalogID,
	)

	b.identityCenterConfigs.Put(&IdentityCenterConfiguration{
		CatalogID:         catalogID,
		InstanceArn:       instanceArn,
		ApplicationArn:    appArn,
		ExternalFiltering: externalFiltering,
		ShareRecipients:   shareRecipients,
	})

	return appArn, nil
}

// DeleteLakeFormationIdentityCenterConfiguration removes the identity center config for a catalog.
func (b *InMemoryBackend) DeleteLakeFormationIdentityCenterConfiguration(catalogID string) error {
	b.mu.Lock("DeleteLakeFormationIdentityCenterConfiguration")
	defer b.mu.Unlock()
	if !b.identityCenterConfigs.Has(catalogID) {
		return awserr.New("identity center configuration not found for catalog: "+catalogID, awserr.ErrNotFound)
	}
	b.identityCenterConfigs.Delete(catalogID)

	return nil
}

// DescribeLakeFormationIdentityCenterConfiguration returns the identity center config for a catalog.
func (b *InMemoryBackend) DescribeLakeFormationIdentityCenterConfiguration(
	catalogID string,
) (*IdentityCenterConfiguration, error) {
	b.mu.RLock("DescribeLakeFormationIdentityCenterConfiguration")
	defer b.mu.RUnlock()
	cfg, ok := b.identityCenterConfigs.Get(catalogID)
	if !ok {
		return nil, awserr.New("identity center configuration not found for catalog: "+catalogID, awserr.ErrNotFound)
	}
	cp := *cfg

	return &cp, nil
}

// UpdateLakeFormationIdentityCenterConfiguration updates or creates the identity center config.
func (b *InMemoryBackend) UpdateLakeFormationIdentityCenterConfiguration(
	catalogID string, externalFiltering *ExternalFilteringConfiguration, appStatus string,
) error {
	// Validate ApplicationStatus if provided.
	if appStatus != "" && appStatus != "ENABLED" && appStatus != "DISABLED" {
		return fmt.Errorf("invalid ApplicationStatus: %s: %w", appStatus, ErrValidation)
	}

	b.mu.Lock("UpdateLakeFormationIdentityCenterConfiguration")
	defer b.mu.Unlock()
	cfg, ok := b.identityCenterConfigs.Get(catalogID)
	if !ok {
		b.identityCenterConfigs.Put(&IdentityCenterConfiguration{
			CatalogID:         catalogID,
			ExternalFiltering: externalFiltering,
			ApplicationStatus: appStatus,
		})

		return nil
	}
	if externalFiltering != nil {
		cfg.ExternalFiltering = externalFiltering
	}
	if appStatus != "" {
		cfg.ApplicationStatus = appStatus
	}

	return nil
}
