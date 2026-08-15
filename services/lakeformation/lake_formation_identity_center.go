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
	serviceIntegrations []ServiceIntegration,
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
		CatalogID:           catalogID,
		InstanceArn:         instanceArn,
		ApplicationArn:      appArn,
		ExternalFiltering:   externalFiltering,
		ShareRecipients:     shareRecipients,
		ServiceIntegrations: serviceIntegrations,
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
// shareRecipients/serviceIntegrations use a nil-vs-non-nil-empty-slice
// distinction to match the real API's "unspecified leaves it unchanged,
// explicit empty list clears it" semantics (encoding/json already
// distinguishes an omitted/null JSON field, which unmarshals to a nil Go
// slice, from an explicit "[]", which unmarshals to a non-nil empty slice).
func (b *InMemoryBackend) UpdateLakeFormationIdentityCenterConfiguration(
	catalogID string, externalFiltering *ExternalFilteringConfiguration, appStatus string,
	shareRecipients []DataLakePrincipal, serviceIntegrations []ServiceIntegration,
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
			CatalogID:           catalogID,
			ExternalFiltering:   externalFiltering,
			ApplicationStatus:   appStatus,
			ShareRecipients:     shareRecipients,
			ServiceIntegrations: serviceIntegrations,
		})

		return nil
	}
	if externalFiltering != nil {
		cfg.ExternalFiltering = externalFiltering
	}
	if appStatus != "" {
		cfg.ApplicationStatus = appStatus
	}
	if shareRecipients != nil {
		cfg.ShareRecipients = shareRecipients
	}
	if serviceIntegrations != nil {
		cfg.ServiceIntegrations = serviceIntegrations
	}

	return nil
}
