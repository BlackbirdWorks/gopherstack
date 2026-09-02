package workmail

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"
)

// --- Availability Configurations ---

// CreateAvailabilityConfiguration creates an availability configuration for a domain.
func (b *InMemoryBackend) CreateAvailabilityConfiguration(
	orgID, domainName string, ewsProvider *AvailabilityEwsProvider, lambdaARN string,
) (*AvailabilityConfiguration, error) {
	b.mu.Lock("CreateAvailabilityConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrOrganizationNotFound, orgID)
	}
	if b.availabilityConfigs.Has(orgKey(orgID, domainName)) {
		return nil, fmt.Errorf(
			"%w: availability configuration for %q already exists",
			ErrNameUnavailable,
			domainName,
		)
	}
	now := time.Now()
	cfg := &AvailabilityConfiguration{
		DateCreated:  now,
		DateModified: now,
		DomainName:   domainName,
		orgID:        orgID,
	}
	if ewsProvider != nil {
		cfg.ProviderType = providerEWS
		cfg.EwsEndpoint = ewsProvider.EwsEndpoint
		cfg.EwsUsername = ewsProvider.EwsUsername
	} else {
		cfg.ProviderType = providerLambda
		cfg.LambdaARN = lambdaARN
	}
	b.availabilityConfigs.Put(cfg)

	return cfg, nil
}

// DeleteAvailabilityConfiguration deletes an availability configuration.
func (b *InMemoryBackend) DeleteAvailabilityConfiguration(orgID, domainName string) error {
	b.mu.Lock("DeleteAvailabilityConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrOrganizationNotFound, orgID)
	}
	if !b.availabilityConfigs.Delete(orgKey(orgID, domainName)) {
		// DeleteAvailabilityConfiguration's own error model declares no
		// not-found type for the configuration itself (only Organization*);
		// no correct code exists to send here (gopherstack-6flj/uox6 sweep).
		return fmt.Errorf(
			"%w: availability configuration for %q not found",
			ErrNotFound,
			domainName,
		)
	}

	return nil
}

// UpdateAvailabilityConfiguration updates an existing availability configuration.
func (b *InMemoryBackend) UpdateAvailabilityConfiguration(
	orgID, domainName string, ewsProvider *AvailabilityEwsProvider, lambdaARN string,
) error {
	b.mu.Lock("UpdateAvailabilityConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrOrganizationNotFound, orgID)
	}
	cfg, ok := b.availabilityConfigs.Get(orgKey(orgID, domainName))
	if !ok {
		return fmt.Errorf(
			"%w: availability configuration for %q not found",
			ErrResourceNotFound,
			domainName,
		)
	}
	cfg.DateModified = time.Now()
	if ewsProvider != nil {
		cfg.ProviderType = providerEWS
		cfg.EwsEndpoint = ewsProvider.EwsEndpoint
		cfg.EwsUsername = ewsProvider.EwsUsername
		cfg.LambdaARN = ""
	} else {
		cfg.ProviderType = providerLambda
		cfg.LambdaARN = lambdaARN
		cfg.EwsEndpoint = ""
		cfg.EwsUsername = ""
	}

	return nil
}

// ListAvailabilityConfigurations lists availability configurations for an org.
func (b *InMemoryBackend) ListAvailabilityConfigurations(
	orgID string, maxResults int32, nextToken string,
) ([]*AvailabilityConfiguration, string, error) {
	b.mu.RLock("ListAvailabilityConfigurations")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrOrganizationNotFound, orgID)
	}
	byOrg := b.availabilityConfigsByOrg.Get(orgID)
	cfgs := make([]*AvailabilityConfiguration, 0, len(byOrg))
	cfgs = append(cfgs, byOrg...)
	sort.Slice(cfgs, func(i, j int) bool { return cfgs[i].DomainName < cfgs[j].DomainName })
	page, next := paginate(cfgs, maxResults, nextToken)

	return page, next, nil
}

// testEwsProvider validates an EWS provider's endpoint and username, shared
// by the inline-provider and stored-config paths below.
func testEwsProvider(endpoint, username string) (bool, string) {
	if endpoint == "" {
		return false, "EwsEndpoint is required"
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Hostname() == "" {
		return false, fmt.Sprintf("invalid EwsEndpoint: %v", err)
	}
	if parsed.Scheme != "https" && parsed.Scheme != "http" {
		return false, "EwsEndpoint must use http or https scheme"
	}
	if username == "" {
		return false, "EwsUsername is required"
	}

	return true, ""
}

// testLambdaProvider validates a Lambda provider's ARN, shared by the
// inline-provider and stored-config paths below.
func testLambdaProvider(arn string) (bool, string) {
	if arn == "" {
		return false, "LambdaArn is required"
	}
	if !strings.HasPrefix(arn, "arn:") {
		return false, fmt.Sprintf("invalid LambdaArn %q: must begin with arn:", arn)
	}

	return true, ""
}

// TestAvailabilityConfiguration simulates testing a configuration. "The
// request must contain either one provider definition (EwsProvider or
// LambdaProvider) or the DomainName parameter. If the DomainName parameter
// is provided, the configuration stored under the DomainName will be
// tested." (api_op_TestAvailabilityConfiguration.go) -- an inline provider
// tests those credentials directly, without requiring a prior
// CreateAvailabilityConfiguration call.
func (b *InMemoryBackend) TestAvailabilityConfiguration(
	orgID, domainName string, ewsProvider *AvailabilityEwsProvider, lambdaARN string,
) (bool, string, error) {
	b.mu.RLock("TestAvailabilityConfiguration")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return false, "", fmt.Errorf("%w: organization %q not found", ErrOrganizationNotFound, orgID)
	}

	switch {
	case ewsProvider != nil:
		passed, reason := testEwsProvider(ewsProvider.EwsEndpoint, ewsProvider.EwsUsername)

		return passed, reason, nil
	case lambdaARN != "":
		passed, reason := testLambdaProvider(lambdaARN)

		return passed, reason, nil
	}

	if domainName == "" {
		return false, "", fmt.Errorf("%w: domainName is required", ErrValidation)
	}

	cfg, ok := b.availabilityConfigs.Get(orgKey(orgID, domainName))
	if !ok {
		return false, "", fmt.Errorf(
			"%w: availability configuration for %q not found",
			ErrResourceNotFound,
			domainName,
		)
	}

	switch cfg.ProviderType {
	case providerEWS:
		passed, reason := testEwsProvider(cfg.EwsEndpoint, cfg.EwsUsername)

		return passed, reason, nil
	case providerLambda:
		passed, reason := testLambdaProvider(cfg.LambdaARN)

		return passed, reason, nil
	}

	return true, "", nil
}
