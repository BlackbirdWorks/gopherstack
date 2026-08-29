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
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
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
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	if !b.availabilityConfigs.Delete(orgKey(orgID, domainName)) {
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
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	cfg, ok := b.availabilityConfigs.Get(orgKey(orgID, domainName))
	if !ok {
		return fmt.Errorf(
			"%w: availability configuration for %q not found",
			ErrNotFound,
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
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	byOrg := b.availabilityConfigsByOrg.Get(orgID)
	cfgs := make([]*AvailabilityConfiguration, 0, len(byOrg))
	cfgs = append(cfgs, byOrg...)
	sort.Slice(cfgs, func(i, j int) bool { return cfgs[i].DomainName < cfgs[j].DomainName })
	page, next := paginate(cfgs, maxResults, nextToken)

	return page, next, nil
}

// TestAvailabilityConfiguration simulates testing a configuration.
func (b *InMemoryBackend) TestAvailabilityConfiguration(
	orgID, domainName string,
) (bool, string, error) {
	b.mu.RLock("TestAvailabilityConfiguration")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return false, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	if domainName == "" {
		return false, "", fmt.Errorf("%w: domainName is required", ErrValidation)
	}

	cfg, ok := b.availabilityConfigs.Get(orgKey(orgID, domainName))
	if !ok {
		return false, "", fmt.Errorf(
			"%w: availability configuration for %q not found",
			ErrNotFound,
			domainName,
		)
	}

	switch cfg.ProviderType {
	case providerEWS:
		if cfg.EwsEndpoint == "" {
			return false, "EwsEndpoint is required", nil
		}
		parsed, err := url.Parse(cfg.EwsEndpoint)
		if err != nil || parsed.Hostname() == "" {
			return false, fmt.Sprintf("invalid EwsEndpoint: %v", err), nil
		}
		if parsed.Scheme != "https" && parsed.Scheme != "http" {
			return false, "EwsEndpoint must use http or https scheme", nil
		}
		if cfg.EwsUsername == "" {
			return false, "EwsUsername is required", nil
		}
	case providerLambda:
		if cfg.LambdaARN == "" {
			return false, "LambdaArn is required", nil
		}
		if !strings.HasPrefix(cfg.LambdaARN, "arn:") {
			return false, fmt.Sprintf("invalid LambdaArn %q: must begin with arn:", cfg.LambdaARN), nil
		}
	}

	return true, "", nil
}
