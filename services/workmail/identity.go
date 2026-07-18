package workmail

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// --- Identity Center Applications ---

// CreateIdentityCenterApplication creates a new IAM Identity Center application.
func (b *InMemoryBackend) CreateIdentityCenterApplication(
	instanceARN, name string, //nolint:revive // existing issue.
) (string, error) {
	b.mu.Lock("CreateIdentityCenterApplication")
	defer b.mu.Unlock()

	appARN := arn.Build("sso", b.region, b.accountID, "application/"+newID())
	b.identityCenterApps[appARN] = name

	return appARN, nil
}

// DeleteIdentityCenterApplication removes an IAM Identity Center application.
func (b *InMemoryBackend) DeleteIdentityCenterApplication(applicationARN string) error {
	b.mu.Lock("DeleteIdentityCenterApplication")
	defer b.mu.Unlock()

	if _, ok := b.identityCenterApps[applicationARN]; !ok {
		return fmt.Errorf(
			"%w: identity center application %q not found",
			ErrNotFound,
			applicationARN,
		)
	}
	delete(b.identityCenterApps, applicationARN)

	return nil
}

// --- Identity Provider Configuration ---

// PutIdentityProviderConfiguration creates or updates IdP configuration.
func (b *InMemoryBackend) PutIdentityProviderConfiguration(
	orgID, authMode, identityCenterAppARN, identityCenterInstanceARN, patStatus string,
	patLifetimeDays int32,
) error {
	b.mu.Lock("PutIdentityProviderConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	cfg := &IdentityProviderConfiguration{
		AuthMode:                  authMode,
		IdentityCenterAppARN:      identityCenterAppARN,
		IdentityCenterInstanceARN: identityCenterInstanceARN,
		PATStatus:                 patStatus,
		orgID:                     orgID,
	}
	if patLifetimeDays > 0 {
		cfg.PATLifetimeDays = &patLifetimeDays
	}
	b.idpConfig.Put(cfg)

	return nil
}

// DeleteIdentityProviderConfiguration removes IdP configuration.
func (b *InMemoryBackend) DeleteIdentityProviderConfiguration(orgID string) error {
	b.mu.Lock("DeleteIdentityProviderConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	b.idpConfig.Delete(orgID)

	return nil
}

// DescribeIdentityProviderConfiguration returns IdP configuration for an org.
func (b *InMemoryBackend) DescribeIdentityProviderConfiguration(
	orgID string,
) (*IdentityProviderConfiguration, error) {
	b.mu.RLock("DescribeIdentityProviderConfiguration")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	cfg, ok := b.idpConfig.Get(orgID)
	if !ok {
		return nil, fmt.Errorf("%w: identity provider configuration not found", ErrNotFound)
	}

	return cfg, nil
}
