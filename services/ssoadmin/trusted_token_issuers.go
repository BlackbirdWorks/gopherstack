package ssoadmin

import (
	"fmt"
	"maps"
	"net/url"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// validateTrustedTokenIssuerType rejects non-OIDC_JWT types.
func validateTrustedTokenIssuerType(issuerType string) error {
	if issuerType != "" && issuerType != ttiTypeOIDCJWT {
		return fmt.Errorf("%w: TrustedTokenIssuerType must be OIDC_JWT", awserr.ErrInvalidParameter)
	}

	return nil
}

// validateOIDCJWTConfig validates OIDC JWT trusted token issuer configuration when provided.
func validateOIDCJWTConfig(cfg *OidcJwtConfiguration) error {
	if cfg == nil {
		return nil
	}
	if cfg.IssuerURL == "" {
		return fmt.Errorf("%w: OidcJwtConfiguration.IssuerUrl is required", awserr.ErrInvalidParameter)
	}
	if len(cfg.IssuerURL) > maxIssuerURLLen {
		return fmt.Errorf("%w: OidcJwtConfiguration.IssuerUrl exceeds maximum length of %d",
			awserr.ErrInvalidParameter, maxIssuerURLLen)
	}
	if _, err := url.ParseRequestURI(cfg.IssuerURL); err != nil {
		return fmt.Errorf("%w: OidcJwtConfiguration.IssuerUrl must be a valid URL", awserr.ErrInvalidParameter)
	}
	if cfg.JwksRetrievalOption != "" && cfg.JwksRetrievalOption != jwksRetrievalOpenIDDiscovery {
		return fmt.Errorf("%w: OidcJwtConfiguration.JwksRetrievalOption must be OPEN_ID_DISCOVERY",
			awserr.ErrInvalidParameter)
	}

	return nil
}

// copyTrustedTokenIssuer returns a deep copy of a TrustedTokenIssuer. Must be called with mu held.
func copyTrustedTokenIssuer(tti *TrustedTokenIssuer) *TrustedTokenIssuer {
	cp := *tti
	cp.Tags = make(map[string]string, len(tti.Tags))
	maps.Copy(cp.Tags, tti.Tags)
	if tti.TrustedTokenIssuerConfiguration != nil {
		cfgCopy := *tti.TrustedTokenIssuerConfiguration
		if tti.TrustedTokenIssuerConfiguration.OidcJwtConfiguration != nil {
			oidcCopy := *tti.TrustedTokenIssuerConfiguration.OidcJwtConfiguration
			cfgCopy.OidcJwtConfiguration = &oidcCopy
		}
		cp.TrustedTokenIssuerConfiguration = &cfgCopy
	}

	return &cp
}

// CreateTrustedTokenIssuer creates a trusted token issuer within an SSO instance.
func (b *InMemoryBackend) CreateTrustedTokenIssuer(
	instanceArn, name, issuerType string,
	tags map[string]string,
	cfg *TrustedTokenIssuerConfiguration,
) (*TrustedTokenIssuer, error) {
	b.mu.Lock("CreateTrustedTokenIssuer")
	defer b.mu.Unlock()

	// Default to OIDC_JWT if not specified.
	if issuerType == "" {
		issuerType = ttiTypeOIDCJWT
	}
	if err := validateTrustedTokenIssuerType(issuerType); err != nil {
		return nil, err
	}
	var oidcCfg *OidcJwtConfiguration
	if cfg != nil {
		oidcCfg = cfg.OidcJwtConfiguration
	}
	if err := validateOIDCJWTConfig(oidcCfg); err != nil {
		return nil, err
	}

	if !b.instances.Has(instanceArn) {
		return nil, ErrInstanceNotFound
	}
	for _, ti := range b.trustedTokenIssuersByInstance.Get(instanceArn) {
		if ti.Name == name {
			return nil, ErrTrustedTokenIssuerAlreadyExists
		}
	}

	if tags != nil {
		if err := validateTags(tags); err != nil {
			return nil, err
		}
	}

	id := uuid.NewString()[:uuidShortLen]
	instanceID := instanceARNToID(instanceArn)
	arnStr := arn.Build("sso", "", b.accountID, fmt.Sprintf("trustedTokenIssuer/%s/tti-%s", instanceID, id))
	ti := &TrustedTokenIssuer{
		TrustedTokenIssuerArn:           arnStr,
		InstanceArn:                     instanceArn,
		Name:                            name,
		TrustedTokenIssuerType:          issuerType,
		Tags:                            make(map[string]string),
		TrustedTokenIssuerConfiguration: cfg,
	}
	if tags != nil {
		maps.Copy(ti.Tags, tags)
	}
	b.trustedTokenIssuers.Put(ti)

	return copyTrustedTokenIssuer(ti), nil
}

// DeleteTrustedTokenIssuer deletes a trusted token issuer.
func (b *InMemoryBackend) DeleteTrustedTokenIssuer(trustedTokenIssuerArn string) error {
	b.mu.Lock("DeleteTrustedTokenIssuer")
	defer b.mu.Unlock()

	if !b.trustedTokenIssuers.Has(trustedTokenIssuerArn) {
		return ErrTrustedTokenIssuerNotFound
	}
	b.trustedTokenIssuers.Delete(trustedTokenIssuerArn)

	return nil
}

// DescribeTrustedTokenIssuer returns a trusted token issuer.
func (b *InMemoryBackend) DescribeTrustedTokenIssuer(
	trustedTokenIssuerArn string,
) (*TrustedTokenIssuer, error) {
	b.mu.RLock("DescribeTrustedTokenIssuer")
	defer b.mu.RUnlock()

	issuer, ok := b.trustedTokenIssuers.Get(trustedTokenIssuerArn)
	if !ok {
		return nil, ErrTrustedTokenIssuerNotFound
	}

	return copyTrustedTokenIssuer(issuer), nil
}

// ListTrustedTokenIssuers lists trusted token issuers for an instance.
func (b *InMemoryBackend) ListTrustedTokenIssuers(instanceArn string) []*TrustedTokenIssuer {
	b.mu.RLock("ListTrustedTokenIssuers")
	defer b.mu.RUnlock()

	if instanceArn != "" {
		grouped := b.trustedTokenIssuersByInstance.Get(instanceArn)
		result := make([]*TrustedTokenIssuer, 0, len(grouped))
		for _, issuer := range grouped {
			result = append(result, copyTrustedTokenIssuer(issuer))
		}

		return result
	}

	result := make([]*TrustedTokenIssuer, 0, b.trustedTokenIssuers.Len())
	for _, issuer := range b.trustedTokenIssuers.All() {
		result = append(result, copyTrustedTokenIssuer(issuer))
	}

	return result
}

// UpdateTrustedTokenIssuer updates mutable trusted token issuer fields.
func (b *InMemoryBackend) UpdateTrustedTokenIssuer(
	trustedTokenIssuerArn,
	name,
	issuerType string,
	cfg *TrustedTokenIssuerConfiguration,
) (*TrustedTokenIssuer, error) {
	b.mu.Lock("UpdateTrustedTokenIssuer")
	defer b.mu.Unlock()

	if issuerType != "" {
		if err := validateTrustedTokenIssuerType(issuerType); err != nil {
			return nil, err
		}
	}
	if cfg != nil {
		if err := validateOIDCJWTConfig(cfg.OidcJwtConfiguration); err != nil {
			return nil, err
		}
	}

	issuer, ok := b.trustedTokenIssuers.Get(trustedTokenIssuerArn)
	if !ok {
		return nil, ErrTrustedTokenIssuerNotFound
	}
	if name != "" {
		issuer.Name = name
	}
	if issuerType != "" {
		issuer.TrustedTokenIssuerType = issuerType
	}
	if cfg != nil {
		issuer.TrustedTokenIssuerConfiguration = cfg
	}

	return copyTrustedTokenIssuer(issuer), nil
}
