package iam

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// ---- SAML Provider operations ----

// CreateSAMLProvider creates a new IAM SAML identity provider.
// The provider name is used to build the ARN; it must be unique.
func (b *InMemoryBackend) CreateSAMLProvider(name, samlMetadataDocument string) (*SAMLProvider, error) {
	b.mu.Lock("CreateSAMLProvider")
	defer b.mu.Unlock()

	providerArn := arn.Build("iam", "", b.accountID, "saml-provider/"+name)

	if _, exists := b.samlProviders[providerArn]; exists {
		return nil, fmt.Errorf("%w: SAML provider %q already exists", ErrSAMLProviderAlreadyExists, name)
	}

	p := SAMLProvider{
		Arn:                  providerArn,
		SAMLMetadataDocument: samlMetadataDocument,
		CreateDate:           time.Now().UTC(),
	}
	b.samlProviders[providerArn] = p

	return &p, nil
}

// UpdateSAMLProvider replaces the SAML metadata document for an existing provider.
func (b *InMemoryBackend) UpdateSAMLProvider(providerArn, samlMetadataDocument string) (*SAMLProvider, error) {
	b.mu.Lock("UpdateSAMLProvider")
	defer b.mu.Unlock()

	p, exists := b.samlProviders[providerArn]
	if !exists {
		return nil, fmt.Errorf("%w: SAML provider %q not found", ErrSAMLProviderNotFound, providerArn)
	}

	p.SAMLMetadataDocument = samlMetadataDocument
	b.samlProviders[providerArn] = p

	return &p, nil
}

// DeleteSAMLProvider removes a SAML provider by ARN.
func (b *InMemoryBackend) DeleteSAMLProvider(providerArn string) error {
	b.mu.Lock("DeleteSAMLProvider")
	defer b.mu.Unlock()

	if _, exists := b.samlProviders[providerArn]; !exists {
		return fmt.Errorf("%w: SAML provider %q not found", ErrSAMLProviderNotFound, providerArn)
	}

	delete(b.samlProviders, providerArn)

	return nil
}

// GetSAMLProvider retrieves a SAML provider by ARN.
func (b *InMemoryBackend) GetSAMLProvider(providerArn string) (*SAMLProvider, error) {
	b.mu.RLock("GetSAMLProvider")
	defer b.mu.RUnlock()

	p, exists := b.samlProviders[providerArn]
	if !exists {
		return nil, fmt.Errorf("%w: SAML provider %q not found", ErrSAMLProviderNotFound, providerArn)
	}

	return &p, nil
}

// ListSAMLProviders returns all SAML providers sorted by ARN.
func (b *InMemoryBackend) ListSAMLProviders() ([]SAMLProvider, error) {
	b.mu.RLock("ListSAMLProviders")
	defer b.mu.RUnlock()

	result := make([]SAMLProvider, 0, len(b.samlProviders))
	for _, p := range b.samlProviders {
		result = append(result, p)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].Arn < result[j].Arn })

	return result, nil
}

// ---- OIDC Provider operations ----

// oidcProviderHostFromURL extracts the host portion from an OIDC provider URL.
// For example, "https://token.actions.githubusercontent.com" → "token.actions.githubusercontent.com".
func oidcProviderHostFromURL(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		// Treat a URL without a scheme/host as a bare hostname.
		host := strings.TrimPrefix(rawURL, "https://")
		host = strings.TrimPrefix(host, "http://")
		host = strings.TrimSuffix(host, "/")

		if host == "" {
			return "", fmt.Errorf("%w: %q", ErrInvalidOIDCProviderURL, rawURL)
		}

		return host, nil
	}

	return u.Host, nil
}

// CreateOpenIDConnectProvider creates a new IAM OIDC identity provider.
func (b *InMemoryBackend) CreateOpenIDConnectProvider(
	rawURL string, clientIDs, thumbprints []string,
) (*OIDCProvider, error) {
	b.mu.Lock("CreateOpenIDConnectProvider")
	defer b.mu.Unlock()

	host, err := oidcProviderHostFromURL(rawURL)
	if err != nil {
		return nil, err
	}

	providerArn := arn.Build("iam", "", b.accountID, "oidc-provider/"+host)

	if _, exists := b.oidcProviders[providerArn]; exists {
		return nil, fmt.Errorf("%w: OIDC provider for URL %q already exists", ErrOIDCProviderAlreadyExists, rawURL)
	}

	p := OIDCProvider{
		Arn:            providerArn,
		URL:            rawURL,
		ClientIDList:   append([]string(nil), clientIDs...),
		ThumbprintList: append([]string(nil), thumbprints...),
		CreateDate:     time.Now().UTC(),
	}
	b.oidcProviders[providerArn] = p

	return &p, nil
}

// UpdateOpenIDConnectProviderThumbprint replaces the thumbprint list for an existing OIDC provider.
func (b *InMemoryBackend) UpdateOpenIDConnectProviderThumbprint(providerArn string, thumbprints []string) error {
	b.mu.Lock("UpdateOpenIDConnectProviderThumbprint")
	defer b.mu.Unlock()

	p, exists := b.oidcProviders[providerArn]
	if !exists {
		return fmt.Errorf("%w: OIDC provider %q not found", ErrOIDCProviderNotFound, providerArn)
	}

	p.ThumbprintList = append([]string(nil), thumbprints...)
	b.oidcProviders[providerArn] = p

	return nil
}

// DeleteOpenIDConnectProvider removes an OIDC provider by ARN.
func (b *InMemoryBackend) DeleteOpenIDConnectProvider(providerArn string) error {
	b.mu.Lock("DeleteOpenIDConnectProvider")
	defer b.mu.Unlock()

	if _, exists := b.oidcProviders[providerArn]; !exists {
		return fmt.Errorf("%w: OIDC provider %q not found", ErrOIDCProviderNotFound, providerArn)
	}

	delete(b.oidcProviders, providerArn)

	return nil
}

// GetOpenIDConnectProvider retrieves an OIDC provider by ARN.
func (b *InMemoryBackend) GetOpenIDConnectProvider(providerArn string) (*OIDCProvider, error) {
	b.mu.RLock("GetOpenIDConnectProvider")
	defer b.mu.RUnlock()

	p, exists := b.oidcProviders[providerArn]
	if !exists {
		return nil, fmt.Errorf("%w: OIDC provider %q not found", ErrOIDCProviderNotFound, providerArn)
	}

	return &p, nil
}

// ListOpenIDConnectProviders returns all OIDC providers sorted by ARN.
func (b *InMemoryBackend) ListOpenIDConnectProviders() ([]OIDCProvider, error) {
	b.mu.RLock("ListOpenIDConnectProviders")
	defer b.mu.RUnlock()

	result := make([]OIDCProvider, 0, len(b.oidcProviders))
	for _, p := range b.oidcProviders {
		result = append(result, p)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].Arn < result[j].Arn })

	return result, nil
}

// ---- Login Profile operations ----

// CreateLoginProfile creates a console login profile for an IAM user.
// The password parameter is accepted for API compatibility but not stored;
// this is an in-memory mock and passwords are not persisted.
func (b *InMemoryBackend) CreateLoginProfile(userName, _ /*password*/ string, passwordResetRequired bool) (*LoginProfile, error) {
	b.mu.Lock("CreateLoginProfile")
	defer b.mu.Unlock()

	if _, exists := b.users[userName]; !exists {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	if _, exists := b.loginProfiles[userName]; exists {
		return nil, fmt.Errorf("%w: login profile for user %q already exists", ErrLoginProfileAlreadyExists, userName)
	}

	lp := LoginProfile{
		UserName:              userName,
		CreateDate:            time.Now().UTC(),
		PasswordResetRequired: passwordResetRequired,
	}
	b.loginProfiles[userName] = lp

	return &lp, nil
}

// UpdateLoginProfile updates the console login profile for an IAM user.
// The password parameter is accepted for API compatibility but not stored;
// this is an in-memory mock and passwords are not persisted.
func (b *InMemoryBackend) UpdateLoginProfile(userName, _ /*password*/ string, passwordResetRequired bool) error {
	b.mu.Lock("UpdateLoginProfile")
	defer b.mu.Unlock()

	lp, exists := b.loginProfiles[userName]
	if !exists {
		return fmt.Errorf("%w: login profile for user %q not found", ErrLoginProfileNotFound, userName)
	}

	lp.PasswordResetRequired = passwordResetRequired
	b.loginProfiles[userName] = lp

	return nil
}

// DeleteLoginProfile removes the console login profile for an IAM user.
func (b *InMemoryBackend) DeleteLoginProfile(userName string) error {
	b.mu.Lock("DeleteLoginProfile")
	defer b.mu.Unlock()

	if _, exists := b.loginProfiles[userName]; !exists {
		return fmt.Errorf("%w: login profile for user %q not found", ErrLoginProfileNotFound, userName)
	}

	delete(b.loginProfiles, userName)

	return nil
}

// GetLoginProfile retrieves the console login profile for an IAM user.
func (b *InMemoryBackend) GetLoginProfile(userName string) (*LoginProfile, error) {
	b.mu.RLock("GetLoginProfile")
	defer b.mu.RUnlock()

	lp, exists := b.loginProfiles[userName]
	if !exists {
		return nil, fmt.Errorf("%w: login profile for user %q not found", ErrLoginProfileNotFound, userName)
	}

	return &lp, nil
}
