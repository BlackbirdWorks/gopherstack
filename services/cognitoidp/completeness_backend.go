package cognitoidp

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"
)

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

// IdentityProvider represents a federated identity provider attached to a user pool.
type IdentityProvider struct {
	ProviderDetails  map[string]string `json:"providerDetails,omitempty"`
	AttributeMapping map[string]string `json:"attributeMapping,omitempty"`
	CreatedAt        time.Time         `json:"createdAt"`
	LastModifiedAt   time.Time         `json:"lastModifiedAt"`
	UserPoolID       string            `json:"userPoolID,omitempty"`
	ProviderName     string            `json:"providerName,omitempty"`
	ProviderType     string            `json:"providerType,omitempty"`
	IdpIdentifiers   []string          `json:"idpIdentifiers,omitempty"`
}

// UserPoolDomain holds the custom domain configuration for a user pool.
type UserPoolDomain struct {
	Domain                 string `json:"domain,omitempty"`
	UserPoolID             string `json:"userPoolID,omitempty"`
	CloudFrontDistribution string `json:"cloudFrontDistribution,omitempty"`
	CertificateArn         string `json:"certificateArn,omitempty"`
	Status                 string `json:"status,omitempty"`
}

// RiskConfiguration stores adaptive authentication settings for a pool or client.
type RiskConfiguration struct {
	// Stored as an opaque blob; individual fields not needed for emulation.
	Raw map[string]any `json:"raw,omitempty"`
}

// LogDeliveryConfig holds log delivery destination configuration for a pool.
type LogDeliveryConfig struct {
	Raw map[string]any `json:"raw,omitempty"`
}

// UICustomization stores hosted-UI CSS and logo settings for a pool or client.
type UICustomization struct {
	CreatedAt      time.Time `json:"createdAt"`
	LastModifiedAt time.Time `json:"lastModifiedAt"`
	UserPoolID     string    `json:"userPoolID,omitempty"`
	ClientID       string    `json:"clientID,omitempty"`
	CSS            string    `json:"css,omitempty"`
	ImageURL       string    `json:"imageURL,omitempty"`
}

// ManagedLoginBranding stores managed login branding for a pool client.
type ManagedLoginBranding struct {
	CreatedAt              time.Time `json:"createdAt"`
	LastModifiedAt         time.Time `json:"lastModifiedAt"`
	ManagedLoginBrandingID string    `json:"managedLoginBrandingID,omitempty"`
	UserPoolID             string    `json:"userPoolID,omitempty"`
	ClientID               string    `json:"clientID,omitempty"`
}

// Terms stores the terms and conditions text for a user pool.
type Terms struct {
	UserPoolID string `json:"userPoolID,omitempty"`
	Text       string `json:"text,omitempty"`
}

// UserImportJob represents a bulk user import job.
type UserImportJob struct {
	CreatedAt  time.Time `json:"createdAt"`
	JobID      string    `json:"jobID,omitempty"`
	JobName    string    `json:"jobName,omitempty"`
	UserPoolID string    `json:"userPoolID,omitempty"`
	Status     string    `json:"status,omitempty"` // Created | Pending | InProgress | Stopping ...
}

// Device tracking status values (DeviceRememberedStatusType).
const (
	DeviceStatusRemembered    = "remembered"
	DeviceStatusNotRemembered = "not_remembered"
)

// Device represents a tracked/remembered device for a user, keyed by DeviceKey.
type Device struct {
	CreatedAt           time.Time         `json:"createdAt"`
	LastModifiedAt      time.Time         `json:"lastModifiedAt"`
	LastAuthenticatedAt time.Time         `json:"lastAuthenticatedAt"`
	Attributes          map[string]string `json:"attributes,omitempty"`
	DeviceKey           string            `json:"deviceKey,omitempty"`
	Status              string            `json:"status,omitempty"`
}

// WebAuthnCredential represents a registered passkey/WebAuthn credential for a user.
type WebAuthnCredential struct {
	CreatedAt               time.Time `json:"createdAt"`
	CredentialID            string    `json:"credentialID,omitempty"`
	FriendlyName            string    `json:"friendlyName,omitempty"`
	RelyingPartyID          string    `json:"relyingPartyID,omitempty"`
	AuthenticatorAttachment string    `json:"authenticatorAttachment,omitempty"`
}

// AuthEvent represents an adaptive-authentication (risk) sign-in event for a
// user, and its feedback state once reviewed via [Admin]UpdateAuthEventFeedback.
type AuthEvent struct {
	CreatedAt     time.Time `json:"createdAt"`
	FeedbackDate  time.Time `json:"feedbackDate"`
	EventID       string    `json:"eventID,omitempty"`
	EventType     string    `json:"eventType,omitempty"`
	EventResponse string    `json:"eventResponse,omitempty"`
	FeedbackValue string    `json:"feedbackValue,omitempty"`
}

// MFAOptionType is the legacy per-user MFA delivery option
// (SetUserSettings/AdminSetUserSettings), e.g. {DeliveryMedium: "SMS", AttributeName: "phone_number"}.
type MFAOptionType struct {
	DeliveryMedium string `json:"deliveryMedium,omitempty"`
	AttributeName  string `json:"attributeName,omitempty"`
}

// ProviderLink records a federated identity linked to a user via AdminLinkProviderForUser.
type ProviderLink struct {
	ProviderName           string `json:"providerName,omitempty"`
	ProviderAttributeName  string `json:"providerAttributeName,omitempty"`
	ProviderAttributeValue string `json:"providerAttributeValue,omitempty"`
}

// userStateKey builds the composite key used by the per-user device, WebAuthn
// credential, and auth-event stores.
func userStateKey(userPoolID, username string) string {
	return userPoolID + ":" + username
}

// ---------------------------------------------------------------------------
// Identity Provider
// ---------------------------------------------------------------------------

// CreateIdentityProvider creates a new identity provider in the given pool.
func (b *InMemoryBackend) CreateIdentityProvider(
	userPoolID, providerName, providerType string,
	providerDetails map[string]string,
) (*IdentityProvider, error) {
	b.mu.Lock("CreateIdentityProvider")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if b.identityProviders[userPoolID] == nil {
		b.identityProviders[userPoolID] = make(map[string]*IdentityProvider)
	}

	if _, exists := b.identityProviders[userPoolID][providerName]; exists {
		return nil, fmt.Errorf("%w: identity provider %q already exists in pool %q",
			ErrAlreadyExists, providerName, userPoolID)
	}

	now := time.Now()
	idp := &IdentityProvider{
		UserPoolID:      userPoolID,
		ProviderName:    providerName,
		ProviderType:    providerType,
		ProviderDetails: maps.Clone(providerDetails),
		CreatedAt:       now,
		LastModifiedAt:  now,
	}
	b.identityProviders[userPoolID][providerName] = idp

	cp := *idp
	cp.ProviderDetails = maps.Clone(idp.ProviderDetails)

	return &cp, nil
}

// DescribeIdentityProvider returns an identity provider by pool and provider name.
func (b *InMemoryBackend) DescribeIdentityProvider(userPoolID, providerName string) (*IdentityProvider, error) {
	b.mu.RLock("DescribeIdentityProvider")
	defer b.mu.RUnlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	idp, ok := b.identityProviders[userPoolID][providerName]
	if !ok {
		return nil, fmt.Errorf("%w: identity provider %q not found in pool %q",
			ErrUserPoolNotFound, providerName, userPoolID)
	}

	cp := *idp
	cp.ProviderDetails = maps.Clone(idp.ProviderDetails)

	return &cp, nil
}

// GetIdentityProviderByIdentifier searches all providers in a pool for the given identifier string.
func (b *InMemoryBackend) GetIdentityProviderByIdentifier(userPoolID, identifier string) (*IdentityProvider, error) {
	b.mu.RLock("GetIdentityProviderByIdentifier")
	defer b.mu.RUnlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	for _, idp := range b.identityProviders[userPoolID] {
		if idp.ProviderName == identifier {
			cp := *idp
			cp.ProviderDetails = maps.Clone(idp.ProviderDetails)

			return &cp, nil
		}

		if slices.Contains(idp.IdpIdentifiers, identifier) {
			cp := *idp
			cp.ProviderDetails = maps.Clone(idp.ProviderDetails)

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: identity provider with identifier %q not found in pool %q",
		ErrUserPoolNotFound, identifier, userPoolID)
}

// ListIdentityProviders returns all identity providers for a pool sorted by name.
func (b *InMemoryBackend) ListIdentityProviders(userPoolID string) ([]*IdentityProvider, error) {
	b.mu.RLock("ListIdentityProviders")
	defer b.mu.RUnlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	poolProviders := b.identityProviders[userPoolID]
	out := make([]*IdentityProvider, 0, len(poolProviders))

	for _, idp := range poolProviders {
		cp := *idp
		cp.ProviderDetails = maps.Clone(idp.ProviderDetails)
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ProviderName < out[j].ProviderName })

	return out, nil
}

// UpdateIdentityProvider updates an existing identity provider's details.
func (b *InMemoryBackend) UpdateIdentityProvider(
	userPoolID, providerName string,
	providerDetails map[string]string,
) (*IdentityProvider, error) {
	b.mu.Lock("UpdateIdentityProvider")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	idp, ok := b.identityProviders[userPoolID][providerName]
	if !ok {
		return nil, fmt.Errorf("%w: identity provider %q not found in pool %q",
			ErrUserPoolNotFound, providerName, userPoolID)
	}

	if providerDetails != nil {
		idp.ProviderDetails = maps.Clone(providerDetails)
	}
	idp.LastModifiedAt = time.Now()

	cp := *idp
	cp.ProviderDetails = maps.Clone(idp.ProviderDetails)

	return &cp, nil
}

// DeleteIdentityProvider removes an identity provider from a pool.
func (b *InMemoryBackend) DeleteIdentityProvider(userPoolID, providerName string) error {
	b.mu.Lock("DeleteIdentityProvider")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.identityProviders[userPoolID][providerName]; !ok {
		return fmt.Errorf("%w: identity provider %q not found in pool %q",
			ErrUserPoolNotFound, providerName, userPoolID)
	}

	delete(b.identityProviders[userPoolID], providerName)

	return nil
}

// ---------------------------------------------------------------------------
// User Pool Domain
// ---------------------------------------------------------------------------

// CreateUserPoolDomain registers a domain for a user pool.
func (b *InMemoryBackend) CreateUserPoolDomain(userPoolID, domain string) (*UserPoolDomain, error) {
	b.mu.Lock("CreateUserPoolDomain")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, exists := b.domains[domain]; exists {
		return nil, fmt.Errorf("%w: domain %q already exists", ErrAlreadyExists, domain)
	}

	d := &UserPoolDomain{
		Domain:                 domain,
		UserPoolID:             userPoolID,
		CloudFrontDistribution: domain + ".auth." + b.region + ".amazoncognito.com",
		Status:                 "ACTIVE",
	}
	b.domains[domain] = d

	cp := *d

	return &cp, nil
}

// DescribeUserPoolDomain returns domain details by domain name.
func (b *InMemoryBackend) DescribeUserPoolDomain(domain string) (*UserPoolDomain, error) {
	b.mu.RLock("DescribeUserPoolDomain")
	defer b.mu.RUnlock()

	d, ok := b.domains[domain]
	if !ok {
		return nil, fmt.Errorf("%w: domain %q not found", ErrUserPoolNotFound, domain)
	}

	cp := *d

	return &cp, nil
}

// FindUserPoolDomain returns a domain by name, or nil if not found (no error).
// Use instead of DescribeUserPoolDomain when the caller treats "not found" as an empty result.
func (b *InMemoryBackend) FindUserPoolDomain(domain string) *UserPoolDomain {
	b.mu.RLock("FindUserPoolDomain")
	defer b.mu.RUnlock()

	d := b.domains[domain]
	if d == nil {
		return nil
	}

	cp := *d

	return &cp
}

// UpdateUserPoolDomain updates a domain (e.g., custom certificate). Returns the cloudfront domain.
func (b *InMemoryBackend) UpdateUserPoolDomain(userPoolID, domain string) (string, error) {
	b.mu.Lock("UpdateUserPoolDomain")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return "", fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	d, ok := b.domains[domain]
	if !ok {
		return "", fmt.Errorf("%w: domain %q not found", ErrUserPoolNotFound, domain)
	}

	return d.CloudFrontDistribution, nil
}

// DeleteUserPoolDomain removes a domain from a user pool.
func (b *InMemoryBackend) DeleteUserPoolDomain(userPoolID, domain string) error {
	b.mu.Lock("DeleteUserPoolDomain")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.domains[domain]; !ok {
		return fmt.Errorf("%w: domain %q not found", ErrUserPoolNotFound, domain)
	}

	delete(b.domains, domain)

	return nil
}

// ---------------------------------------------------------------------------
// Tags
// ---------------------------------------------------------------------------

// TagResource adds or updates tags on a resource identified by ARN.
func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if b.resourceTags[arn] == nil {
		b.resourceTags[arn] = make(map[string]string)
	}

	maps.Copy(b.resourceTags[arn], tags)
}

// UntagResource removes tag keys from a resource identified by ARN.
func (b *InMemoryBackend) UntagResource(arn string, tagKeys []string) {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if b.resourceTags[arn] == nil {
		return
	}

	for _, k := range tagKeys {
		delete(b.resourceTags[arn], k)
	}
}

// ListTagsForResource returns a copy of the tag map for the given ARN.
func (b *InMemoryBackend) ListTagsForResource(arn string) map[string]string {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	return maps.Clone(b.resourceTags[arn])
}

// ---------------------------------------------------------------------------
// Risk Configuration
// ---------------------------------------------------------------------------

// riskKey builds the map key for risk configuration (pool-level if clientID="").
func riskKey(poolID, clientID string) string {
	return poolID + ":" + clientID
}

// SetRiskConfiguration stores a risk configuration blob for a pool (and optional client).
func (b *InMemoryBackend) SetRiskConfiguration(poolID, clientID string, raw map[string]any) error {
	b.mu.Lock("SetRiskConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.pools[poolID]; !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, poolID)
	}

	rawCopy := make(map[string]any, len(raw))
	maps.Copy(rawCopy, raw)
	b.riskConfigurations[riskKey(poolID, clientID)] = &RiskConfiguration{Raw: rawCopy}

	return nil
}

// DescribeRiskConfiguration retrieves the risk configuration for a pool and optional client.
func (b *InMemoryBackend) DescribeRiskConfiguration(poolID, clientID string) (*RiskConfiguration, error) {
	b.mu.RLock("DescribeRiskConfiguration")
	defer b.mu.RUnlock()

	if _, ok := b.pools[poolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, poolID)
	}

	rc := b.riskConfigurations[riskKey(poolID, clientID)]
	if rc == nil {
		return &RiskConfiguration{Raw: map[string]any{}}, nil
	}

	rawCopy := make(map[string]any, len(rc.Raw))
	maps.Copy(rawCopy, rc.Raw)

	return &RiskConfiguration{Raw: rawCopy}, nil
}

// ---------------------------------------------------------------------------
// Log Delivery
// ---------------------------------------------------------------------------

// SetLogDeliveryConfiguration stores the log delivery config for a pool.
func (b *InMemoryBackend) SetLogDeliveryConfiguration(poolID string, raw map[string]any) error {
	b.mu.Lock("SetLogDeliveryConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.pools[poolID]; !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, poolID)
	}

	rawCopy := make(map[string]any, len(raw))
	maps.Copy(rawCopy, raw)
	b.logDeliveryConfigs[poolID] = &LogDeliveryConfig{Raw: rawCopy}

	return nil
}

// GetLogDeliveryConfiguration retrieves the log delivery config for a pool.
func (b *InMemoryBackend) GetLogDeliveryConfiguration(poolID string) (*LogDeliveryConfig, error) {
	b.mu.RLock("GetLogDeliveryConfiguration")
	defer b.mu.RUnlock()

	if _, ok := b.pools[poolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, poolID)
	}

	cfg := b.logDeliveryConfigs[poolID]
	if cfg == nil {
		return &LogDeliveryConfig{Raw: map[string]any{}}, nil
	}

	rawCopy := make(map[string]any, len(cfg.Raw))
	maps.Copy(rawCopy, cfg.Raw)

	return &LogDeliveryConfig{Raw: rawCopy}, nil
}

// ---------------------------------------------------------------------------
// UI Customization
// ---------------------------------------------------------------------------

// uiKey builds the map key for UI customization.
func uiKey(poolID, clientID string) string {
	return poolID + ":" + clientID
}

// SetUICustomization stores hosted-UI CSS for a pool (and optional client).
func (b *InMemoryBackend) SetUICustomization(poolID, clientID, css string) (*UICustomization, error) {
	b.mu.Lock("SetUICustomization")
	defer b.mu.Unlock()

	if _, ok := b.pools[poolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, poolID)
	}

	ui := &UICustomization{UserPoolID: poolID, ClientID: clientID, CSS: css}
	b.uiCustomizations[uiKey(poolID, clientID)] = ui
	cp := *ui

	return &cp, nil
}

// GetUICustomization retrieves hosted-UI CSS for a pool and optional client.
func (b *InMemoryBackend) GetUICustomization(poolID, clientID string) (*UICustomization, error) {
	b.mu.RLock("GetUICustomization")
	defer b.mu.RUnlock()

	if _, ok := b.pools[poolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, poolID)
	}

	ui := b.uiCustomizations[uiKey(poolID, clientID)]
	if ui == nil {
		return &UICustomization{UserPoolID: poolID, ClientID: clientID}, nil
	}

	cp := *ui

	return &cp, nil
}

// ---------------------------------------------------------------------------
// Managed Login Branding
// ---------------------------------------------------------------------------

// CreateManagedLoginBranding creates a managed login branding record.
func (b *InMemoryBackend) CreateManagedLoginBranding(userPoolID, clientID string) (*ManagedLoginBranding, error) {
	b.mu.Lock("CreateManagedLoginBranding")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if b.managedLoginBrandings[userPoolID] == nil {
		b.managedLoginBrandings[userPoolID] = make(map[string]*ManagedLoginBranding)
	}

	id := "mlb-" + randomAlphanumeric(managedLoginBrandingIDLen)
	now := time.Now()
	mlb := &ManagedLoginBranding{
		ManagedLoginBrandingID: id,
		UserPoolID:             userPoolID,
		ClientID:               clientID,
		CreatedAt:              now,
		LastModifiedAt:         now,
	}
	b.managedLoginBrandings[userPoolID][id] = mlb

	cp := *mlb

	return &cp, nil
}

// DescribeManagedLoginBranding returns a managed login branding by ID.
func (b *InMemoryBackend) DescribeManagedLoginBranding(userPoolID, brandingID string) (*ManagedLoginBranding, error) {
	b.mu.RLock("DescribeManagedLoginBranding")
	defer b.mu.RUnlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	mlb, ok := b.managedLoginBrandings[userPoolID][brandingID]
	if !ok {
		return nil, fmt.Errorf("%w: managed login branding %q not found in pool %q",
			ErrUserPoolNotFound, brandingID, userPoolID)
	}

	cp := *mlb

	return &cp, nil
}

// DescribeManagedLoginBrandingByClient returns the managed login branding for a client.
func (b *InMemoryBackend) DescribeManagedLoginBrandingByClient(
	userPoolID, clientID string,
) (*ManagedLoginBranding, error) {
	b.mu.RLock("DescribeManagedLoginBrandingByClient")
	defer b.mu.RUnlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	for _, mlb := range b.managedLoginBrandings[userPoolID] {
		if mlb.ClientID == clientID {
			cp := *mlb

			return &cp, nil
		}
	}

	return &ManagedLoginBranding{UserPoolID: userPoolID, ClientID: clientID}, nil
}

// UpdateManagedLoginBranding updates a managed login branding record.
func (b *InMemoryBackend) UpdateManagedLoginBranding(userPoolID, brandingID string) (*ManagedLoginBranding, error) {
	b.mu.Lock("UpdateManagedLoginBranding")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	mlb, ok := b.managedLoginBrandings[userPoolID][brandingID]
	if !ok {
		return nil, fmt.Errorf("%w: managed login branding %q not found in pool %q",
			ErrUserPoolNotFound, brandingID, userPoolID)
	}

	mlb.LastModifiedAt = time.Now()
	cp := *mlb

	return &cp, nil
}

// DeleteManagedLoginBranding removes a managed login branding record.
func (b *InMemoryBackend) DeleteManagedLoginBranding(userPoolID, brandingID string) error {
	b.mu.Lock("DeleteManagedLoginBranding")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.managedLoginBrandings[userPoolID][brandingID]; !ok {
		return fmt.Errorf("%w: managed login branding %q not found in pool %q",
			ErrUserPoolNotFound, brandingID, userPoolID)
	}

	delete(b.managedLoginBrandings[userPoolID], brandingID)

	return nil
}

// ---------------------------------------------------------------------------
// Terms
// ---------------------------------------------------------------------------

// CreateTerms sets the terms and conditions text for a user pool.
func (b *InMemoryBackend) CreateTerms(userPoolID, text string) (*Terms, error) {
	b.mu.Lock("CreateTerms")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	t := &Terms{UserPoolID: userPoolID, Text: text}
	b.terms[userPoolID] = t
	cp := *t

	return &cp, nil
}

// DescribeTerms returns the terms and conditions for a user pool.
func (b *InMemoryBackend) DescribeTerms(userPoolID string) (*Terms, error) {
	b.mu.RLock("DescribeTerms")
	defer b.mu.RUnlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	t := b.terms[userPoolID]
	if t == nil {
		return &Terms{UserPoolID: userPoolID}, nil
	}

	cp := *t

	return &cp, nil
}

// ListTerms returns terms for a pool (returns slice of at most one element).
func (b *InMemoryBackend) ListTerms(userPoolID string) ([]*Terms, error) {
	b.mu.RLock("ListTerms")
	defer b.mu.RUnlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	t := b.terms[userPoolID]
	if t == nil {
		return []*Terms{}, nil
	}

	cp := *t

	return []*Terms{&cp}, nil
}

// UpdateTerms replaces the terms text for a user pool.
func (b *InMemoryBackend) UpdateTerms(userPoolID, text string) (*Terms, error) {
	b.mu.Lock("UpdateTerms")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	t := &Terms{UserPoolID: userPoolID, Text: text}
	b.terms[userPoolID] = t
	cp := *t

	return &cp, nil
}

// DeleteTerms removes the terms and conditions for a user pool.
func (b *InMemoryBackend) DeleteTerms(userPoolID string) error {
	b.mu.Lock("DeleteTerms")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	delete(b.terms, userPoolID)

	return nil
}

// ---------------------------------------------------------------------------
// User Import Job
// ---------------------------------------------------------------------------

const (
	managedLoginBrandingIDLen = 8
	userImportJobIDLen        = 10
)

// CreateUserImportJob creates a new import job for a user pool.
func (b *InMemoryBackend) CreateUserImportJob(userPoolID, jobName string) (*UserImportJob, error) {
	b.mu.Lock("CreateUserImportJob")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if b.userImportJobs[userPoolID] == nil {
		b.userImportJobs[userPoolID] = make(map[string]*UserImportJob)
	}

	jobID := "import-" + randomAlphanumeric(userImportJobIDLen)
	job := &UserImportJob{
		JobID:      jobID,
		JobName:    jobName,
		UserPoolID: userPoolID,
		Status:     "Created",
		CreatedAt:  time.Now(),
	}
	b.userImportJobs[userPoolID][jobID] = job

	cp := *job

	return &cp, nil
}

// DescribeUserImportJob returns a user import job by pool and job ID.
func (b *InMemoryBackend) DescribeUserImportJob(userPoolID, jobID string) (*UserImportJob, error) {
	b.mu.RLock("DescribeUserImportJob")
	defer b.mu.RUnlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	job, ok := b.userImportJobs[userPoolID][jobID]
	if !ok {
		return nil, fmt.Errorf("%w: import job %q not found in pool %q",
			ErrUserPoolNotFound, jobID, userPoolID)
	}

	cp := *job

	return &cp, nil
}

// ListUserImportJobs returns all import jobs for a pool sorted by creation time.
func (b *InMemoryBackend) ListUserImportJobs(userPoolID string) ([]*UserImportJob, error) {
	b.mu.RLock("ListUserImportJobs")
	defer b.mu.RUnlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	poolJobs := b.userImportJobs[userPoolID]
	out := make([]*UserImportJob, 0, len(poolJobs))

	for _, job := range poolJobs {
		cp := *job
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })

	return out, nil
}

// StartUserImportJob transitions a Created job to InProgress.
func (b *InMemoryBackend) StartUserImportJob(userPoolID, jobID string) (*UserImportJob, error) {
	b.mu.Lock("StartUserImportJob")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	job, ok := b.userImportJobs[userPoolID][jobID]
	if !ok {
		return nil, fmt.Errorf("%w: import job %q not found in pool %q",
			ErrUserPoolNotFound, jobID, userPoolID)
	}

	if job.Status != "Created" && job.Status != "Pending" {
		return nil, fmt.Errorf("%w: import job %q cannot be started from status %q",
			ErrInvalidParameter, jobID, job.Status)
	}

	job.Status = "InProgress"
	cp := *job

	return &cp, nil
}

// StopUserImportJob transitions an InProgress job to Stopped.
func (b *InMemoryBackend) StopUserImportJob(userPoolID, jobID string) (*UserImportJob, error) {
	b.mu.Lock("StopUserImportJob")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	job, ok := b.userImportJobs[userPoolID][jobID]
	if !ok {
		return nil, fmt.Errorf("%w: import job %q not found in pool %q",
			ErrUserPoolNotFound, jobID, userPoolID)
	}

	if job.Status != "InProgress" {
		return nil, fmt.Errorf("%w: import job %q cannot be stopped from status %q",
			ErrInvalidParameter, jobID, job.Status)
	}

	job.Status = "Stopped"
	cp := *job

	return &cp, nil
}

// ---------------------------------------------------------------------------
// User Pool Client Secrets
// ---------------------------------------------------------------------------

// ListUserPoolClientSecrets returns the secret(s) for a client. AWS allows at most one active secret.
func (b *InMemoryBackend) ListUserPoolClientSecrets(userPoolID, clientID string) ([]string, error) {
	b.mu.RLock("ListUserPoolClientSecrets")
	defer b.mu.RUnlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	client, ok := b.clients[clientID]
	if !ok || client.UserPoolID != userPoolID {
		return nil, fmt.Errorf("%w: client %q not found in pool %q", ErrClientNotFound, clientID, userPoolID)
	}

	if client.ClientSecret == "" {
		return []string{}, nil
	}

	return []string{client.ClientSecret}, nil
}

// DeleteUserPoolClientSecret removes the client secret from a pool client.
func (b *InMemoryBackend) DeleteUserPoolClientSecret(userPoolID, clientID string) error {
	b.mu.Lock("DeleteUserPoolClientSecret")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	client, ok := b.clients[clientID]
	if !ok || client.UserPoolID != userPoolID {
		return fmt.Errorf("%w: client %q not found in pool %q", ErrClientNotFound, clientID, userPoolID)
	}

	client.ClientSecret = ""

	return nil
}

// ---------------------------------------------------------------------------
// Device tracking
// ---------------------------------------------------------------------------

// paginateDevicesLocked returns a page of devices for the given store key,
// sorted by DeviceKey for stable pagination. Caller must hold b.mu.
func (b *InMemoryBackend) paginateDevicesLocked(key string, limit int, nextToken string) ([]*Device, string) {
	devices := b.devices[key]
	all := make([]*Device, 0, len(devices))

	for _, d := range devices {
		cp := *d
		cp.Attributes = maps.Clone(d.Attributes)
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].DeviceKey < all[j].DeviceKey })

	startIdx := 0

	if nextToken != "" {
		for i, d := range all {
			if d.DeviceKey == nextToken {
				startIdx = i

				break
			}
		}
	}

	all = all[startIdx:]

	if limit <= 0 || limit > len(all) {
		return all, ""
	}

	page := all[:limit]
	newToken := ""

	if limit < len(all) {
		newToken = all[limit].DeviceKey
	}

	return page, newToken
}

// ConfirmDevice registers a new device for the authenticated user, or
// refreshes last-authenticated/attributes if the device is already known. If
// deviceKey is empty, a new one is generated: AWS normally derives DeviceKey
// client-side from SRP device-verifier material handed out during
// authentication, but this emulator does not mint device metadata during
// InitiateAuth, so it provisions a key here so ListDevices/GetDevice can
// enumerate the device afterward. Returns the confirmed device key and
// whether user confirmation is necessary (always false: this emulator does
// not model the adaptive-auth device-confirmation workflow).
func (b *InMemoryBackend) ConfirmDevice(accessToken, deviceKey, deviceName string) (string, bool, error) {
	b.mu.Lock("ConfirmDevice")
	defer b.mu.Unlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return "", false, err
	}

	if deviceKey == "" {
		deviceKey = b.region + "_" + uuid.New().String()
	}

	key := userStateKey(user.UserPoolID, user.Username)
	if b.devices[key] == nil {
		b.devices[key] = make(map[string]*Device)
	}

	now := time.Now()

	if existing, ok := b.devices[key][deviceKey]; ok {
		existing.LastAuthenticatedAt = now
		existing.LastModifiedAt = now

		if deviceName != "" {
			if existing.Attributes == nil {
				existing.Attributes = map[string]string{}
			}

			existing.Attributes["device_name"] = deviceName
		}

		return deviceKey, false, nil
	}

	attrs := map[string]string{}
	if deviceName != "" {
		attrs["device_name"] = deviceName
	}

	b.devices[key][deviceKey] = &Device{
		DeviceKey:           deviceKey,
		CreatedAt:           now,
		LastModifiedAt:      now,
		LastAuthenticatedAt: now,
		Attributes:          attrs,
		Status:              DeviceStatusNotRemembered,
	}

	return deviceKey, false, nil
}

// AdminGetDevice returns a single tracked device for a user (admin operation).
func (b *InMemoryBackend) AdminGetDevice(userPoolID, username, deviceKey string) (*Device, error) {
	b.mu.RLock("AdminGetDevice")
	defer b.mu.RUnlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.users[userPoolID][username]; !ok {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	dev, ok := b.devices[userStateKey(userPoolID, username)][deviceKey]
	if !ok {
		return nil, fmt.Errorf("%w: device %q not found", ErrDeviceNotFound, deviceKey)
	}

	cp := *dev
	cp.Attributes = maps.Clone(dev.Attributes)

	return &cp, nil
}

// GetDevice returns a single tracked device for the authenticated user.
func (b *InMemoryBackend) GetDevice(accessToken, deviceKey string) (*Device, error) {
	b.mu.RLock("GetDevice")
	defer b.mu.RUnlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return nil, err
	}

	dev, ok := b.devices[userStateKey(user.UserPoolID, user.Username)][deviceKey]
	if !ok {
		return nil, fmt.Errorf("%w: device %q not found", ErrDeviceNotFound, deviceKey)
	}

	cp := *dev
	cp.Attributes = maps.Clone(dev.Attributes)

	return &cp, nil
}

// AdminListDevices returns a page of tracked devices for a user (admin operation).
func (b *InMemoryBackend) AdminListDevices(
	userPoolID, username string,
	limit int,
	nextToken string,
) ([]*Device, string, error) {
	b.mu.RLock("AdminListDevices")
	defer b.mu.RUnlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, "", fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.users[userPoolID][username]; !ok {
		return nil, "", fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	devices, token := b.paginateDevicesLocked(userStateKey(userPoolID, username), limit, nextToken)

	return devices, token, nil
}

// ListDevices returns a page of tracked devices for the authenticated user.
func (b *InMemoryBackend) ListDevices(accessToken string, limit int, nextToken string) ([]*Device, string, error) {
	b.mu.RLock("ListDevices")
	defer b.mu.RUnlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return nil, "", err
	}

	devices, token := b.paginateDevicesLocked(userStateKey(user.UserPoolID, user.Username), limit, nextToken)

	return devices, token, nil
}

// validDeviceStatus reports whether status is a recognized DeviceRememberedStatusType value.
func validDeviceStatus(status string) bool {
	return status == DeviceStatusRemembered || status == DeviceStatusNotRemembered
}

// AdminUpdateDeviceStatus updates a tracked device's remembered status (admin operation).
func (b *InMemoryBackend) AdminUpdateDeviceStatus(userPoolID, username, deviceKey, status string) error {
	b.mu.Lock("AdminUpdateDeviceStatus")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.users[userPoolID][username]; !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	if !validDeviceStatus(status) {
		return fmt.Errorf("%w: DeviceRememberedStatus must be %q or %q",
			ErrInvalidParameter, DeviceStatusRemembered, DeviceStatusNotRemembered)
	}

	dev, ok := b.devices[userStateKey(userPoolID, username)][deviceKey]
	if !ok {
		return fmt.Errorf("%w: device %q not found", ErrDeviceNotFound, deviceKey)
	}

	dev.Status = status
	dev.LastModifiedAt = time.Now()

	return nil
}

// UpdateDeviceStatus updates a tracked device's remembered status for the authenticated user.
func (b *InMemoryBackend) UpdateDeviceStatus(accessToken, deviceKey, status string) error {
	b.mu.Lock("UpdateDeviceStatus")
	defer b.mu.Unlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return err
	}

	if !validDeviceStatus(status) {
		return fmt.Errorf("%w: DeviceRememberedStatus must be %q or %q",
			ErrInvalidParameter, DeviceStatusRemembered, DeviceStatusNotRemembered)
	}

	dev, ok := b.devices[userStateKey(user.UserPoolID, user.Username)][deviceKey]
	if !ok {
		return fmt.Errorf("%w: device %q not found", ErrDeviceNotFound, deviceKey)
	}

	dev.Status = status
	dev.LastModifiedAt = time.Now()

	return nil
}

// ForgetDevice deletes a tracked device for the authenticated user.
func (b *InMemoryBackend) ForgetDevice(accessToken, deviceKey string) error {
	b.mu.Lock("ForgetDevice")
	defer b.mu.Unlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return err
	}

	key := userStateKey(user.UserPoolID, user.Username)
	if _, ok := b.devices[key][deviceKey]; !ok {
		return fmt.Errorf("%w: device %q not found", ErrDeviceNotFound, deviceKey)
	}

	delete(b.devices[key], deviceKey)

	return nil
}

// ---------------------------------------------------------------------------
// WebAuthn / passkeys
// ---------------------------------------------------------------------------

const (
	webauthnChallengeLen   = 32
	webauthnUserHandleLen  = 16
	webauthnTimeoutMillis  = 60000
	webauthnRelyingPartyID = "localhost"
)

// StartWebAuthnRegistration returns real WebAuthn CredentialCreationOptions
// (rp, user, challenge, pubKeyCredParams) for the authenticated user to pass
// to the browser's navigator.credentials.create().
func (b *InMemoryBackend) StartWebAuthnRegistration(accessToken string) (map[string]any, error) {
	b.mu.RLock("StartWebAuthnRegistration")
	defer b.mu.RUnlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return nil, err
	}

	rpName := user.UserPoolID
	if pool, ok := b.pools[user.UserPoolID]; ok && pool.Name != "" {
		rpName = pool.Name
	}

	challenge := make([]byte, webauthnChallengeLen)
	if _, randErr := rand.Read(challenge); randErr != nil {
		return nil, fmt.Errorf("generating webauthn challenge: %w", randErr)
	}

	userHandle := make([]byte, webauthnUserHandleLen)
	if _, randErr := rand.Read(userHandle); randErr != nil {
		return nil, fmt.Errorf("generating webauthn user handle: %w", randErr)
	}

	enc := base64.RawURLEncoding

	const jsonKeyName = "name"

	return map[string]any{
		"rp": map[string]any{
			"id":        webauthnRelyingPartyID,
			jsonKeyName: rpName,
		},
		"user": map[string]any{
			"id":          enc.EncodeToString(userHandle),
			jsonKeyName:   user.Username,
			"displayName": user.Username,
		},
		"challenge": enc.EncodeToString(challenge),
		"pubKeyCredParams": []map[string]any{
			{"type": "public-key", "alg": -7},
			{"type": "public-key", "alg": -257},
		},
		"timeout":     webauthnTimeoutMillis,
		"attestation": "none",
		"authenticatorSelection": map[string]any{
			"userVerification": "preferred",
		},
	}, nil
}

// CompleteWebAuthnRegistration stores a WebAuthn credential for the authenticated user.
func (b *InMemoryBackend) CompleteWebAuthnRegistration(
	accessToken, credentialID, authenticatorAttachment string,
) (*WebAuthnCredential, error) {
	b.mu.Lock("CompleteWebAuthnRegistration")
	defer b.mu.Unlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return nil, err
	}

	if credentialID == "" {
		return nil, fmt.Errorf("%w: Credential.id is required", ErrInvalidParameter)
	}

	key := userStateKey(user.UserPoolID, user.Username)
	if b.webauthnCredentials[key] == nil {
		b.webauthnCredentials[key] = make(map[string]*WebAuthnCredential)
	}

	cred := &WebAuthnCredential{
		CredentialID:            credentialID,
		FriendlyName:            fmt.Sprintf("Passkey %d", len(b.webauthnCredentials[key])+1),
		RelyingPartyID:          webauthnRelyingPartyID,
		AuthenticatorAttachment: authenticatorAttachment,
		CreatedAt:               time.Now(),
	}
	b.webauthnCredentials[key][credentialID] = cred

	cp := *cred

	return &cp, nil
}

// ListWebAuthnCredentials returns a page of WebAuthn credentials for the authenticated user.
func (b *InMemoryBackend) ListWebAuthnCredentials(
	accessToken string,
	limit int,
	nextToken string,
) ([]*WebAuthnCredential, string, error) {
	b.mu.RLock("ListWebAuthnCredentials")
	defer b.mu.RUnlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return nil, "", err
	}

	creds := b.webauthnCredentials[userStateKey(user.UserPoolID, user.Username)]
	all := make([]*WebAuthnCredential, 0, len(creds))

	for _, c := range creds {
		cp := *c
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].CredentialID < all[j].CredentialID })

	startIdx := 0

	if nextToken != "" {
		for i, c := range all {
			if c.CredentialID == nextToken {
				startIdx = i

				break
			}
		}
	}

	all = all[startIdx:]

	if limit <= 0 || limit > len(all) {
		return all, "", nil
	}

	page := all[:limit]
	newToken := ""

	if limit < len(all) {
		newToken = all[limit].CredentialID
	}

	return page, newToken, nil
}

// DeleteWebAuthnCredential removes a WebAuthn credential for the authenticated user.
func (b *InMemoryBackend) DeleteWebAuthnCredential(accessToken, credentialID string) error {
	b.mu.Lock("DeleteWebAuthnCredential")
	defer b.mu.Unlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return err
	}

	key := userStateKey(user.UserPoolID, user.Username)
	if _, ok := b.webauthnCredentials[key][credentialID]; !ok {
		return fmt.Errorf("%w: credential %q not found", ErrWebAuthnCredentialNotFound, credentialID)
	}

	delete(b.webauthnCredentials[key], credentialID)

	return nil
}

// ---------------------------------------------------------------------------
// Auth event feedback
// ---------------------------------------------------------------------------

// Auth event feedback values (FeedbackValueType).
const (
	AuthEventFeedbackValid   = "Valid"
	AuthEventFeedbackInvalid = "Invalid"
)

func validAuthEventFeedbackValue(v string) bool {
	return v == AuthEventFeedbackValid || v == AuthEventFeedbackInvalid
}

// paginateAuthEventsLocked returns a page of auth events for the given store
// key, newest first. Caller must hold b.mu.
func (b *InMemoryBackend) paginateAuthEventsLocked(key string, limit int, nextToken string) ([]*AuthEvent, string) {
	events := b.authEvents[key]
	all := make([]*AuthEvent, 0, len(events))

	for _, e := range events {
		cp := *e
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool {
		if all[i].CreatedAt.Equal(all[j].CreatedAt) {
			return all[i].EventID < all[j].EventID
		}

		return all[i].CreatedAt.After(all[j].CreatedAt)
	})

	startIdx := 0

	if nextToken != "" {
		for i, e := range all {
			if e.EventID == nextToken {
				startIdx = i

				break
			}
		}
	}

	all = all[startIdx:]

	if limit <= 0 || limit > len(all) {
		return all, ""
	}

	page := all[:limit]
	newToken := ""

	if limit < len(all) {
		newToken = all[limit].EventID
	}

	return page, newToken
}

// AdminListUserAuthEvents returns stored adaptive-authentication events for a
// user (admin operation). This emulator does not hook sign-in flows
// (InitiateAuth/AdminInitiateAuth) to synthesize risk events, so the store is
// real but starts empty per user; it returns a real, validated, paginated
// empty result rather than a hardcoded one (pool/user existence and
// NextToken semantics are honored).
func (b *InMemoryBackend) AdminListUserAuthEvents(
	userPoolID, username string,
	limit int,
	nextToken string,
) ([]*AuthEvent, string, error) {
	b.mu.RLock("AdminListUserAuthEvents")
	defer b.mu.RUnlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return nil, "", fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.users[userPoolID][username]; !ok {
		return nil, "", fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	events, token := b.paginateAuthEventsLocked(userStateKey(userPoolID, username), limit, nextToken)

	return events, token, nil
}

// updateAuthEventFeedbackLocked validates and applies feedback to a stored
// auth event. Caller must hold b.mu (write lock).
func (b *InMemoryBackend) updateAuthEventFeedbackLocked(userPoolID, username, eventID, feedbackValue string) error {
	if _, ok := b.pools[userPoolID]; !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.users[userPoolID][username]; !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	if !validAuthEventFeedbackValue(feedbackValue) {
		return fmt.Errorf("%w: FeedbackValue must be %q or %q",
			ErrInvalidParameter, AuthEventFeedbackValid, AuthEventFeedbackInvalid)
	}

	ev, ok := b.authEvents[userStateKey(userPoolID, username)][eventID]
	if !ok {
		return fmt.Errorf("%w: auth event %q not found", ErrAuthEventNotFound, eventID)
	}

	ev.FeedbackValue = feedbackValue
	ev.FeedbackDate = time.Now()

	return nil
}

// AdminUpdateAuthEventFeedback records feedback on a stored auth event (admin operation).
func (b *InMemoryBackend) AdminUpdateAuthEventFeedback(userPoolID, username, eventID, feedbackValue string) error {
	b.mu.Lock("AdminUpdateAuthEventFeedback")
	defer b.mu.Unlock()

	return b.updateAuthEventFeedbackLocked(userPoolID, username, eventID, feedbackValue)
}

// UpdateAuthEventFeedback records feedback on a stored auth event using an
// unauthenticated FeedbackToken flow (matches AWS: this op takes
// UserPoolId/Username directly rather than an AccessToken).
func (b *InMemoryBackend) UpdateAuthEventFeedback(userPoolID, username, eventID, feedbackValue string) error {
	b.mu.Lock("UpdateAuthEventFeedback")
	defer b.mu.Unlock()

	return b.updateAuthEventFeedbackLocked(userPoolID, username, eventID, feedbackValue)
}

// ---------------------------------------------------------------------------
// User settings (legacy MFAOptions) and provider linking
// ---------------------------------------------------------------------------

// AdminSetUserSettings persists legacy MFAOptions onto a user (admin operation).
func (b *InMemoryBackend) AdminSetUserSettings(userPoolID, username string, mfaOptions []MFAOptionType) error {
	b.mu.Lock("AdminSetUserSettings")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	user, ok := b.users[userPoolID][username]
	if !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	user.MFAOptions = mfaOptions

	return nil
}

// SetUserSettings persists legacy MFAOptions onto the authenticated user.
func (b *InMemoryBackend) SetUserSettings(accessToken string, mfaOptions []MFAOptionType) error {
	b.mu.Lock("SetUserSettings")
	defer b.mu.Unlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return err
	}

	user.MFAOptions = mfaOptions

	return nil
}

// AdminLinkProviderForUser links a federated identity (SourceUser) to an
// existing Cognito user (DestinationUser) in the given pool.
func (b *InMemoryBackend) AdminLinkProviderForUser(
	userPoolID, destinationUsername string,
	sourceProviderName, sourceAttrName, sourceAttrValue string,
) error {
	b.mu.Lock("AdminLinkProviderForUser")
	defer b.mu.Unlock()

	if _, ok := b.pools[userPoolID]; !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if destinationUsername == "" {
		return fmt.Errorf("%w: DestinationUser.ProviderAttributeValue is required", ErrInvalidParameter)
	}

	user, ok := b.users[userPoolID][destinationUsername]
	if !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, destinationUsername)
	}

	if sourceProviderName == "" || sourceAttrName == "" || sourceAttrValue == "" {
		return fmt.Errorf(
			"%w: SourceUser must include ProviderName, ProviderAttributeName, and ProviderAttributeValue",
			ErrInvalidParameter)
	}

	user.LinkedProviders = append(user.LinkedProviders, ProviderLink{
		ProviderName:           sourceProviderName,
		ProviderAttributeName:  sourceAttrName,
		ProviderAttributeValue: sourceAttrValue,
	})

	return nil
}

// ---------------------------------------------------------------------------
// User auth factors
// ---------------------------------------------------------------------------

// Auth factor values (AuthFactorType) recognized by GetUserAuthFactors.
const (
	authFactorPassword = "PASSWORD"
	authFactorSMSOTP   = "SMS_OTP"
	authFactorWebAuthn = "WEB_AUTHN"
)

// GetUserAuthFactors returns the authenticated user and the sign-in factors
// currently configured for their account, derived from stored MFA/WebAuthn state.
func (b *InMemoryBackend) GetUserAuthFactors(accessToken string) (*User, []string, error) {
	b.mu.RLock("GetUserAuthFactors")
	defer b.mu.RUnlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return nil, nil, err
	}

	factorSet := map[string]struct{}{}

	if user.PasswordHash != "" {
		factorSet[authFactorPassword] = struct{}{}
	}

	if slices.Contains(user.UserMFASettingList, "SMS_MFA") {
		factorSet[authFactorSMSOTP] = struct{}{}
	}

	for _, opt := range user.MFAOptions {
		if opt.DeliveryMedium == "SMS" {
			factorSet[authFactorSMSOTP] = struct{}{}
		}
	}

	if len(b.webauthnCredentials[userStateKey(user.UserPoolID, user.Username)]) > 0 {
		factorSet[authFactorWebAuthn] = struct{}{}
	}

	factors := make([]string, 0, len(factorSet))
	for f := range factorSet {
		factors = append(factors, f)
	}

	sort.Strings(factors)

	cp := *user

	return &cp, factors, nil
}
