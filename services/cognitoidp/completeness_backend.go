package cognitoidp

import (
	"fmt"
	"maps"
	"sort"
	"time"
)

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

// IdentityProvider represents a federated identity provider attached to a user pool.
type IdentityProvider struct {
	ProviderDetails map[string]string
	CreatedAt       time.Time
	LastModifiedAt  time.Time
	UserPoolID      string
	ProviderName    string
	ProviderType    string
}

// UserPoolDomain holds the custom domain configuration for a user pool.
type UserPoolDomain struct {
	Domain                 string
	UserPoolID             string
	CloudFrontDistribution string
	Status                 string
}

// RiskConfiguration stores adaptive authentication settings for a pool or client.
type RiskConfiguration struct {
	// Stored as an opaque blob; individual fields not needed for emulation.
	Raw map[string]any
}

// LogDeliveryConfig holds log delivery destination configuration for a pool.
type LogDeliveryConfig struct {
	Raw map[string]any
}

// UICustomization stores hosted-UI CSS and logo settings for a pool or client.
type UICustomization struct {
	UserPoolID string
	ClientID   string
	CSS        string
}

// ManagedLoginBranding stores managed login branding for a pool client.
type ManagedLoginBranding struct {
	CreatedAt              time.Time
	LastModifiedAt         time.Time
	ManagedLoginBrandingID string
	UserPoolID             string
	ClientID               string
}

// Terms stores the terms and conditions text for a user pool.
type Terms struct {
	UserPoolID string
	Text       string
}

// UserImportJob represents a bulk user import job.
type UserImportJob struct {
	CreatedAt  time.Time
	JobID      string
	JobName    string
	UserPoolID string
	Status     string // Created | Pending | InProgress | Stopping | Stopped | Succeeded | Failed | Expired
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
