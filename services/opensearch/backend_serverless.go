package opensearch

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const defaultSamlSessionTimeoutB64 = "MTY4MDAwMDAwMDAwMA=="

// Serverless collection status values.
const (
	slCollectionStatusActive   = "ACTIVE"
	slCollectionStatusCreating = "CREATING"
	slPolicyTypeData           = "data"
	slPolicyTypeNetwork        = "network"
	slSecurityConfigTypeSAML   = "saml"
)

// ServerlessCollection represents an OpenSearch Serverless collection.
type ServerlessCollection struct {
	StatusUntil        time.Time         `json:"statusUntil,omitzero"`
	Tags               map[string]string `json:"tags,omitempty"`
	KmsKeyArn          string            `json:"kmsKeyArn,omitempty"`
	DashboardEndpoint  string            `json:"dashboardEndpoint,omitempty"`
	Description        string            `json:"description,omitempty"`
	ID                 string            `json:"id"`
	CollectionEndpoint string            `json:"collectionEndpoint,omitempty"`
	Name               string            `json:"name"`
	Status             string            `json:"status"`
	Type               string            `json:"type"`
	Arn                string            `json:"arn"`
	CreatedDate        float64           `json:"createdDate"`
	LastModifiedDate   float64           `json:"lastModifiedDate"`
}

// ServerlessAccessPolicy represents an OpenSearch Serverless access policy.
type ServerlessAccessPolicy struct {
	Description      string  `json:"description,omitempty"`
	Name             string  `json:"name"`
	Policy           string  `json:"policy"`
	PolicyVersion    string  `json:"policyVersion"`
	Type             string  `json:"type"`
	CreatedDate      float64 `json:"createdDate"`
	LastModifiedDate float64 `json:"lastModifiedDate"`
}

// ServerlessSecurityConfig represents an OpenSearch Serverless security config.
type ServerlessSecurityConfig struct {
	SamlOptions      *ServerlessSAMLOptions `json:"samlOptions,omitempty"`
	ConfigVersion    string                 `json:"configVersion"`
	Description      string                 `json:"description,omitempty"`
	ID               string                 `json:"id"`
	Type             string                 `json:"type"`
	CreatedDate      float64                `json:"createdDate"`
	LastModifiedDate float64                `json:"lastModifiedDate"`
}

// ServerlessSAMLOptions holds SAML options for a serverless security config.
type ServerlessSAMLOptions struct {
	GroupAttribute string `json:"groupAttribute,omitempty"`
	Metadata       string `json:"metadata,omitempty"`
	UserAttribute  string `json:"userAttribute,omitempty"`
	SessionTimeout int    `json:"sessionTimeout,omitempty"`
}

// ServerlessEncryptionPolicy represents an OpenSearch Serverless encryption policy.
type ServerlessEncryptionPolicy struct {
	Description      string  `json:"description,omitempty"`
	Name             string  `json:"name"`
	Policy           string  `json:"policy"`
	PolicyVersion    string  `json:"policyVersion"`
	Type             string  `json:"type"`
	CreatedDate      float64 `json:"createdDate"`
	LastModifiedDate float64 `json:"lastModifiedDate"`
}

// ServerlessNetworkPolicy represents an OpenSearch Serverless network policy.
type ServerlessNetworkPolicy struct {
	Description      string  `json:"description,omitempty"`
	Name             string  `json:"name"`
	Policy           string  `json:"policy"`
	PolicyVersion    string  `json:"policyVersion"`
	Type             string  `json:"type"`
	CreatedDate      float64 `json:"createdDate"`
	LastModifiedDate float64 `json:"lastModifiedDate"`
}

// serverlessCollectionKey returns a stable key for a collection by name.
func serverlessCollectionKey(name string) string { return "sl-coll:" + name }

// serverlessAccessPolicyKey returns a key for an access policy by type and name.
func serverlessAccessPolicyKey(policyType, name string) string {
	return "sl-ap:" + policyType + ":" + name
}

// serverlessSecurityConfigKey returns a key for a security config by ID.
func serverlessSecurityConfigKey(id string) string { return "sl-sc:" + id }

// serverlessEncryptionPolicyKey returns a key for an encryption policy.
func serverlessEncryptionPolicyKey(policyType, name string) string {
	return "sl-ep:" + policyType + ":" + name
}

// serverlessNetworkPolicyKey returns a key for a network policy.
func serverlessNetworkPolicyKey(policyType, name string) string {
	return "sl-np:" + policyType + ":" + name
}

// CreateServerlessCollection creates a new OpenSearch Serverless collection.
func (b *InMemoryBackend) CreateServerlessCollection(
	name, collectionType, description, kmsKeyArn string,
	tagMap map[string]string,
) (*ServerlessCollection, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateServerlessCollection")
	defer b.mu.Unlock()

	b.slCollCounter++
	id := fmt.Sprintf("sl-%d", b.slCollCounter)

	now := float64(time.Now().Unix())
	region := b.region
	accountID := b.accountID
	collARN := arn.Build("aoss", region, accountID, fmt.Sprintf("collection/%s", id))
	collEndpoint := fmt.Sprintf("https://%s.%s.aoss.amazonaws.com", id, region)
	dashEndpoint := fmt.Sprintf("https://%s.%s.aoss.amazonaws.com/_dashboards", id, region)

	if collectionType == "" {
		collectionType = "SEARCH"
	}

	coll := &ServerlessCollection{
		ID:                 id,
		Name:               name,
		Arn:                collARN,
		Status:             slCollectionStatusActive,
		Type:               collectionType,
		Description:        description,
		KmsKeyArn:          kmsKeyArn,
		CollectionEndpoint: collEndpoint,
		DashboardEndpoint:  dashEndpoint,
		CreatedDate:        now,
		LastModifiedDate:   now,
		Tags:               tagMap,
	}

	// Real CREATING → ACTIVE transition. With no configured delay this settles
	// immediately (preserving the historical fast behaviour); with a delay the
	// collection is observably CREATING until the window elapses.
	if b.processingDelay > 0 {
		coll.Status = slCollectionStatusCreating
		coll.StatusUntil = b.clock().Add(b.processingDelay)
	}

	b.slCollections[serverlessCollectionKey(name)] = coll

	cp := *coll
	resolveCollectionStatus(&cp, b.clock())

	return &cp, nil
}

// resolveCollectionStatus settles a collection copy's transient CREATING status
// to ACTIVE once its window has elapsed. It never mutates stored state.
func resolveCollectionStatus(c *ServerlessCollection, now time.Time) {
	if c.Status == slCollectionStatusCreating && !c.StatusUntil.IsZero() &&
		!now.Before(c.StatusUntil) {
		c.Status = slCollectionStatusActive
	}
}

// collectionDeleteElapsed reports whether a DELETING collection has passed its
// window and should be treated as removed.
func collectionDeleteElapsed(c *ServerlessCollection, now time.Time) bool {
	return c.Status == statusDeleting && !c.StatusUntil.IsZero() && !now.Before(c.StatusUntil)
}

// purgeExpiredCollectionsLocked removes collections past their deleting window.
// The caller must hold the write lock.
func (b *InMemoryBackend) purgeExpiredCollectionsLocked() {
	now := b.clock()
	for key, c := range b.slCollections {
		if collectionDeleteElapsed(c, now) {
			delete(b.slCollections, key)
		}
	}
}

// BatchGetServerlessCollections returns a list of collections by IDs or names.
// Empty ids/names returns all collections.
func (b *InMemoryBackend) BatchGetServerlessCollections(ids, names []string) []*ServerlessCollection {
	b.mu.RLock("BatchGetServerlessCollections")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	nameSet := make(map[string]bool, len(names))
	for _, id := range ids {
		idSet[id] = true
	}
	for _, n := range names {
		nameSet[n] = true
	}

	now := b.clock()

	var out []*ServerlessCollection

	for _, c := range b.slCollections {
		if collectionDeleteElapsed(c, now) {
			continue
		}

		if len(idSet) == 0 && len(nameSet) == 0 {
			cp := *c
			resolveCollectionStatus(&cp, now)
			out = append(out, &cp)

			continue
		}

		if idSet[c.ID] || nameSet[c.Name] {
			cp := *c
			resolveCollectionStatus(&cp, now)
			out = append(out, &cp)
		}
	}

	return out
}

// DeleteServerlessCollection removes a collection by ID. With a processing delay
// configured the collection first enters an observable DELETING window before it
// is finally removed.
func (b *InMemoryBackend) DeleteServerlessCollection(id string) (*ServerlessCollection, error) {
	b.mu.Lock("DeleteServerlessCollection")
	defer b.mu.Unlock()

	b.purgeExpiredCollectionsLocked()

	now := b.clock()

	for key, c := range b.slCollections {
		if c.ID != id || collectionDeleteElapsed(c, now) {
			continue
		}

		if b.processingDelay == 0 {
			cp := *c
			cp.Status = statusDeleted
			delete(b.slCollections, key)

			return &cp, nil
		}

		c.Status = statusDeleting
		c.StatusUntil = now.Add(b.processingDelay)
		cp := *c

		return &cp, nil
	}

	return nil, fmt.Errorf("%w: serverless collection %s not found", ErrDomainNotFound, id)
}

// CreateServerlessAccessPolicy creates a new access policy.
func (b *InMemoryBackend) CreateServerlessAccessPolicy(
	policyType, name, description, policy string,
) (*ServerlessAccessPolicy, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateServerlessAccessPolicy")
	defer b.mu.Unlock()

	key := serverlessAccessPolicyKey(policyType, name)
	if _, exists := b.slAccessPolicies[key]; exists {
		return nil, fmt.Errorf("%w: access policy %s already exists", ErrApplicationAlreadyExists, name)
	}

	now := float64(time.Now().Unix())
	ap := &ServerlessAccessPolicy{
		Name:             name,
		Type:             policyType,
		Policy:           policy,
		Description:      description,
		PolicyVersion:    defaultSamlSessionTimeoutB64,
		CreatedDate:      now,
		LastModifiedDate: now,
	}

	b.slAccessPolicies[key] = ap

	cp := *ap

	return &cp, nil
}

// GetServerlessAccessPolicy retrieves an access policy by type and name.
func (b *InMemoryBackend) GetServerlessAccessPolicy(policyType, name string) (*ServerlessAccessPolicy, error) {
	b.mu.RLock("GetServerlessAccessPolicy")
	defer b.mu.RUnlock()

	ap, ok := b.slAccessPolicies[serverlessAccessPolicyKey(policyType, name)]
	if !ok {
		return nil, fmt.Errorf("%w: access policy %s not found", ErrApplicationNotFound, name)
	}

	cp := *ap

	return &cp, nil
}

// ListServerlessAccessPolicies lists access policies filtered by type.
func (b *InMemoryBackend) ListServerlessAccessPolicies(policyType string) []*ServerlessAccessPolicy {
	b.mu.RLock("ListServerlessAccessPolicies")
	defer b.mu.RUnlock()

	var out []*ServerlessAccessPolicy

	for _, ap := range b.slAccessPolicies {
		if policyType == "" || ap.Type == policyType {
			cp := *ap
			out = append(out, &cp)
		}
	}

	return out
}

// UpdateServerlessAccessPolicy updates an existing access policy.
func (b *InMemoryBackend) UpdateServerlessAccessPolicy(
	policyType, name, description, policy, policyVersion string,
) (*ServerlessAccessPolicy, error) {
	b.mu.Lock("UpdateServerlessAccessPolicy")
	defer b.mu.Unlock()

	key := serverlessAccessPolicyKey(policyType, name)
	ap, ok := b.slAccessPolicies[key]
	if !ok {
		return nil, fmt.Errorf("%w: access policy %s not found", ErrApplicationNotFound, name)
	}

	if description != "" {
		ap.Description = description
	}

	if policy != "" {
		ap.Policy = policy
	}

	_ = policyVersion
	ap.LastModifiedDate = float64(time.Now().Unix())
	ap.PolicyVersion = fmt.Sprintf("v%d", time.Now().UnixMilli())

	cp := *ap

	return &cp, nil
}

// DeleteServerlessAccessPolicy removes an access policy by type and name.
func (b *InMemoryBackend) DeleteServerlessAccessPolicy(policyType, name string) error {
	b.mu.Lock("DeleteServerlessAccessPolicy")
	defer b.mu.Unlock()

	key := serverlessAccessPolicyKey(policyType, name)
	if _, ok := b.slAccessPolicies[key]; !ok {
		return fmt.Errorf("%w: access policy %s not found", ErrApplicationNotFound, name)
	}

	delete(b.slAccessPolicies, key)

	return nil
}

// CreateServerlessSecurityConfig creates a new security config.
func (b *InMemoryBackend) CreateServerlessSecurityConfig(
	configType, description string,
	samlOptions *ServerlessSAMLOptions,
) (*ServerlessSecurityConfig, error) {
	b.mu.Lock("CreateServerlessSecurityConfig")
	defer b.mu.Unlock()

	b.slSecConfigCounter++
	id := fmt.Sprintf("saml/%s/%d", b.accountID, b.slSecConfigCounter)

	now := float64(time.Now().Unix())
	sc := &ServerlessSecurityConfig{
		ID:               id,
		Type:             configType,
		Description:      description,
		SamlOptions:      samlOptions,
		ConfigVersion:    defaultSamlSessionTimeoutB64,
		CreatedDate:      now,
		LastModifiedDate: now,
	}

	b.slSecurityConfigs[serverlessSecurityConfigKey(id)] = sc

	cp := *sc

	return &cp, nil
}

// GetServerlessSecurityConfig retrieves a security config by ID.
func (b *InMemoryBackend) GetServerlessSecurityConfig(id string) (*ServerlessSecurityConfig, error) {
	b.mu.RLock("GetServerlessSecurityConfig")
	defer b.mu.RUnlock()

	sc, ok := b.slSecurityConfigs[serverlessSecurityConfigKey(id)]
	if !ok {
		return nil, fmt.Errorf("%w: security config %s not found", ErrApplicationNotFound, id)
	}

	cp := *sc

	return &cp, nil
}

// ListServerlessSecurityConfigs lists security configs filtered by type.
func (b *InMemoryBackend) ListServerlessSecurityConfigs(configType string) []*ServerlessSecurityConfig {
	b.mu.RLock("ListServerlessSecurityConfigs")
	defer b.mu.RUnlock()

	var out []*ServerlessSecurityConfig

	for _, sc := range b.slSecurityConfigs {
		if configType == "" || sc.Type == configType {
			cp := *sc
			out = append(out, &cp)
		}
	}

	return out
}

// UpdateServerlessSecurityConfig updates an existing security config.
func (b *InMemoryBackend) UpdateServerlessSecurityConfig(
	id, description, configVersion string,
	samlOptions *ServerlessSAMLOptions,
) (*ServerlessSecurityConfig, error) {
	b.mu.Lock("UpdateServerlessSecurityConfig")
	defer b.mu.Unlock()

	sc, ok := b.slSecurityConfigs[serverlessSecurityConfigKey(id)]
	if !ok {
		return nil, fmt.Errorf("%w: security config %s not found", ErrApplicationNotFound, id)
	}

	if description != "" {
		sc.Description = description
	}

	if samlOptions != nil {
		sc.SamlOptions = samlOptions
	}

	_ = configVersion
	sc.LastModifiedDate = float64(time.Now().Unix())
	sc.ConfigVersion = fmt.Sprintf("v%d", time.Now().UnixMilli())

	cp := *sc

	return &cp, nil
}

// DeleteServerlessSecurityConfig removes a security config by ID.
func (b *InMemoryBackend) DeleteServerlessSecurityConfig(id string) error {
	b.mu.Lock("DeleteServerlessSecurityConfig")
	defer b.mu.Unlock()

	key := serverlessSecurityConfigKey(id)
	if _, ok := b.slSecurityConfigs[key]; !ok {
		return fmt.Errorf("%w: security config %s not found", ErrApplicationNotFound, id)
	}

	delete(b.slSecurityConfigs, key)

	return nil
}

// CreateServerlessEncryptionPolicy creates a new encryption policy.
func (b *InMemoryBackend) CreateServerlessEncryptionPolicy(
	policyType, name, description, policy string,
) (*ServerlessEncryptionPolicy, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateServerlessEncryptionPolicy")
	defer b.mu.Unlock()

	key := serverlessEncryptionPolicyKey(policyType, name)
	if _, exists := b.slEncryptionPolicies[key]; exists {
		return nil, fmt.Errorf("%w: encryption policy %s already exists", ErrApplicationAlreadyExists, name)
	}

	now := float64(time.Now().Unix())
	ep := &ServerlessEncryptionPolicy{
		Name:             name,
		Type:             policyType,
		Policy:           policy,
		Description:      description,
		PolicyVersion:    defaultSamlSessionTimeoutB64,
		CreatedDate:      now,
		LastModifiedDate: now,
	}

	b.slEncryptionPolicies[key] = ep

	cp := *ep

	return &cp, nil
}

// GetServerlessEncryptionPolicy retrieves an encryption policy by type and name.
func (b *InMemoryBackend) GetServerlessEncryptionPolicy(policyType, name string) (*ServerlessEncryptionPolicy, error) {
	b.mu.RLock("GetServerlessEncryptionPolicy")
	defer b.mu.RUnlock()

	ep, ok := b.slEncryptionPolicies[serverlessEncryptionPolicyKey(policyType, name)]
	if !ok {
		return nil, fmt.Errorf("%w: encryption policy %s not found", ErrApplicationNotFound, name)
	}

	cp := *ep

	return &cp, nil
}

// ListServerlessEncryptionPolicies lists encryption policies filtered by type.
func (b *InMemoryBackend) ListServerlessEncryptionPolicies(policyType string) []*ServerlessEncryptionPolicy {
	b.mu.RLock("ListServerlessEncryptionPolicies")
	defer b.mu.RUnlock()

	var out []*ServerlessEncryptionPolicy

	for _, ep := range b.slEncryptionPolicies {
		if policyType == "" || ep.Type == policyType {
			cp := *ep
			out = append(out, &cp)
		}
	}

	return out
}

// UpdateServerlessEncryptionPolicy updates an existing encryption policy.
func (b *InMemoryBackend) UpdateServerlessEncryptionPolicy(
	policyType, name, description, policy, policyVersion string,
) (*ServerlessEncryptionPolicy, error) {
	b.mu.Lock("UpdateServerlessEncryptionPolicy")
	defer b.mu.Unlock()

	key := serverlessEncryptionPolicyKey(policyType, name)
	ep, ok := b.slEncryptionPolicies[key]
	if !ok {
		return nil, fmt.Errorf("%w: encryption policy %s not found", ErrApplicationNotFound, name)
	}

	if description != "" {
		ep.Description = description
	}

	if policy != "" {
		ep.Policy = policy
	}

	_ = policyVersion
	ep.LastModifiedDate = float64(time.Now().Unix())
	ep.PolicyVersion = fmt.Sprintf("v%d", time.Now().UnixMilli())

	cp := *ep

	return &cp, nil
}

// DeleteServerlessEncryptionPolicy removes an encryption policy.
func (b *InMemoryBackend) DeleteServerlessEncryptionPolicy(policyType, name string) error {
	b.mu.Lock("DeleteServerlessEncryptionPolicy")
	defer b.mu.Unlock()

	key := serverlessEncryptionPolicyKey(policyType, name)
	if _, ok := b.slEncryptionPolicies[key]; !ok {
		return fmt.Errorf("%w: encryption policy %s not found", ErrApplicationNotFound, name)
	}

	delete(b.slEncryptionPolicies, key)

	return nil
}

// CreateServerlessNetworkPolicy creates a new network policy.
func (b *InMemoryBackend) CreateServerlessNetworkPolicy(
	policyType, name, description, policy string,
) (*ServerlessNetworkPolicy, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateServerlessNetworkPolicy")
	defer b.mu.Unlock()

	key := serverlessNetworkPolicyKey(policyType, name)
	if _, exists := b.slNetworkPolicies[key]; exists {
		return nil, fmt.Errorf("%w: network policy %s already exists", ErrApplicationAlreadyExists, name)
	}

	now := float64(time.Now().Unix())
	np := &ServerlessNetworkPolicy{
		Name:             name,
		Type:             policyType,
		Policy:           policy,
		Description:      description,
		PolicyVersion:    defaultSamlSessionTimeoutB64,
		CreatedDate:      now,
		LastModifiedDate: now,
	}

	b.slNetworkPolicies[key] = np

	cp := *np

	return &cp, nil
}

// ListServerlessNetworkPolicies lists network policies filtered by type.
func (b *InMemoryBackend) ListServerlessNetworkPolicies(policyType string) []*ServerlessNetworkPolicy {
	b.mu.RLock("ListServerlessNetworkPolicies")
	defer b.mu.RUnlock()

	var out []*ServerlessNetworkPolicy

	for _, np := range b.slNetworkPolicies {
		if policyType == "" || np.Type == policyType {
			cp := *np
			out = append(out, &cp)
		}
	}

	return out
}

// DeleteServerlessNetworkPolicy removes a network policy.
func (b *InMemoryBackend) DeleteServerlessNetworkPolicy(policyType, name string) error {
	b.mu.Lock("DeleteServerlessNetworkPolicy")
	defer b.mu.Unlock()

	key := serverlessNetworkPolicyKey(policyType, name)
	if _, ok := b.slNetworkPolicies[key]; !ok {
		return fmt.Errorf("%w: network policy %s not found", ErrApplicationNotFound, name)
	}

	delete(b.slNetworkPolicies, key)

	return nil
}
