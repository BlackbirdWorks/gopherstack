package verifiedpermissions

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrPolicyStoreNotFound is returned when a policy store is not found.
	ErrPolicyStoreNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrPolicyNotFound is returned when a policy is not found.
	ErrPolicyNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrPolicyTemplateNotFound is returned when a policy template is not found.
	ErrPolicyTemplateNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
)

// PolicyStore represents an Amazon Verified Permissions policy store.
type PolicyStore struct {
	CreatedDate   time.Time
	LastUpdated   time.Time
	Tags          map[string]string
	PolicyStoreID string
	Arn           string
	Description   string
	AccountID     string
	Region        string
}

// Policy represents a policy in a Verified Permissions policy store.
type Policy struct {
	CreatedDate   time.Time
	LastUpdated   time.Time
	PolicyStoreID string
	PolicyID      string
	PolicyType    string
	Statement     string
}

// PolicyTemplate represents a policy template in a Verified Permissions policy store.
type PolicyTemplate struct {
	CreatedDate      time.Time
	LastUpdated      time.Time
	PolicyStoreID    string
	PolicyTemplateID string
	Description      string
	Statement        string
}

// policyStoreARN builds the ARN for a policy store.
func policyStoreARN(accountID, region, policyStoreID string) string {
	return arn.Build("verifiedpermissions", region, accountID, "policy-store/"+policyStoreID)
}

// clonePolicyStore returns a deep copy of a PolicyStore.
func clonePolicyStore(ps *PolicyStore) *PolicyStore {
	cp := *ps
	cp.Tags = make(map[string]string, len(ps.Tags))
	maps.Copy(cp.Tags, ps.Tags)

	return &cp
}

// clonePolicy returns a deep copy of a Policy.
func clonePolicy(p *Policy) *Policy {
	cp := *p

	return &cp
}

// clonePolicyTemplate returns a deep copy of a PolicyTemplate.
func clonePolicyTemplate(pt *PolicyTemplate) *PolicyTemplate {
	cp := *pt

	return &cp
}

// InMemoryBackend is the in-memory store for Verified Permissions resources.
type InMemoryBackend struct {
	policyStores    map[string]*PolicyStore
	policies        map[string]map[string]*Policy         // policyStoreID -> policyID -> Policy
	policyTemplates map[string]map[string]*PolicyTemplate // policyStoreID -> templateID -> PolicyTemplate
	mu              *lockmetrics.RWMutex
	accountID       string
	region          string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		policyStores:    make(map[string]*PolicyStore),
		policies:        make(map[string]map[string]*Policy),
		policyTemplates: make(map[string]map[string]*PolicyTemplate),
		accountID:       accountID,
		region:          region,
		mu:              lockmetrics.New("verifiedpermissions"),
	}
}

// CreatePolicyStore creates a new policy store.
func (b *InMemoryBackend) CreatePolicyStore(description string, tags map[string]string) (*PolicyStore, error) {
	b.mu.Lock("CreatePolicyStore")
	defer b.mu.Unlock()

	id := uuid.NewString()
	merged := make(map[string]string, len(tags))
	maps.Copy(merged, tags)

	now := time.Now()
	ps := &PolicyStore{
		PolicyStoreID: id,
		Arn:           policyStoreARN(b.accountID, b.region, id),
		Description:   description,
		CreatedDate:   now,
		LastUpdated:   now,
		Tags:          merged,
		AccountID:     b.accountID,
		Region:        b.region,
	}
	b.policyStores[id] = ps
	b.policies[id] = make(map[string]*Policy)
	b.policyTemplates[id] = make(map[string]*PolicyTemplate)

	return clonePolicyStore(ps), nil
}

// GetPolicyStore returns the policy store with the given ID.
func (b *InMemoryBackend) GetPolicyStore(policyStoreID string) (*PolicyStore, error) {
	b.mu.RLock("GetPolicyStore")
	defer b.mu.RUnlock()

	ps, ok := b.policyStores[policyStoreID]
	if !ok {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	return clonePolicyStore(ps), nil
}

// ListPolicyStores returns all policy stores sorted by creation date (newest first).
func (b *InMemoryBackend) ListPolicyStores() []PolicyStore {
	b.mu.RLock("ListPolicyStores")
	defer b.mu.RUnlock()

	out := make([]PolicyStore, 0, len(b.policyStores))
	for _, ps := range b.policyStores {
		out = append(out, *clonePolicyStore(ps))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedDate.After(out[j].CreatedDate)
	})

	return out
}

// UpdatePolicyStore updates the description of a policy store.
func (b *InMemoryBackend) UpdatePolicyStore(policyStoreID, description string) (*PolicyStore, error) {
	b.mu.Lock("UpdatePolicyStore")
	defer b.mu.Unlock()

	ps, ok := b.policyStores[policyStoreID]
	if !ok {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	ps.Description = description
	ps.LastUpdated = time.Now()

	return clonePolicyStore(ps), nil
}

// DeletePolicyStore removes a policy store and all its policies and templates.
func (b *InMemoryBackend) DeletePolicyStore(policyStoreID string) error {
	b.mu.Lock("DeletePolicyStore")
	defer b.mu.Unlock()

	if _, ok := b.policyStores[policyStoreID]; !ok {
		return fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	delete(b.policyStores, policyStoreID)
	delete(b.policies, policyStoreID)
	delete(b.policyTemplates, policyStoreID)

	return nil
}

// CreatePolicy creates a new policy in the given policy store.
func (b *InMemoryBackend) CreatePolicy(policyStoreID, policyType, statement string) (*Policy, error) {
	b.mu.Lock("CreatePolicy")
	defer b.mu.Unlock()

	if _, ok := b.policyStores[policyStoreID]; !ok {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	id := uuid.NewString()
	now := time.Now()
	p := &Policy{
		PolicyID:      id,
		PolicyStoreID: policyStoreID,
		PolicyType:    policyType,
		Statement:     statement,
		CreatedDate:   now,
		LastUpdated:   now,
	}
	b.policies[policyStoreID][id] = p

	return clonePolicy(p), nil
}

// GetPolicy returns the policy with the given ID.
func (b *InMemoryBackend) GetPolicy(policyStoreID, policyID string) (*Policy, error) {
	b.mu.RLock("GetPolicy")
	defer b.mu.RUnlock()

	policies, ok := b.policies[policyStoreID]
	if !ok {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	p, ok := policies[policyID]
	if !ok {
		return nil, fmt.Errorf("%w: policy %s not found", ErrPolicyNotFound, policyID)
	}

	return clonePolicy(p), nil
}

// ListPolicies returns all policies in a policy store sorted by creation date.
func (b *InMemoryBackend) ListPolicies(policyStoreID string) ([]Policy, error) {
	b.mu.RLock("ListPolicies")
	defer b.mu.RUnlock()

	policies, ok := b.policies[policyStoreID]
	if !ok {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	out := make([]Policy, 0, len(policies))
	for _, p := range policies {
		out = append(out, *clonePolicy(p))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedDate.Before(out[j].CreatedDate)
	})

	return out, nil
}

// UpdatePolicy updates the statement of an existing policy.
func (b *InMemoryBackend) UpdatePolicy(policyStoreID, policyID, statement string) (*Policy, error) {
	b.mu.Lock("UpdatePolicy")
	defer b.mu.Unlock()

	policies, ok := b.policies[policyStoreID]
	if !ok {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	p, ok := policies[policyID]
	if !ok {
		return nil, fmt.Errorf("%w: policy %s not found", ErrPolicyNotFound, policyID)
	}

	if statement != "" {
		p.Statement = statement
	}

	p.LastUpdated = time.Now()

	return clonePolicy(p), nil
}

// DeletePolicy removes a policy from the given policy store.
func (b *InMemoryBackend) DeletePolicy(policyStoreID, policyID string) error {
	b.mu.Lock("DeletePolicy")
	defer b.mu.Unlock()

	policies, ok := b.policies[policyStoreID]
	if !ok {
		return fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	if _, exists := policies[policyID]; !exists {
		return fmt.Errorf("%w: policy %s not found", ErrPolicyNotFound, policyID)
	}

	delete(policies, policyID)

	return nil
}

// CreatePolicyTemplate creates a new policy template in the given policy store.
func (b *InMemoryBackend) CreatePolicyTemplate(policyStoreID, description, statement string) (*PolicyTemplate, error) {
	b.mu.Lock("CreatePolicyTemplate")
	defer b.mu.Unlock()

	if _, ok := b.policyStores[policyStoreID]; !ok {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	id := uuid.NewString()
	now := time.Now()
	pt := &PolicyTemplate{
		PolicyTemplateID: id,
		PolicyStoreID:    policyStoreID,
		Description:      description,
		Statement:        statement,
		CreatedDate:      now,
		LastUpdated:      now,
	}
	b.policyTemplates[policyStoreID][id] = pt

	return clonePolicyTemplate(pt), nil
}

// GetPolicyTemplate returns the policy template with the given ID.
func (b *InMemoryBackend) GetPolicyTemplate(policyStoreID, policyTemplateID string) (*PolicyTemplate, error) {
	b.mu.RLock("GetPolicyTemplate")
	defer b.mu.RUnlock()

	templates, ok := b.policyTemplates[policyStoreID]
	if !ok {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	pt, ok := templates[policyTemplateID]
	if !ok {
		return nil, fmt.Errorf("%w: policy template %s not found", ErrPolicyTemplateNotFound, policyTemplateID)
	}

	return clonePolicyTemplate(pt), nil
}

// ListPolicyTemplates returns all policy templates in a policy store sorted by creation date.
func (b *InMemoryBackend) ListPolicyTemplates(policyStoreID string) ([]PolicyTemplate, error) {
	b.mu.RLock("ListPolicyTemplates")
	defer b.mu.RUnlock()

	templates, ok := b.policyTemplates[policyStoreID]
	if !ok {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	out := make([]PolicyTemplate, 0, len(templates))
	for _, pt := range templates {
		out = append(out, *clonePolicyTemplate(pt))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedDate.Before(out[j].CreatedDate)
	})

	return out, nil
}

// UpdatePolicyTemplate updates the description and statement of a policy template.
func (b *InMemoryBackend) UpdatePolicyTemplate(
	policyStoreID, policyTemplateID, description, statement string,
) (*PolicyTemplate, error) {
	b.mu.Lock("UpdatePolicyTemplate")
	defer b.mu.Unlock()

	templates, ok := b.policyTemplates[policyStoreID]
	if !ok {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	pt, ok := templates[policyTemplateID]
	if !ok {
		return nil, fmt.Errorf("%w: policy template %s not found", ErrPolicyTemplateNotFound, policyTemplateID)
	}

	if description != "" {
		pt.Description = description
	}

	if statement != "" {
		pt.Statement = statement
	}

	pt.LastUpdated = time.Now()

	return clonePolicyTemplate(pt), nil
}

// DeletePolicyTemplate removes a policy template from the given policy store.
func (b *InMemoryBackend) DeletePolicyTemplate(policyStoreID, policyTemplateID string) error {
	b.mu.Lock("DeletePolicyTemplate")
	defer b.mu.Unlock()

	templates, ok := b.policyTemplates[policyStoreID]
	if !ok {
		return fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	if _, exists := templates[policyTemplateID]; !exists {
		return fmt.Errorf("%w: policy template %s not found", ErrPolicyTemplateNotFound, policyTemplateID)
	}

	delete(templates, policyTemplateID)

	return nil
}
