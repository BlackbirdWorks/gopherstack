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

// timeFormat is the ISO 8601 timestamp format used by Verified Permissions API responses.
const timeFormat = "2006-01-02T15:04:05.000Z"

var (
	// ErrPolicyStoreNotFound is returned when a policy store is not found.
	ErrPolicyStoreNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrPolicyNotFound is returned when a policy is not found.
	ErrPolicyNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrPolicyTemplateNotFound is returned when a policy template is not found.
	ErrPolicyTemplateNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrIdentitySourceNotFound is returned when an identity source is not found.
	ErrIdentitySourceNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrSchemaNotFound is returned when no schema has been set for a policy store.
	ErrSchemaNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrValidation is returned when input fails validation.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

// PolicyStore represents an Amazon Verified Permissions policy store.
type PolicyStore struct {
	CreatedDate   time.Time         `json:"createdDate"`
	LastUpdated   time.Time         `json:"lastUpdated"`
	Tags          map[string]string `json:"tags,omitempty"`
	PolicyStoreID string            `json:"policyStoreID"`
	Arn           string            `json:"arn"`
	Description   string            `json:"description"`
	AccountID     string            `json:"accountID"`
	Region        string            `json:"region"`
}

// Policy represents a policy in a Verified Permissions policy store.
type Policy struct {
	CreatedDate   time.Time `json:"createdDate"`
	LastUpdated   time.Time `json:"lastUpdated"`
	PolicyStoreID string    `json:"policyStoreID"`
	PolicyID      string    `json:"policyID"`
	PolicyType    string    `json:"policyType"`
	Statement     string    `json:"statement"`
}

// PolicyTemplate represents a policy template in a Verified Permissions policy store.
type PolicyTemplate struct {
	CreatedDate      time.Time `json:"createdDate"`
	LastUpdated      time.Time `json:"lastUpdated"`
	PolicyStoreID    string    `json:"policyStoreID"`
	PolicyTemplateID string    `json:"policyTemplateID"`
	Description      string    `json:"description"`
	Statement        string    `json:"statement"`
}

// IdentitySource represents an Amazon Verified Permissions identity source.
type IdentitySource struct {
	CreatedDate         time.Time `json:"createdDate"`
	LastUpdated         time.Time `json:"lastUpdated"`
	IdentitySourceID    string    `json:"identitySourceId"`
	PolicyStoreID       string    `json:"policyStoreId"`
	PrincipalEntityType string    `json:"principalEntityType"`
	UserPoolArn         string    `json:"userPoolArn,omitempty"`
	OpenIDIssuer        string    `json:"openIdIssuer,omitempty"`
	ClientIDs           []string  `json:"clientIds,omitempty"`
}

// PolicyStoreSchema holds the Cedar schema for a policy store.
type PolicyStoreSchema struct {
	CreatedDate time.Time `json:"createdDate"`
	LastUpdated time.Time `json:"lastUpdated"`
	Schema      string    `json:"schema"`
}

// AuthorizationRequest represents a single authorization evaluation request.
type AuthorizationRequest struct {
	PrincipalEntityType string `json:"principalEntityType,omitempty"`
	PrincipalEntityID   string `json:"principalEntityId,omitempty"`
	ActionType          string `json:"actionType,omitempty"`
	ActionID            string `json:"actionId,omitempty"`
	ResourceEntityType  string `json:"resourceEntityType,omitempty"`
	ResourceEntityID    string `json:"resourceEntityId,omitempty"`
}

// AuthDecision is the result of a single authorization evaluation.
type AuthDecision struct {
	Request             AuthorizationRequest `json:"request"`
	Decision            string               `json:"decision"`
	DeterminingPolicies []string             `json:"determiningPolicies"`
	Errors              []string             `json:"errors"`
}

// BatchGetPolicyItem identifies a policy to retrieve in a batch request.
type BatchGetPolicyItem struct {
	PolicyStoreID string `json:"policyStoreId"`
	PolicyID      string `json:"policyId"`
}

// BatchGetPolicyResult holds the results of a BatchGetPolicy call.
type BatchGetPolicyResult struct {
	Results []batchGetPolicyOutputItem `json:"results"`
	Errors  []batchGetPolicyErrorItem  `json:"errors"`
}

type batchGetPolicyOutputItem struct {
	PolicyStoreID   string `json:"policyStoreId"`
	PolicyID        string `json:"policyId"`
	PolicyType      string `json:"policyType"`
	CreatedDate     string `json:"createdDate"`
	LastUpdatedDate string `json:"lastUpdatedDate"`
}

type batchGetPolicyErrorItem struct {
	PolicyStoreID string `json:"policyStoreId"`
	PolicyID      string `json:"policyId"`
	Code          string `json:"code"`
	Message       string `json:"message"`
}

// decisionAllow is the ALLOW decision string returned by authorization evaluations.
const decisionAllow = "ALLOW"

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
	policyStores    map[string]*PolicyStore               // policyStoreID -> PolicyStore
	policies        map[string]map[string]*Policy         // policyStoreID -> policyID -> Policy
	policyTemplates map[string]map[string]*PolicyTemplate // policyStoreID -> templateID -> PolicyTemplate
	identitySources map[string]map[string]*IdentitySource // policyStoreID -> identitySourceID -> IdentitySource
	schemas         map[string]*PolicyStoreSchema         // policyStoreID -> schema
	arnIndex        map[string]string                     // ARN -> policyStoreID for O(1) tag ops
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
		identitySources: make(map[string]map[string]*IdentitySource),
		schemas:         make(map[string]*PolicyStoreSchema),
		arnIndex:        make(map[string]string),
		accountID:       accountID,
		region:          region,
		mu:              lockmetrics.New("verifiedpermissions"),
	}
}

// AccountID returns the AWS account ID configured for this backend.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

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
	b.identitySources[id] = make(map[string]*IdentitySource)
	b.arnIndex[ps.Arn] = id

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

	ps, ok := b.policyStores[policyStoreID]
	if !ok {
		return fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	delete(b.arnIndex, ps.Arn)
	delete(b.policyStores, policyStoreID)
	delete(b.policies, policyStoreID)
	delete(b.policyTemplates, policyStoreID)
	delete(b.identitySources, policyStoreID)
	delete(b.schemas, policyStoreID)

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

// Reset clears all policy store state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.policyStores = make(map[string]*PolicyStore)
	b.policies = make(map[string]map[string]*Policy)
	b.policyTemplates = make(map[string]map[string]*PolicyTemplate)
	b.identitySources = make(map[string]map[string]*IdentitySource)
	b.schemas = make(map[string]*PolicyStoreSchema)
	b.arnIndex = make(map[string]string)
}

// TagResource adds or updates tags on a policy store identified by its ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	id, ok := b.arnIndex[resourceARN]
	if !ok {
		return fmt.Errorf("%w: policy store with ARN %q not found", ErrPolicyStoreNotFound, resourceARN)
	}

	ps := b.policyStores[id]
	if ps.Tags == nil {
		ps.Tags = make(map[string]string, len(tags))
	}

	maps.Copy(ps.Tags, tags)

	return nil
}

// UntagResource removes tags from a policy store identified by its ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	id, ok := b.arnIndex[resourceARN]
	if !ok {
		return fmt.Errorf("%w: policy store with ARN %q not found", ErrPolicyStoreNotFound, resourceARN)
	}

	ps := b.policyStores[id]
	for _, k := range tagKeys {
		delete(ps.Tags, k)
	}

	return nil
}

// ListTagsForResource returns the tags for a policy store identified by its ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	id, ok := b.arnIndex[resourceARN]
	if !ok {
		return nil, fmt.Errorf("%w: policy store with ARN %q not found", ErrPolicyStoreNotFound, resourceARN)
	}

	return maps.Clone(b.policyStores[id].Tags), nil
}

// BatchGetPolicy retrieves multiple policies in a single request.
func (b *InMemoryBackend) BatchGetPolicy(items []BatchGetPolicyItem) BatchGetPolicyResult {
	b.mu.RLock("BatchGetPolicy")
	defer b.mu.RUnlock()

	result := BatchGetPolicyResult{
		Results: make([]batchGetPolicyOutputItem, 0, len(items)),
		Errors:  make([]batchGetPolicyErrorItem, 0, len(items)),
	}

	for _, item := range items {
		policies, ok := b.policies[item.PolicyStoreID]
		if !ok {
			result.Errors = append(result.Errors, batchGetPolicyErrorItem{
				PolicyStoreID: item.PolicyStoreID,
				PolicyID:      item.PolicyID,
				Code:          "POLICY_STORE_NOT_FOUND",
				Message:       fmt.Sprintf("policy store %s not found", item.PolicyStoreID),
			})

			continue
		}

		p, ok := policies[item.PolicyID]
		if !ok {
			result.Errors = append(result.Errors, batchGetPolicyErrorItem{
				PolicyStoreID: item.PolicyStoreID,
				PolicyID:      item.PolicyID,
				Code:          "POLICY_NOT_FOUND",
				Message:       fmt.Sprintf("policy %s not found", item.PolicyID),
			})

			continue
		}

		result.Results = append(result.Results, batchGetPolicyOutputItem{
			PolicyStoreID:   p.PolicyStoreID,
			PolicyID:        p.PolicyID,
			PolicyType:      p.PolicyType,
			CreatedDate:     p.CreatedDate.UTC().Format(timeFormat),
			LastUpdatedDate: p.LastUpdated.UTC().Format(timeFormat),
		})
	}

	return result
}

// BatchIsAuthorized evaluates a batch of authorization requests.
// In this in-memory implementation all requests return ALLOW when the policy store exists.
func (b *InMemoryBackend) BatchIsAuthorized(
	policyStoreID string,
	requests []AuthorizationRequest,
) ([]AuthDecision, error) {
	b.mu.RLock("BatchIsAuthorized")
	defer b.mu.RUnlock()

	if _, ok := b.policyStores[policyStoreID]; !ok {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	decisions := make([]AuthDecision, 0, len(requests))

	for _, req := range requests {
		decisions = append(decisions, AuthDecision{
			Decision:            decisionAllow,
			DeterminingPolicies: []string{},
			Errors:              []string{},
			Request:             req,
		})
	}

	return decisions, nil
}

// BatchIsAuthorizedWithToken evaluates a batch of authorization requests using a token.
// In this in-memory implementation all requests return ALLOW when the policy store exists.
func (b *InMemoryBackend) BatchIsAuthorizedWithToken(
	policyStoreID string,
	requests []AuthorizationRequest,
) ([]AuthDecision, error) {
	b.mu.RLock("BatchIsAuthorizedWithToken")
	defer b.mu.RUnlock()

	if _, ok := b.policyStores[policyStoreID]; !ok {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	decisions := make([]AuthDecision, 0, len(requests))

	for _, req := range requests {
		decisions = append(decisions, AuthDecision{
			Decision:            decisionAllow,
			DeterminingPolicies: []string{},
			Errors:              []string{},
			Request:             req,
		})
	}

	return decisions, nil
}

// IsAuthorized evaluates a single authorization request.
// In this in-memory implementation it returns ALLOW when the policy store exists.
func (b *InMemoryBackend) IsAuthorized(policyStoreID string, req AuthorizationRequest) (*AuthDecision, error) {
	b.mu.RLock("IsAuthorized")
	defer b.mu.RUnlock()

	if _, ok := b.policyStores[policyStoreID]; !ok {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	return &AuthDecision{
		Decision:            decisionAllow,
		DeterminingPolicies: []string{},
		Errors:              []string{},
		Request:             req,
	}, nil
}

// IsAuthorizedWithToken evaluates a single authorization request using a token.
// In this in-memory implementation it returns ALLOW when the policy store exists.
func (b *InMemoryBackend) IsAuthorizedWithToken(
	policyStoreID string,
	req AuthorizationRequest,
) (*AuthDecision, error) {
	b.mu.RLock("IsAuthorizedWithToken")
	defer b.mu.RUnlock()

	if _, ok := b.policyStores[policyStoreID]; !ok {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	return &AuthDecision{
		Decision:            decisionAllow,
		DeterminingPolicies: []string{},
		Errors:              []string{},
		Request:             req,
	}, nil
}

// CreateIdentitySource creates a new identity source in the given policy store.
func (b *InMemoryBackend) CreateIdentitySource(
	policyStoreID, userPoolArn, openIDIssuer, principalEntityType string,
	clientIDs []string,
) (*IdentitySource, error) {
	b.mu.Lock("CreateIdentitySource")
	defer b.mu.Unlock()

	if _, ok := b.policyStores[policyStoreID]; !ok {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	id := uuid.NewString()
	now := time.Now()

	cloned := make([]string, len(clientIDs))
	copy(cloned, clientIDs)

	is := &IdentitySource{
		IdentitySourceID:    id,
		PolicyStoreID:       policyStoreID,
		PrincipalEntityType: principalEntityType,
		UserPoolArn:         userPoolArn,
		ClientIDs:           cloned,
		OpenIDIssuer:        openIDIssuer,
		CreatedDate:         now,
		LastUpdated:         now,
	}

	if b.identitySources[policyStoreID] == nil {
		b.identitySources[policyStoreID] = make(map[string]*IdentitySource)
	}

	b.identitySources[policyStoreID][id] = is

	return cloneIdentitySource(is), nil
}

// GetIdentitySource returns the identity source with the given ID.
func (b *InMemoryBackend) GetIdentitySource(policyStoreID, identitySourceID string) (*IdentitySource, error) {
	b.mu.RLock("GetIdentitySource")
	defer b.mu.RUnlock()

	sources, ok := b.identitySources[policyStoreID]
	if !ok {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	is, ok := sources[identitySourceID]
	if !ok {
		return nil, fmt.Errorf("%w: identity source %s not found", ErrIdentitySourceNotFound, identitySourceID)
	}

	return cloneIdentitySource(is), nil
}

// DeleteIdentitySource removes an identity source from the given policy store.
func (b *InMemoryBackend) DeleteIdentitySource(policyStoreID, identitySourceID string) error {
	b.mu.Lock("DeleteIdentitySource")
	defer b.mu.Unlock()

	sources, ok := b.identitySources[policyStoreID]
	if !ok {
		return fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	if _, exists := sources[identitySourceID]; !exists {
		return fmt.Errorf("%w: identity source %s not found", ErrIdentitySourceNotFound, identitySourceID)
	}

	delete(sources, identitySourceID)

	return nil
}

// PutSchema creates or replaces the schema for a policy store.
func (b *InMemoryBackend) PutSchema(policyStoreID, schema string) error {
	b.mu.Lock("PutSchema")
	defer b.mu.Unlock()

	if _, ok := b.policyStores[policyStoreID]; !ok {
		return fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	existing, ok := b.schemas[policyStoreID]
	if ok {
		existing.Schema = schema
		existing.LastUpdated = time.Now()
	} else {
		now := time.Now()
		b.schemas[policyStoreID] = &PolicyStoreSchema{
			Schema:      schema,
			CreatedDate: now,
			LastUpdated: now,
		}
	}

	return nil
}

// GetSchema returns the schema for a policy store.
func (b *InMemoryBackend) GetSchema(policyStoreID string) (*PolicyStoreSchema, error) {
	b.mu.RLock("GetSchema")
	defer b.mu.RUnlock()

	if _, ok := b.policyStores[policyStoreID]; !ok {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	s, ok := b.schemas[policyStoreID]
	if !ok {
		return nil, fmt.Errorf("%w: no schema found for policy store %s", ErrSchemaNotFound, policyStoreID)
	}

	cp := *s

	return &cp, nil
}

// cloneIdentitySource returns a deep copy of an IdentitySource.
func cloneIdentitySource(is *IdentitySource) *IdentitySource {
	cp := *is

	if len(is.ClientIDs) > 0 {
		cp.ClientIDs = make([]string, len(is.ClientIDs))
		copy(cp.ClientIDs, is.ClientIDs)
	}

	return &cp
}

// ListIdentitySources returns all identity sources for a policy store sorted by creation date.
func (b *InMemoryBackend) ListIdentitySources(policyStoreID string) ([]IdentitySource, error) {
	b.mu.RLock("ListIdentitySources")
	defer b.mu.RUnlock()

	if _, ok := b.policyStores[policyStoreID]; !ok {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	sources := b.identitySources[policyStoreID]
	out := make([]IdentitySource, 0, len(sources))

	for _, is := range sources {
		out = append(out, *cloneIdentitySource(is))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedDate.Before(out[j].CreatedDate)
	})

	return out, nil
}

// UpdateIdentitySource updates the configuration and principal entity type of an identity source.
func (b *InMemoryBackend) UpdateIdentitySource(
	policyStoreID, identitySourceID, userPoolArn, openIDIssuer, principalEntityType string,
	clientIDs []string,
) (*IdentitySource, error) {
	b.mu.Lock("UpdateIdentitySource")
	defer b.mu.Unlock()

	sources, ok := b.identitySources[policyStoreID]
	if !ok {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	is, ok := sources[identitySourceID]
	if !ok {
		return nil, fmt.Errorf("%w: identity source %s not found", ErrIdentitySourceNotFound, identitySourceID)
	}

	if userPoolArn != "" {
		is.UserPoolArn = userPoolArn
		cloned := make([]string, len(clientIDs))
		copy(cloned, clientIDs)
		is.ClientIDs = cloned
		is.OpenIDIssuer = ""
	} else if openIDIssuer != "" {
		is.OpenIDIssuer = openIDIssuer
		is.UserPoolArn = ""
		is.ClientIDs = nil
	}

	if principalEntityType != "" {
		is.PrincipalEntityType = principalEntityType
	}

	is.LastUpdated = time.Now()

	return cloneIdentitySource(is), nil
}

// AddPolicyStoreInternal inserts a pre-built PolicyStore directly into the backend (for test seeding).
func (b *InMemoryBackend) AddPolicyStoreInternal(ps *PolicyStore) {
	b.mu.Lock("AddPolicyStoreInternal")
	defer b.mu.Unlock()

	b.policyStores[ps.PolicyStoreID] = ps
	b.policies[ps.PolicyStoreID] = make(map[string]*Policy)
	b.policyTemplates[ps.PolicyStoreID] = make(map[string]*PolicyTemplate)
	b.identitySources[ps.PolicyStoreID] = make(map[string]*IdentitySource)
	b.arnIndex[ps.Arn] = ps.PolicyStoreID
}

// AddPolicyInternal inserts a pre-built Policy directly into the backend (for test seeding).
func (b *InMemoryBackend) AddPolicyInternal(p *Policy) {
	b.mu.Lock("AddPolicyInternal")
	defer b.mu.Unlock()

	if b.policies[p.PolicyStoreID] == nil {
		b.policies[p.PolicyStoreID] = make(map[string]*Policy)
	}

	b.policies[p.PolicyStoreID][p.PolicyID] = p
}

// AddPolicyTemplateInternal inserts a pre-built PolicyTemplate directly into the backend (for test seeding).
func (b *InMemoryBackend) AddPolicyTemplateInternal(pt *PolicyTemplate) {
	b.mu.Lock("AddPolicyTemplateInternal")
	defer b.mu.Unlock()

	if b.policyTemplates[pt.PolicyStoreID] == nil {
		b.policyTemplates[pt.PolicyStoreID] = make(map[string]*PolicyTemplate)
	}

	b.policyTemplates[pt.PolicyStoreID][pt.PolicyTemplateID] = pt
}

// AddIdentitySourceInternal inserts a pre-built IdentitySource directly into the backend (for test seeding).
func (b *InMemoryBackend) AddIdentitySourceInternal(is *IdentitySource) {
	b.mu.Lock("AddIdentitySourceInternal")
	defer b.mu.Unlock()

	if b.identitySources[is.PolicyStoreID] == nil {
		b.identitySources[is.PolicyStoreID] = make(map[string]*IdentitySource)
	}

	b.identitySources[is.PolicyStoreID][is.IdentitySourceID] = is
}
