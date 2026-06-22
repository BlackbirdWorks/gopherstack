package verifiedpermissions

import (
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	cedar "github.com/cedar-policy/cedar-go"
	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
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
	// ErrConflict is returned when a resource conflict prevents an operation.
	ErrConflict = awserr.New("ConflictException", awserr.ErrConflict)
)

// ValidationMode constants for policy store validation settings.
const (
	ValidationModeOff    = "OFF"
	ValidationModeStrict = "STRICT"
)

// DeletionProtection constants for policy store deletion protection.
const (
	DeletionProtectionEnabled  = "ENABLED"
	DeletionProtectionDisabled = "DISABLED"
)

// PolicyStore represents an Amazon Verified Permissions policy store.
type PolicyStore struct {
	CreatedDate        time.Time         `json:"createdDate"`
	LastUpdated        time.Time         `json:"lastUpdated"`
	Tags               map[string]string `json:"tags,omitempty"`
	PolicyStoreID      string            `json:"policyStoreID"`
	Arn                string            `json:"arn"`
	Description        string            `json:"description"`
	AccountID          string            `json:"accountID"`
	Region             string            `json:"region"`
	ValidationMode     string            `json:"validationMode"`
	DeletionProtection string            `json:"deletionProtection"`
}

// Policy represents a policy in a Verified Permissions policy store.
type Policy struct {
	CreatedDate         time.Time `json:"createdDate"`
	LastUpdated         time.Time `json:"lastUpdated"`
	PolicyStoreID       string    `json:"policyStoreID"`
	PolicyID            string    `json:"policyID"`
	PolicyType          string    `json:"policyType"` // STATIC | TEMPLATE_LINKED
	Statement           string    `json:"statement"`
	Description         string    `json:"description,omitempty"`
	PolicyTemplateID    string    `json:"policyTemplateID,omitempty"`
	PrincipalEntityType string    `json:"principalEntityType,omitempty"`
	PrincipalEntityID   string    `json:"principalEntityID,omitempty"`
	ResourceEntityType  string    `json:"resourceEntityType,omitempty"`
	ResourceEntityID    string    `json:"resourceEntityID,omitempty"`
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

// CognitoGroupConfig holds Cognito group-to-Cedar-entity mapping configuration.
type CognitoGroupConfig struct {
	GroupEntityType string `json:"groupEntityType,omitempty"`
}

// OIDCGroupConfig holds OIDC group claim to Cedar entity mapping configuration.
type OIDCGroupConfig struct {
	GroupClaim      string `json:"groupClaim,omitempty"`
	GroupEntityType string `json:"groupEntityType,omitempty"`
}

// OIDCTokenSelection holds configuration for which OIDC token to use for authorization.
type OIDCTokenSelection struct {
	TokenType        string   `json:"tokenType,omitempty"` // IDENTITY | ACCESS
	PrincipalIDClaim string   `json:"principalIdClaim,omitempty"`
	Audiences        []string `json:"audiences,omitempty"`
}

// IdentitySource represents an Amazon Verified Permissions identity source.
type IdentitySource struct {
	CreatedDate         time.Time           `json:"createdDate"`
	LastUpdated         time.Time           `json:"lastUpdated"`
	CognitoGroupConfig  *CognitoGroupConfig `json:"cognitoGroupConfig,omitempty"`
	OIDCGroupConfig     *OIDCGroupConfig    `json:"oidcGroupConfig,omitempty"`
	OIDCTokenSelection  *OIDCTokenSelection `json:"oidcTokenSelection,omitempty"`
	IdentitySourceID    string              `json:"identitySourceId"`
	PolicyStoreID       string              `json:"policyStoreId"`
	PrincipalEntityType string              `json:"principalEntityType"`
	UserPoolArn         string              `json:"userPoolArn,omitempty"`
	OpenIDIssuer        string              `json:"openIdIssuer,omitempty"`
	EntityIDPrefix      string              `json:"entityIdPrefix,omitempty"`
	ClientIDs           []string            `json:"clientIds,omitempty"`
}

// PolicyStoreSchema holds the Cedar schema for a policy store.
type PolicyStoreSchema struct {
	CreatedDate time.Time `json:"createdDate"`
	LastUpdated time.Time `json:"lastUpdated"`
	Schema      string    `json:"schema"`
	Namespaces  []string  `json:"namespaces,omitempty"`
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
	Results []Policy                  `json:"results"`
	Errors  []batchGetPolicyErrorItem `json:"errors"`
}

type batchGetPolicyErrorItem struct {
	PolicyStoreID string `json:"policyStoreId"`
	PolicyID      string `json:"policyId"`
	Code          string `json:"code"`
	Message       string `json:"message"`
}

// CreatePolicyParams holds parameters for creating a policy.
type CreatePolicyParams struct {
	PolicyType          string // "STATIC" or "TEMPLATE_LINKED"
	Statement           string // STATIC only
	Description         string // STATIC only
	PolicyTemplateID    string // TEMPLATE_LINKED only
	PrincipalEntityType string // TEMPLATE_LINKED only
	PrincipalEntityID   string // TEMPLATE_LINKED only
	ResourceEntityType  string // TEMPLATE_LINKED only
	ResourceEntityID    string // TEMPLATE_LINKED only
}

// UpdatePolicyParams holds parameters for updating a policy.
type UpdatePolicyParams struct {
	// For STATIC updates:
	Statement   string
	Description string
	// For TEMPLATE_LINKED principal/resource updates (template id is immutable):
	PrincipalEntityType string
	PrincipalEntityID   string
	ResourceEntityType  string
	ResourceEntityID    string
}

// IdentitySourceConfig holds full identity source configuration for create/update.
type IdentitySourceConfig struct {
	// Cognito
	UserPoolArn            string
	ClientIDs              []string
	CognitoGroupEntityType string
	// OIDC
	Issuer              string
	EntityIDPrefix      string
	OIDCGroupClaim      string
	OIDCGroupEntityType string
	// Token selection
	TokenType        string // "IDENTITY" or "ACCESS"
	PrincipalIDClaim string
	Audiences        []string
}

// ListPoliciesFilter holds filter params for ListPolicies.
type ListPoliciesFilter struct {
	PolicyType          string
	PolicyTemplateID    string
	PrincipalEntityType string
	PrincipalEntityID   string
	ResourceEntityType  string
	ResourceEntityID    string
}

// decisionAllow / decisionDeny are the decision strings returned by authorization evaluations.
const (
	decisionAllow = "ALLOW"
	decisionDeny  = "DENY"
)

const (
	policyTypeStatic         = "STATIC"
	policyTypeTemplateLinked = "TEMPLATE_LINKED"
	arnKindPolicyStore       = "policyStore"
	arnKindPolicy            = "policy"
	arnKindPolicyTemplate    = "policyTemplate"
	arnKindIdentitySource    = "identitySource"
)

// arnNoRegion builds an ARN with empty region (verifiedpermissions uses global ARNs).
func arnNoRegion(accountID, resourceType, resourceID string) string {
	return arn.Build("verifiedpermissions", "", accountID, fmt.Sprintf("%s/%s", resourceType, resourceID))
}

// policyStoreARN builds the ARN for a policy store.
func policyStoreARN(accountID, _, policyStoreID string) string {
	return arnNoRegion(accountID, "policy-store", policyStoreID)
}

// policyARN builds the ARN for a policy.
func policyARN(accountID, policyStoreID, policyID string) string {
	return arn.Build("verifiedpermissions", "", accountID, fmt.Sprintf("policy/%s/%s", policyStoreID, policyID))
}

// policyTemplateARN builds the ARN for a policy template.
func policyTemplateARN(accountID, policyStoreID, templateID string) string {
	resource := fmt.Sprintf("policy-template/%s/%s", policyStoreID, templateID)

	return arn.Build("verifiedpermissions", "", accountID, resource)
}

// identitySourceARN builds the ARN for an identity source.
func identitySourceARN(accountID, policyStoreID, sourceID string) string {
	resource := fmt.Sprintf("identity-source/%s/%s", policyStoreID, sourceID)

	return arn.Build("verifiedpermissions", "", accountID, resource)
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

// cloneIdentitySource returns a deep copy of an IdentitySource.
func cloneIdentitySource(is *IdentitySource) *IdentitySource {
	cp := *is

	if len(is.ClientIDs) > 0 {
		cp.ClientIDs = make([]string, len(is.ClientIDs))
		copy(cp.ClientIDs, is.ClientIDs)
	}

	if is.CognitoGroupConfig != nil {
		cfg := *is.CognitoGroupConfig
		cp.CognitoGroupConfig = &cfg
	}

	if is.OIDCGroupConfig != nil {
		cfg := *is.OIDCGroupConfig
		cp.OIDCGroupConfig = &cfg
	}

	if is.OIDCTokenSelection != nil {
		sel := *is.OIDCTokenSelection
		if len(is.OIDCTokenSelection.Audiences) > 0 {
			sel.Audiences = make([]string, len(is.OIDCTokenSelection.Audiences))
			copy(sel.Audiences, is.OIDCTokenSelection.Audiences)
		}
		cp.OIDCTokenSelection = &sel
	}

	return &cp
}

// InMemoryBackend is the in-memory store for Verified Permissions resources.
type InMemoryBackend struct {
	policyStores    map[string]*PolicyStore               // policyStoreID -> PolicyStore
	policies        map[string]map[string]*Policy         // policyStoreID -> policyID -> Policy
	policyTemplates map[string]map[string]*PolicyTemplate // policyStoreID -> templateID -> PolicyTemplate
	identitySources map[string]map[string]*IdentitySource // policyStoreID -> identitySourceID -> IdentitySource
	schemas         map[string]*PolicyStoreSchema         // policyStoreID -> schema
	// arnIndex maps ARN -> (resourceType, policyStoreID, resourceID) for O(1) tag ops
	// values are encoded as "policyStore:<id>", "policy:<storeID>:<policyID>", etc.
	arnIndex  map[string]string
	mu        *lockmetrics.RWMutex
	accountID string
	region    string
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

// validateTagInput checks tag count, key/value length, and reserved prefix constraints.
func validateTagInput(existing map[string]string, newTags map[string]string) error {
	const maxTagCount = 50
	const maxKeyLen = 128
	const maxValLen = 256

	total := len(existing) + len(newTags)
	// Adjust for overwrites
	for k := range newTags {
		if _, exists := existing[k]; exists {
			total--
		}
	}

	if total > maxTagCount {
		return fmt.Errorf("%w: tag count would exceed %d", ErrValidation, maxTagCount)
	}

	for k, v := range newTags {
		if strings.HasPrefix(k, "aws:") {
			return fmt.Errorf("%w: tag key %q uses reserved prefix \"aws:\"", ErrValidation, k)
		}

		if len(k) > maxKeyLen {
			return fmt.Errorf("%w: tag key %q exceeds maximum length %d", ErrValidation, k, maxKeyLen)
		}

		if len(v) > maxValLen {
			return fmt.Errorf("%w: tag value for key %q exceeds maximum length %d", ErrValidation, k, maxValLen)
		}
	}

	return nil
}

// parseCedarStatement validates a Cedar policy statement using the cedar-go parser.
func parseCedarStatement(statement string) error {
	if _, err := cedar.NewPolicyListFromBytes("policy.cedar", []byte(statement)); err != nil {
		return fmt.Errorf("%w: Cedar syntax error: %w", ErrValidation, err)
	}

	return nil
}

// extractSchemaNamespaces parses a Cedar JSON schema and returns its top-level namespace keys.
func extractSchemaNamespaces(schemaJSON string) []string {
	var top map[string]json.RawMessage

	if err := json.Unmarshal([]byte(schemaJSON), &top); err != nil {
		return []string{}
	}

	ns := collections.SortedKeys(top)

	return ns
}

// buildCedarPolicySet builds a cedar PolicySet from stored STATIC policies in a store.
func (b *InMemoryBackend) buildCedarPolicySet(policyStoreID string) *cedar.PolicySet {
	ps := cedar.NewPolicySet()

	policies := b.policies[policyStoreID]
	for id, p := range policies {
		if p.PolicyType != policyTypeStatic || p.Statement == "" {
			continue
		}

		list, err := cedar.NewPolicyListFromBytes("policy.cedar", []byte(p.Statement))
		if err != nil {
			continue
		}

		for i, pol := range list {
			pid := cedar.PolicyID(fmt.Sprintf("%s_p%d", id, i))
			polCopy := pol

			ps.Add(pid, polCopy)
		}
	}

	return ps
}

// evaluateCedar runs cedar authorization and returns the AuthDecision.
func evaluateCedar(ps *cedar.PolicySet, req AuthorizationRequest) AuthDecision {
	cedarReq := cedar.Request{}

	if req.PrincipalEntityType != "" {
		cedarReq.Principal = cedar.NewEntityUID(
			cedar.EntityType(req.PrincipalEntityType),
			cedar.String(req.PrincipalEntityID),
		)
	}

	if req.ActionType != "" {
		cedarReq.Action = cedar.NewEntityUID(cedar.EntityType(req.ActionType), cedar.String(req.ActionID))
	}

	if req.ResourceEntityType != "" {
		cedarReq.Resource = cedar.NewEntityUID(
			cedar.EntityType(req.ResourceEntityType),
			cedar.String(req.ResourceEntityID),
		)
	}

	decision, diag := cedar.Authorize(ps, nil, cedarReq)

	result := AuthDecision{
		Request:             req,
		DeterminingPolicies: []string{},
		Errors:              []string{},
	}

	if decision == cedar.Allow {
		result.Decision = decisionAllow
	} else {
		result.Decision = decisionDeny
	}

	// Collect determining policy IDs (strip the "_p0" suffix to get original ID).
	for _, r := range diag.Reasons {
		rawID := string(r.PolicyID)
		// Strip the suffix added in buildCedarPolicySet.
		if idx := strings.LastIndex(rawID, "_p"); idx >= 0 {
			rawID = rawID[:idx]
		}

		result.DeterminingPolicies = append(result.DeterminingPolicies, rawID)
	}

	for _, e := range diag.Errors {
		result.Errors = append(result.Errors, e.Message)
	}

	return result
}

// CreatePolicyStore creates a new policy store.
func (b *InMemoryBackend) CreatePolicyStore(
	description string,
	tags map[string]string,
	validationMode, deletionProtection string,
) (*PolicyStore, error) {
	b.mu.Lock("CreatePolicyStore")
	defer b.mu.Unlock()

	merged := make(map[string]string, len(tags))
	maps.Copy(merged, tags)

	if err := validateTagInput(nil, merged); err != nil {
		return nil, err
	}

	id := uuid.NewString()
	now := time.Now()

	if deletionProtection == "" {
		deletionProtection = DeletionProtectionDisabled
	}

	ps := &PolicyStore{
		PolicyStoreID:      id,
		Arn:                policyStoreARN(b.accountID, b.region, id),
		Description:        description,
		CreatedDate:        now,
		LastUpdated:        now,
		Tags:               merged,
		AccountID:          b.accountID,
		Region:             b.region,
		ValidationMode:     validationMode,
		DeletionProtection: deletionProtection,
	}
	b.policyStores[id] = ps
	b.policies[id] = make(map[string]*Policy)
	b.policyTemplates[id] = make(map[string]*PolicyTemplate)
	b.identitySources[id] = make(map[string]*IdentitySource)
	b.arnIndex[ps.Arn] = arnKindPolicyStore + ":" + id

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
func (b *InMemoryBackend) ListPolicyStores(nextToken string, maxResults int) ([]PolicyStore, string) {
	b.mu.RLock("ListPolicyStores")
	defer b.mu.RUnlock()

	out := make([]PolicyStore, 0, len(b.policyStores))
	for _, ps := range b.policyStores {
		out = append(out, *clonePolicyStore(ps))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedDate.After(out[j].CreatedDate)
	})

	return paginate(out, nextToken, maxResults, func(ps PolicyStore) string { return ps.PolicyStoreID })
}

// UpdatePolicyStore updates a policy store.
func (b *InMemoryBackend) UpdatePolicyStore(
	policyStoreID, description, validationMode, deletionProtection string,
) (*PolicyStore, error) {
	b.mu.Lock("UpdatePolicyStore")
	defer b.mu.Unlock()

	ps, ok := b.policyStores[policyStoreID]
	if !ok {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	ps.Description = description
	if validationMode != "" {
		ps.ValidationMode = validationMode
	}

	if deletionProtection != "" {
		ps.DeletionProtection = deletionProtection
	}

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

	if ps.DeletionProtection == DeletionProtectionEnabled {
		return fmt.Errorf("%w: policy store %s has deletion protection enabled", ErrConflict, policyStoreID)
	}

	// Remove ARN index entries for all child resources.
	for policyID := range b.policies[policyStoreID] {
		delete(b.arnIndex, policyARN(b.accountID, policyStoreID, policyID))
	}

	for templateID := range b.policyTemplates[policyStoreID] {
		delete(b.arnIndex, policyTemplateARN(b.accountID, policyStoreID, templateID))
	}

	for sourceID := range b.identitySources[policyStoreID] {
		delete(b.arnIndex, identitySourceARN(b.accountID, policyStoreID, sourceID))
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
func (b *InMemoryBackend) CreatePolicy(policyStoreID string, params CreatePolicyParams) (*Policy, error) {
	b.mu.Lock("CreatePolicy")
	defer b.mu.Unlock()

	_, ok := b.policyStores[policyStoreID]
	if !ok {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	if params.PolicyType == policyTypeStatic {
		if err := parseCedarStatement(params.Statement); err != nil {
			return nil, err
		}
	}

	if params.PolicyType == policyTypeTemplateLinked {
		// Validate the referenced template exists.
		if _, exists := b.policyTemplates[policyStoreID][params.PolicyTemplateID]; !exists {
			return nil, fmt.Errorf(
				"%w: policy template %s not found in policy store %s",
				ErrPolicyTemplateNotFound, params.PolicyTemplateID, policyStoreID,
			)
		}
	}

	id := uuid.NewString()
	now := time.Now()
	p := &Policy{
		PolicyID:            id,
		PolicyStoreID:       policyStoreID,
		PolicyType:          params.PolicyType,
		Statement:           params.Statement,
		Description:         params.Description,
		PolicyTemplateID:    params.PolicyTemplateID,
		PrincipalEntityType: params.PrincipalEntityType,
		PrincipalEntityID:   params.PrincipalEntityID,
		ResourceEntityType:  params.ResourceEntityType,
		ResourceEntityID:    params.ResourceEntityID,
		CreatedDate:         now,
		LastUpdated:         now,
	}
	b.policies[policyStoreID][id] = p
	b.arnIndex[policyARN(b.accountID, policyStoreID, id)] = arnKindPolicy + ":" + policyStoreID + ":" + id

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

// ListPolicies returns policies in a policy store, with optional filter and pagination.
func (b *InMemoryBackend) ListPolicies(
	policyStoreID string,
	filter ListPoliciesFilter,
	nextToken string,
	maxResults int,
) ([]Policy, string, error) {
	b.mu.RLock("ListPolicies")
	defer b.mu.RUnlock()

	policies, ok := b.policies[policyStoreID]
	if !ok {
		return nil, "", fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	out := make([]Policy, 0, len(policies))

	for _, p := range policies {
		if !matchesPolicyFilter(p, filter) {
			continue
		}

		out = append(out, *clonePolicy(p))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedDate.Before(out[j].CreatedDate)
	})

	page, tok := paginate(out, nextToken, maxResults, func(p Policy) string { return p.PolicyID })

	return page, tok, nil
}

// matchesPolicyFilter returns true if the policy matches all non-empty filter fields.
func matchesPolicyFilter(p *Policy, f ListPoliciesFilter) bool {
	if f.PolicyType != "" && p.PolicyType != f.PolicyType {
		return false
	}

	if f.PolicyTemplateID != "" && p.PolicyTemplateID != f.PolicyTemplateID {
		return false
	}

	if f.PrincipalEntityType != "" && p.PrincipalEntityType != f.PrincipalEntityType {
		return false
	}

	if f.PrincipalEntityID != "" && p.PrincipalEntityID != f.PrincipalEntityID {
		return false
	}

	if f.ResourceEntityType != "" && p.ResourceEntityType != f.ResourceEntityType {
		return false
	}

	if f.ResourceEntityID != "" && p.ResourceEntityID != f.ResourceEntityID {
		return false
	}

	return true
}

// UpdatePolicy updates an existing policy.
func (b *InMemoryBackend) UpdatePolicy(policyStoreID, policyID string, params UpdatePolicyParams) (*Policy, error) {
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

	switch p.PolicyType {
	case policyTypeStatic:
		if params.Statement != "" {
			if err := parseCedarStatement(params.Statement); err != nil {
				return nil, err
			}

			p.Statement = params.Statement
		}

		if params.Description != "" {
			p.Description = params.Description
		}
	case policyTypeTemplateLinked:
		// Policy template ID is immutable; only principal/resource bindings may change.
		if params.PrincipalEntityType != "" {
			p.PrincipalEntityType = params.PrincipalEntityType
		}

		if params.PrincipalEntityID != "" {
			p.PrincipalEntityID = params.PrincipalEntityID
		}

		if params.ResourceEntityType != "" {
			p.ResourceEntityType = params.ResourceEntityType
		}

		if params.ResourceEntityID != "" {
			p.ResourceEntityID = params.ResourceEntityID
		}
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

	delete(b.arnIndex, policyARN(b.accountID, policyStoreID, policyID))
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
	b.arnIndex[policyTemplateARN(b.accountID, policyStoreID, id)] = arnKindPolicyTemplate + ":" + policyStoreID + ":" + id

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
func (b *InMemoryBackend) ListPolicyTemplates(
	policyStoreID, nextToken string,
	maxResults int,
) ([]PolicyTemplate, string, error) {
	b.mu.RLock("ListPolicyTemplates")
	defer b.mu.RUnlock()

	templates, ok := b.policyTemplates[policyStoreID]
	if !ok {
		return nil, "", fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	out := make([]PolicyTemplate, 0, len(templates))
	for _, pt := range templates {
		out = append(out, *clonePolicyTemplate(pt))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedDate.Before(out[j].CreatedDate)
	})

	page, tok := paginate(out, nextToken, maxResults, func(pt PolicyTemplate) string { return pt.PolicyTemplateID })

	return page, tok, nil
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

	delete(b.arnIndex, policyTemplateARN(b.accountID, policyStoreID, policyTemplateID))
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

// resolveARN looks up an ARN in the index and returns the resource info tuple.
// Returns (resourceType, policyStoreID) e.g. ("policyStore", "abc").
func (b *InMemoryBackend) resolveARN(resourceARN string) (string, string, bool) {
	val, exists := b.arnIndex[resourceARN]
	if !exists {
		return "", "", false
	}

	parts := strings.SplitN(val, ":", 3) //nolint:mnd // max 3 parts: kind:storeID:resourceID
	switch parts[0] {
	case arnKindPolicyStore:
		return arnKindPolicyStore, parts[1], true
	case arnKindPolicy:
		return arnKindPolicy, parts[1], true
	case arnKindPolicyTemplate:
		return arnKindPolicyTemplate, parts[1], true
	case arnKindIdentitySource:
		return arnKindIdentitySource, parts[1], true
	}

	return "", "", false
}

// TagResource adds or updates tags on a resource identified by its ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	rtype, storeID, ok := b.resolveARN(resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource with ARN %q not found", ErrPolicyStoreNotFound, resourceARN)
	}

	// Only policy stores have a Tags field in the current model; extend for others.
	if rtype == arnKindPolicyStore {
		ps := b.policyStores[storeID]
		if ps == nil {
			return fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, storeID)
		}

		if err := validateTagInput(ps.Tags, tags); err != nil {
			return err
		}

		if ps.Tags == nil {
			ps.Tags = make(map[string]string, len(tags))
		}

		maps.Copy(ps.Tags, tags)
	}

	return nil
}

// UntagResource removes tags from a resource identified by its ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	rtype, storeID, ok := b.resolveARN(resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource with ARN %q not found", ErrPolicyStoreNotFound, resourceARN)
	}

	if rtype == "policyStore" {
		ps := b.policyStores[storeID]
		if ps == nil {
			return fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, storeID)
		}

		for _, k := range tagKeys {
			delete(ps.Tags, k)
		}
	}

	return nil
}

// ListTagsForResource returns the tags for a resource identified by its ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	rtype, storeID, ok := b.resolveARN(resourceARN)
	if !ok {
		return nil, fmt.Errorf("%w: resource with ARN %q not found", ErrPolicyStoreNotFound, resourceARN)
	}

	if rtype == "policyStore" {
		ps := b.policyStores[storeID]
		if ps == nil {
			return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, storeID)
		}

		return maps.Clone(ps.Tags), nil
	}

	return map[string]string{}, nil
}

// BatchGetPolicy retrieves multiple policies in a single request.
func (b *InMemoryBackend) BatchGetPolicy(items []BatchGetPolicyItem) BatchGetPolicyResult {
	// Snapshot needed entries under the lock, then format outside.
	b.mu.RLock("BatchGetPolicy")

	type entry struct {
		policy *Policy
		err    *batchGetPolicyErrorItem
	}

	entries := make([]entry, 0, len(items))

	for _, item := range items {
		policies, ok := b.policies[item.PolicyStoreID]
		if !ok {
			entries = append(entries, entry{err: &batchGetPolicyErrorItem{
				PolicyStoreID: item.PolicyStoreID,
				PolicyID:      item.PolicyID,
				Code:          "POLICY_STORE_NOT_FOUND",
				Message:       fmt.Sprintf("policy store %s not found", item.PolicyStoreID),
			}})

			continue
		}

		p, ok := policies[item.PolicyID]
		if !ok {
			entries = append(entries, entry{err: &batchGetPolicyErrorItem{
				PolicyStoreID: item.PolicyStoreID,
				PolicyID:      item.PolicyID,
				Code:          "POLICY_NOT_FOUND",
				Message:       fmt.Sprintf("policy %s not found", item.PolicyID),
			}})

			continue
		}

		copied := *clonePolicy(p)
		entries = append(entries, entry{policy: &copied})
	}

	b.mu.RUnlock()

	result := BatchGetPolicyResult{
		Results: make([]Policy, 0, len(items)),
		Errors:  make([]batchGetPolicyErrorItem, 0, len(items)),
	}

	for _, e := range entries {
		if e.err != nil {
			result.Errors = append(result.Errors, *e.err)
		} else {
			result.Results = append(result.Results, *e.policy)
		}
	}

	return result
}

const maxBatchRequests = 30

// BatchIsAuthorized evaluates a batch of authorization requests.
func (b *InMemoryBackend) BatchIsAuthorized(
	policyStoreID string,
	requests []AuthorizationRequest,
) ([]AuthDecision, error) {
	if len(requests) > maxBatchRequests {
		return nil, fmt.Errorf(
			"%w: batch size %d exceeds maximum of %d",
			ErrValidation,
			len(requests),
			maxBatchRequests,
		)
	}

	b.mu.RLock("BatchIsAuthorized")

	if _, ok := b.policyStores[policyStoreID]; !ok {
		b.mu.RUnlock()

		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	ps := b.buildCedarPolicySet(policyStoreID)
	b.mu.RUnlock()

	decisions := make([]AuthDecision, 0, len(requests))

	for _, req := range requests {
		decisions = append(decisions, evaluateCedar(ps, req))
	}

	return decisions, nil
}

// BatchIsAuthorizedWithToken evaluates a batch of authorization requests using a token.
func (b *InMemoryBackend) BatchIsAuthorizedWithToken(
	policyStoreID string,
	requests []AuthorizationRequest,
) ([]AuthDecision, error) {
	if len(requests) > maxBatchRequests {
		return nil, fmt.Errorf(
			"%w: batch size %d exceeds maximum of %d",
			ErrValidation,
			len(requests),
			maxBatchRequests,
		)
	}

	b.mu.RLock("BatchIsAuthorizedWithToken")

	if _, ok := b.policyStores[policyStoreID]; !ok {
		b.mu.RUnlock()

		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	ps := b.buildCedarPolicySet(policyStoreID)
	b.mu.RUnlock()

	decisions := make([]AuthDecision, 0, len(requests))

	for _, req := range requests {
		decisions = append(decisions, evaluateCedar(ps, req))
	}

	return decisions, nil
}

// IsAuthorized evaluates a single authorization request against stored Cedar policies.
func (b *InMemoryBackend) IsAuthorized(policyStoreID string, req AuthorizationRequest) (*AuthDecision, error) {
	b.mu.RLock("IsAuthorized")

	if _, ok := b.policyStores[policyStoreID]; !ok {
		b.mu.RUnlock()

		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	ps := b.buildCedarPolicySet(policyStoreID)
	b.mu.RUnlock()

	result := evaluateCedar(ps, req)

	return &result, nil
}

// IsAuthorizedWithToken evaluates a single authorization request using a token.
func (b *InMemoryBackend) IsAuthorizedWithToken(
	policyStoreID string,
	req AuthorizationRequest,
) (*AuthDecision, error) {
	b.mu.RLock("IsAuthorizedWithToken")

	if _, ok := b.policyStores[policyStoreID]; !ok {
		b.mu.RUnlock()

		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	ps := b.buildCedarPolicySet(policyStoreID)
	b.mu.RUnlock()

	result := evaluateCedar(ps, req)

	return &result, nil
}

// CreateIdentitySource creates a new identity source in the given policy store.
func (b *InMemoryBackend) CreateIdentitySource(
	policyStoreID, principalEntityType string,
	cfg IdentitySourceConfig,
) (*IdentitySource, error) {
	b.mu.Lock("CreateIdentitySource")
	defer b.mu.Unlock()

	if _, ok := b.policyStores[policyStoreID]; !ok {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	id := uuid.NewString()
	now := time.Now()

	is := &IdentitySource{
		IdentitySourceID:    id,
		PolicyStoreID:       policyStoreID,
		PrincipalEntityType: principalEntityType,
		CreatedDate:         now,
		LastUpdated:         now,
	}

	applyIdentitySourceConfig(is, cfg)

	if b.identitySources[policyStoreID] == nil {
		b.identitySources[policyStoreID] = make(map[string]*IdentitySource)
	}

	b.identitySources[policyStoreID][id] = is
	b.arnIndex[identitySourceARN(b.accountID, policyStoreID, id)] = arnKindIdentitySource + ":" + policyStoreID + ":" + id

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

	delete(b.arnIndex, identitySourceARN(b.accountID, policyStoreID, identitySourceID))
	delete(sources, identitySourceID)

	return nil
}

// PutSchema creates or replaces the schema for a policy store, extracts namespaces, and returns them.
func (b *InMemoryBackend) PutSchema(policyStoreID, schema string) ([]string, error) {
	b.mu.Lock("PutSchema")
	defer b.mu.Unlock()

	if _, ok := b.policyStores[policyStoreID]; !ok {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	// Validate JSON format.
	if !json.Valid([]byte(schema)) {
		return nil, fmt.Errorf("%w: schema is not valid JSON", ErrValidation)
	}

	namespaces := extractSchemaNamespaces(schema)

	now := time.Now()
	existing, ok := b.schemas[policyStoreID]

	if ok {
		existing.Schema = schema
		existing.LastUpdated = now
		existing.Namespaces = namespaces
	} else {
		b.schemas[policyStoreID] = &PolicyStoreSchema{
			Schema:      schema,
			CreatedDate: now,
			LastUpdated: now,
			Namespaces:  namespaces,
		}
	}

	return namespaces, nil
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
	if len(s.Namespaces) > 0 {
		cp.Namespaces = make([]string, len(s.Namespaces))
		copy(cp.Namespaces, s.Namespaces)
	}

	return &cp, nil
}

// ListIdentitySources returns all identity sources for a policy store sorted by creation date.
func (b *InMemoryBackend) ListIdentitySources(
	policyStoreID, nextToken string,
	maxResults int,
) ([]IdentitySource, string, error) {
	b.mu.RLock("ListIdentitySources")
	defer b.mu.RUnlock()

	if _, ok := b.policyStores[policyStoreID]; !ok {
		return nil, "", fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	sources := b.identitySources[policyStoreID]
	out := make([]IdentitySource, 0, len(sources))

	for _, is := range sources {
		out = append(out, *cloneIdentitySource(is))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedDate.Before(out[j].CreatedDate)
	})

	page, tok := paginate(out, nextToken, maxResults, func(is IdentitySource) string { return is.IdentitySourceID })

	return page, tok, nil
}

// UpdateIdentitySource updates the configuration and principal entity type of an identity source.
func (b *InMemoryBackend) UpdateIdentitySource(
	policyStoreID, identitySourceID, principalEntityType string,
	cfg IdentitySourceConfig,
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

	// Clear old config before applying new one to avoid stale fields.
	is.UserPoolArn = ""
	is.ClientIDs = nil
	is.CognitoGroupConfig = nil
	is.OpenIDIssuer = ""
	is.EntityIDPrefix = ""
	is.OIDCGroupConfig = nil
	is.OIDCTokenSelection = nil

	applyIdentitySourceConfig(is, cfg)

	if principalEntityType != "" {
		is.PrincipalEntityType = principalEntityType
	}

	is.LastUpdated = time.Now()

	return cloneIdentitySource(is), nil
}

// applyIdentitySourceConfig writes cfg fields into is.
func applyIdentitySourceConfig(is *IdentitySource, cfg IdentitySourceConfig) {
	if cfg.UserPoolArn != "" {
		is.UserPoolArn = cfg.UserPoolArn

		cloned := make([]string, len(cfg.ClientIDs))
		copy(cloned, cfg.ClientIDs)
		is.ClientIDs = cloned

		if cfg.CognitoGroupEntityType != "" {
			is.CognitoGroupConfig = &CognitoGroupConfig{GroupEntityType: cfg.CognitoGroupEntityType}
		}
	} else if cfg.Issuer != "" {
		is.OpenIDIssuer = cfg.Issuer
		is.EntityIDPrefix = cfg.EntityIDPrefix

		if cfg.OIDCGroupClaim != "" || cfg.OIDCGroupEntityType != "" {
			is.OIDCGroupConfig = &OIDCGroupConfig{
				GroupClaim:      cfg.OIDCGroupClaim,
				GroupEntityType: cfg.OIDCGroupEntityType,
			}
		}

		if cfg.TokenType != "" || cfg.PrincipalIDClaim != "" || len(cfg.Audiences) > 0 {
			aud := make([]string, len(cfg.Audiences))
			copy(aud, cfg.Audiences)
			is.OIDCTokenSelection = &OIDCTokenSelection{
				TokenType:        cfg.TokenType,
				PrincipalIDClaim: cfg.PrincipalIDClaim,
				Audiences:        aud,
			}
		}
	}
}

// paginate slices items starting after nextToken and up to maxResults.
// Returns the page and the next continuation token (empty string if last page).
func paginate[T any](items []T, nextToken string, maxResults int, idFn func(T) string) ([]T, string) {
	start := 0

	if nextToken != "" {
		for i, item := range items {
			if idFn(item) == nextToken {
				start = i + 1

				break
			}
		}
	}

	items = items[start:]

	if maxResults > 0 && len(items) > maxResults {
		return items[:maxResults], idFn(items[maxResults])
	}

	return items, ""
}

// AddPolicyStoreInternal inserts a pre-built PolicyStore directly into the backend (for test seeding).
func (b *InMemoryBackend) AddPolicyStoreInternal(ps *PolicyStore) {
	b.mu.Lock("AddPolicyStoreInternal")
	defer b.mu.Unlock()

	b.policyStores[ps.PolicyStoreID] = ps
	b.policies[ps.PolicyStoreID] = make(map[string]*Policy)
	b.policyTemplates[ps.PolicyStoreID] = make(map[string]*PolicyTemplate)
	b.identitySources[ps.PolicyStoreID] = make(map[string]*IdentitySource)
	b.arnIndex[ps.Arn] = arnKindPolicyStore + ":" + ps.PolicyStoreID
}

// AddPolicyInternal inserts a pre-built Policy directly into the backend (for test seeding).
func (b *InMemoryBackend) AddPolicyInternal(p *Policy) {
	b.mu.Lock("AddPolicyInternal")
	defer b.mu.Unlock()

	if b.policies[p.PolicyStoreID] == nil {
		b.policies[p.PolicyStoreID] = make(map[string]*Policy)
	}

	b.policies[p.PolicyStoreID][p.PolicyID] = p
	b.arnIndex[policyARN(b.accountID, p.PolicyStoreID, p.PolicyID)] =
		arnKindPolicy + ":" + p.PolicyStoreID + ":" + p.PolicyID
}

// AddPolicyTemplateInternal inserts a pre-built PolicyTemplate directly into the backend (for test seeding).
func (b *InMemoryBackend) AddPolicyTemplateInternal(pt *PolicyTemplate) {
	b.mu.Lock("AddPolicyTemplateInternal")
	defer b.mu.Unlock()

	if b.policyTemplates[pt.PolicyStoreID] == nil {
		b.policyTemplates[pt.PolicyStoreID] = make(map[string]*PolicyTemplate)
	}

	b.policyTemplates[pt.PolicyStoreID][pt.PolicyTemplateID] = pt
	b.arnIndex[policyTemplateARN(b.accountID, pt.PolicyStoreID, pt.PolicyTemplateID)] =
		arnKindPolicyTemplate + ":" + pt.PolicyStoreID + ":" + pt.PolicyTemplateID
}

// AddIdentitySourceInternal inserts a pre-built IdentitySource directly into the backend (for test seeding).
func (b *InMemoryBackend) AddIdentitySourceInternal(is *IdentitySource) {
	b.mu.Lock("AddIdentitySourceInternal")
	defer b.mu.Unlock()

	if b.identitySources[is.PolicyStoreID] == nil {
		b.identitySources[is.PolicyStoreID] = make(map[string]*IdentitySource)
	}

	b.identitySources[is.PolicyStoreID][is.IdentitySourceID] = is
	b.arnIndex[identitySourceARN(b.accountID, is.PolicyStoreID, is.IdentitySourceID)] =
		arnKindIdentitySource + ":" + is.PolicyStoreID + ":" + is.IdentitySourceID
}
