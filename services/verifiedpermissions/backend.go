package verifiedpermissions

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	awsarn "github.com/aws/aws-sdk-go-v2/aws/arn"
	cedar "github.com/cedar-policy/cedar-go"
	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
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
	// ErrTooManyTags is returned when TagResource would push a resource's tag
	// count over the 50-tag limit. Real AWS only declares TooManyTagsException
	// for TagResource -- CreatePolicyStore's tag-count overflow stays a plain
	// ValidationException (ErrValidation), per the SDK's per-op error models.
	ErrTooManyTags = awserr.New("TooManyTagsException", awserr.ErrInvalidParameter)
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
	// policyStoreID is the store.Table primary key (one schema per policy
	// store). It is never part of the wire API, so it carries no json tag
	// and is round-tripped separately through a DTO in persistence.go.
	policyStoreID string
	Namespaces    []string `json:"namespaces,omitempty"`
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

// ListPoliciesFilter holds filter params for ListPolicies. PrincipalUnspecified
// / ResourceUnspecified mirror the wire filter's EntityReference "unspecified"
// variant: when set, only policies with no principal/resource scope match.
type ListPoliciesFilter struct {
	PolicyType           string
	PolicyTemplateID     string
	PrincipalEntityType  string
	PrincipalEntityID    string
	ResourceEntityType   string
	ResourceEntityID     string
	PrincipalUnspecified bool
	ResourceUnspecified  bool
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

// cognitoIssuerFromUserPoolArn derives the OIDC issuer URL that real AWS
// computes for a Cognito-backed identity source from the user pool's ARN
// (arn:aws:cognito-idp:<region>:<account>:userpool/<poolId>), returning
// "https://cognito-idp.<region>.amazonaws.com/<poolId>" -- see the
// CognitoUserPoolConfigurationDetail/Item.Issuer doc in the SDK, which is a
// required response field Verified Permissions always sets even though
// CreateIdentitySource/UpdateIdentitySource callers never provide it
// directly. Returns "" if userPoolArn cannot be parsed as an ARN.
func cognitoIssuerFromUserPoolArn(userPoolArn string) string {
	parsed, err := awsarn.Parse(userPoolArn)
	if err != nil {
		return ""
	}

	poolID := strings.TrimPrefix(parsed.Resource, "userpool/")

	return fmt.Sprintf("https://cognito-idp.%s.amazonaws.com/%s", parsed.Region, poolID)
}

// policyKey, policyTemplateKey, and identitySourceKey build the composite
// store.Table primary key ("policyStoreID/id") shared by the resource tables
// that were previously nested by policy store (see store_setup.go).
func policyKey(policyStoreID, policyID string) string { return policyStoreID + "/" + policyID }

func policyTemplateKey(policyStoreID, policyTemplateID string) string {
	return policyStoreID + "/" + policyTemplateID
}

func identitySourceKey(policyStoreID, identitySourceID string) string {
	return policyStoreID + "/" + identitySourceID
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
//
// policyStores registers directly on b.registry, keyed by its real
// PolicyStoreID field. policies, policyTemplates, and identitySources were
// previously nested by policy store (map[string]map[string]*T); each is now a
// flat *store.Table keyed by the composite "policyStoreID/id" string (see
// policyKey/policyTemplateKey/identitySourceKey), with a companion
// *store.Index grouping entries by policy store for the per-store scans the
// nested maps used to answer directly -- the same pattern services/codeartifact
// uses for its region-nested maps. All three carry real, wire-visible
// PolicyStoreID fields, so each is a "clean" table registered directly on
// b.registry, no DTO wrapper needed (see persistence.go).
//
// schemas has no wire-visible identity field at all (one schema per policy
// store, keyed only by the outer map's policyStoreID), so it gained a hidden
// policyStoreID field purely for this key; it is a "dirty" table (store.New
// only, deliberately NOT store.Register-ed onto b.registry) round-tripped
// through a DTO wrapper in persistence.go.
//
// arnIndex is a derived cache rebuilt from the tables above on Restore, so it
// is never itself persisted. resourceTags, policySetCache, and policySetDirty
// remain plain maps: resourceTags is a non-*T value map (map[string]string)
// still persisted directly; policySetCache/policySetDirty are ephemeral
// caches that are never persisted.
type InMemoryBackend struct {
	registry               *store.Registry
	policyStores           *store.Table[PolicyStore]
	policies               *store.Table[Policy]
	policiesByStore        *store.Index[Policy]
	policyTemplates        *store.Table[PolicyTemplate]
	policyTemplatesByStore *store.Index[PolicyTemplate]
	identitySources        *store.Table[IdentitySource]
	identitySourcesByStore *store.Index[IdentitySource]
	schemas                *store.Table[PolicyStoreSchema]
	// arnIndex maps ARN -> (resourceType, policyStoreID, resourceID) for O(1) tag ops
	// values are encoded as "policyStore:<id>", "policy:<storeID>:<policyID>", etc.
	arnIndex     map[string]string
	resourceTags map[string]map[string]string // ARN -> tags (all resource types)
	// policySetCache caches the compiled Cedar PolicySet per policy store.
	// Invalidated by CreatePolicy/UpdatePolicy/DeletePolicy.
	policySetCache map[string]*cedar.PolicySet
	policySetDirty map[string]bool
	mu             *lockmetrics.RWMutex
	accountID      string
	region         string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:       store.NewRegistry(),
		arnIndex:       make(map[string]string),
		resourceTags:   make(map[string]map[string]string),
		policySetCache: make(map[string]*cedar.PolicySet),
		policySetDirty: make(map[string]bool),
		accountID:      accountID,
		region:         region,
		mu:             lockmetrics.New("verifiedpermissions"),
	}

	registerAllTables(b)

	return b
}

// AccountID returns the AWS account ID configured for this backend.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// validateTagInput checks tag count, key/value length, and reserved prefix
// constraints. tooManyErr is the sentinel returned when the tag count would
// be exceeded: real AWS only declares TooManyTagsException as a possible
// TagResource error (CreatePolicyStore's tag-count overflow is a plain
// ValidationException), so callers pass the sentinel matching their op.
func validateTagInput(existing map[string]string, newTags map[string]string, tooManyErr error) error {
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
		return fmt.Errorf("%w: tag count would exceed %d", tooManyErr, maxTagCount)
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

// buildCedarPolicySet returns the Cedar PolicySet for a store, rebuilding only when dirty.
// Caller must hold at least a read lock; when dirty, caller must hold a write lock to
// promote the cache entry.  In practice all auth callers drop the read lock before calling
// this, then re-acquire as a write lock when dirty — but since we are under a single
// read lock here, we use a simple always-rebuild-when-dirty approach: build outside the
// lock and let the caller store the result.  The returned value is safe to use after the
// lock is released because it is immutable once built.
func (b *InMemoryBackend) buildCedarPolicySet(policyStoreID string) *cedar.PolicySet {
	if !b.policySetDirty[policyStoreID] {
		if cached, ok := b.policySetCache[policyStoreID]; ok {
			return cached
		}
	}

	ps := cedar.NewPolicySet()

	policies := b.policiesByStore.Get(policyStoreID)
	for _, p := range policies {
		if p.PolicyType != policyTypeStatic || p.Statement == "" {
			continue
		}

		list, err := cedar.NewPolicyListFromBytes("policy.cedar", []byte(p.Statement))
		if err != nil {
			continue
		}

		for i, pol := range list {
			pid := cedar.PolicyID(fmt.Sprintf("%s_p%d", p.PolicyID, i))
			polCopy := pol

			ps.Add(pid, polCopy)
		}
	}

	b.policySetCache[policyStoreID] = ps
	b.policySetDirty[policyStoreID] = false

	return ps
}

// invalidatePolicySetCache marks the compiled Cedar policy set for policyStoreID as dirty.
// Must be called under the write lock whenever STATIC policies change.
func (b *InMemoryBackend) invalidatePolicySetCache(policyStoreID string) {
	b.policySetDirty[policyStoreID] = true
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

	if err := validateTagInput(nil, merged, ErrValidation); err != nil {
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
	b.policyStores.Put(ps)
	b.arnIndex[ps.Arn] = arnKindPolicyStore + ":" + id
	if len(merged) > 0 {
		b.resourceTags[ps.Arn] = maps.Clone(merged)
	}

	return clonePolicyStore(ps), nil
}

// GetPolicyStore returns the policy store with the given ID.
func (b *InMemoryBackend) GetPolicyStore(policyStoreID string) (*PolicyStore, error) {
	b.mu.RLock("GetPolicyStore")
	defer b.mu.RUnlock()

	ps, ok := b.policyStores.Get(policyStoreID)
	if !ok {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	return clonePolicyStore(ps), nil
}

// ListPolicyStores returns all policy stores sorted by creation date (newest first).
func (b *InMemoryBackend) ListPolicyStores(nextToken string, maxResults int) ([]PolicyStore, string) {
	b.mu.RLock("ListPolicyStores")
	defer b.mu.RUnlock()

	all := b.policyStores.All()
	out := make([]PolicyStore, 0, len(all))
	for _, ps := range all {
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

	ps, ok := b.policyStores.Get(policyStoreID)
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

	ps, ok := b.policyStores.Get(policyStoreID)
	if !ok {
		return fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	if ps.DeletionProtection == DeletionProtectionEnabled {
		return fmt.Errorf("%w: policy store %s has deletion protection enabled", ErrConflict, policyStoreID)
	}

	// Remove ARN index entries for all child resources, then delete the
	// child resources themselves. Index result slices mutate under Delete,
	// so clone before the delete loop.
	for _, p := range slices.Clone(b.policiesByStore.Get(policyStoreID)) {
		delete(b.arnIndex, policyARN(b.accountID, policyStoreID, p.PolicyID))
		b.policies.Delete(policyKey(policyStoreID, p.PolicyID))
	}

	for _, pt := range slices.Clone(b.policyTemplatesByStore.Get(policyStoreID)) {
		delete(b.arnIndex, policyTemplateARN(b.accountID, policyStoreID, pt.PolicyTemplateID))
		b.policyTemplates.Delete(policyTemplateKey(policyStoreID, pt.PolicyTemplateID))
	}

	for _, is := range slices.Clone(b.identitySourcesByStore.Get(policyStoreID)) {
		delete(b.arnIndex, identitySourceARN(b.accountID, policyStoreID, is.IdentitySourceID))
		b.identitySources.Delete(identitySourceKey(policyStoreID, is.IdentitySourceID))
	}

	delete(b.arnIndex, ps.Arn)
	b.policyStores.Delete(policyStoreID)
	b.schemas.Delete(policyStoreID)

	return nil
}

// CreatePolicy creates a new policy in the given policy store.
func (b *InMemoryBackend) CreatePolicy(policyStoreID string, params CreatePolicyParams) (*Policy, error) {
	b.mu.Lock("CreatePolicy")
	defer b.mu.Unlock()

	if !b.policyStores.Has(policyStoreID) {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	if params.PolicyType == policyTypeStatic {
		if err := parseCedarStatement(params.Statement); err != nil {
			return nil, err
		}
	}

	if params.PolicyType == policyTypeTemplateLinked {
		// Validate the referenced template exists.
		if !b.policyTemplates.Has(policyTemplateKey(policyStoreID, params.PolicyTemplateID)) {
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
	b.policies.Put(p)
	b.arnIndex[policyARN(b.accountID, policyStoreID, id)] = arnKindPolicy + ":" + policyStoreID + ":" + id
	b.invalidatePolicySetCache(policyStoreID)

	return clonePolicy(p), nil
}

// GetPolicy returns the policy with the given ID.
func (b *InMemoryBackend) GetPolicy(policyStoreID, policyID string) (*Policy, error) {
	b.mu.RLock("GetPolicy")
	defer b.mu.RUnlock()

	if !b.policyStores.Has(policyStoreID) {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	p, ok := b.policies.Get(policyKey(policyStoreID, policyID))
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

	if !b.policyStores.Has(policyStoreID) {
		return nil, "", fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	policies := b.policiesByStore.Get(policyStoreID)
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

	return matchesPrincipalFilter(p, f) && matchesResourceFilter(p, f)
}

// matchesPrincipalFilter checks the principal-scope portion of f.
func matchesPrincipalFilter(p *Policy, f ListPoliciesFilter) bool {
	if f.PrincipalUnspecified && (p.PrincipalEntityType != "" || p.PrincipalEntityID != "") {
		return false
	}

	if f.PrincipalEntityType != "" && p.PrincipalEntityType != f.PrincipalEntityType {
		return false
	}

	if f.PrincipalEntityID != "" && p.PrincipalEntityID != f.PrincipalEntityID {
		return false
	}

	return true
}

// matchesResourceFilter checks the resource-scope portion of f.
func matchesResourceFilter(p *Policy, f ListPoliciesFilter) bool {
	if f.ResourceUnspecified && (p.ResourceEntityType != "" || p.ResourceEntityID != "") {
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

	if !b.policyStores.Has(policyStoreID) {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	p, ok := b.policies.Get(policyKey(policyStoreID, policyID))
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
	b.invalidatePolicySetCache(policyStoreID)

	return clonePolicy(p), nil
}

// DeletePolicy removes a policy from the given policy store.
func (b *InMemoryBackend) DeletePolicy(policyStoreID, policyID string) error {
	b.mu.Lock("DeletePolicy")
	defer b.mu.Unlock()

	if !b.policyStores.Has(policyStoreID) {
		return fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	if !b.policies.Has(policyKey(policyStoreID, policyID)) {
		return fmt.Errorf("%w: policy %s not found", ErrPolicyNotFound, policyID)
	}

	delete(b.arnIndex, policyARN(b.accountID, policyStoreID, policyID))
	b.policies.Delete(policyKey(policyStoreID, policyID))
	b.invalidatePolicySetCache(policyStoreID)

	return nil
}

// CreatePolicyTemplate creates a new policy template in the given policy store.
func (b *InMemoryBackend) CreatePolicyTemplate(policyStoreID, description, statement string) (*PolicyTemplate, error) {
	b.mu.Lock("CreatePolicyTemplate")
	defer b.mu.Unlock()

	if !b.policyStores.Has(policyStoreID) {
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
	b.policyTemplates.Put(pt)
	b.arnIndex[policyTemplateARN(b.accountID, policyStoreID, id)] = arnKindPolicyTemplate + ":" + policyStoreID + ":" + id

	return clonePolicyTemplate(pt), nil
}

// GetPolicyTemplate returns the policy template with the given ID.
func (b *InMemoryBackend) GetPolicyTemplate(policyStoreID, policyTemplateID string) (*PolicyTemplate, error) {
	b.mu.RLock("GetPolicyTemplate")
	defer b.mu.RUnlock()

	if !b.policyStores.Has(policyStoreID) {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	pt, ok := b.policyTemplates.Get(policyTemplateKey(policyStoreID, policyTemplateID))
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

	if !b.policyStores.Has(policyStoreID) {
		return nil, "", fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	templates := b.policyTemplatesByStore.Get(policyStoreID)
	page, tok := listByPolicyStore(templates, nextToken, maxResults,
		func(pt *PolicyTemplate) PolicyTemplate { return *clonePolicyTemplate(pt) },
		func(pt PolicyTemplate) time.Time { return pt.CreatedDate },
		func(pt PolicyTemplate) string { return pt.PolicyTemplateID },
	)

	return page, tok, nil
}

// UpdatePolicyTemplate updates the description and statement of a policy template.
func (b *InMemoryBackend) UpdatePolicyTemplate(
	policyStoreID, policyTemplateID, description, statement string,
) (*PolicyTemplate, error) {
	b.mu.Lock("UpdatePolicyTemplate")
	defer b.mu.Unlock()

	if !b.policyStores.Has(policyStoreID) {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	pt, ok := b.policyTemplates.Get(policyTemplateKey(policyStoreID, policyTemplateID))
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

	if !b.policyStores.Has(policyStoreID) {
		return fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	if !b.policyTemplates.Has(policyTemplateKey(policyStoreID, policyTemplateID)) {
		return fmt.Errorf("%w: policy template %s not found", ErrPolicyTemplateNotFound, policyTemplateID)
	}

	delete(b.arnIndex, policyTemplateARN(b.accountID, policyStoreID, policyTemplateID))
	b.policyTemplates.Delete(policyTemplateKey(policyStoreID, policyTemplateID))

	return nil
}

// Reset clears all policy store state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	// schemas is a "dirty" table (hidden policyStoreID field) deliberately
	// NOT on b.registry -- see store_setup.go's registerAllTables doc -- so
	// it needs its own Reset() call here.
	b.schemas.Reset()
	b.arnIndex = make(map[string]string)
	b.resourceTags = make(map[string]map[string]string)
	b.policySetCache = make(map[string]*cedar.PolicySet)
	b.policySetDirty = make(map[string]bool)
}

// resolveARN reports whether resourceARN is a known registered resource ARN.
func (b *InMemoryBackend) resolveARN(resourceARN string) bool {
	_, exists := b.arnIndex[resourceARN]

	return exists
}

// TagResource adds or updates tags on a resource identified by its ARN.
// Supports policy stores, policies, policy templates, and identity sources.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	ok := b.resolveARN(resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource with ARN %q not found", ErrPolicyStoreNotFound, resourceARN)
	}

	existing := b.resourceTags[resourceARN]
	if existing == nil {
		existing = make(map[string]string)
	}

	if err := validateTagInput(existing, tags, ErrTooManyTags); err != nil {
		return err
	}

	if b.resourceTags[resourceARN] == nil {
		b.resourceTags[resourceARN] = make(map[string]string, len(tags))
	}

	maps.Copy(b.resourceTags[resourceARN], tags)

	return nil
}

// UntagResource removes tags from a resource identified by its ARN.
// Supports policy stores, policies, policy templates, and identity sources.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	ok := b.resolveARN(resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource with ARN %q not found", ErrPolicyStoreNotFound, resourceARN)
	}

	for _, k := range tagKeys {
		delete(b.resourceTags[resourceARN], k)
	}

	return nil
}

// ListTagsForResource returns the tags for a resource identified by its ARN.
// Supports policy stores, policies, policy templates, and identity sources.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	ok := b.resolveARN(resourceARN)
	if !ok {
		return nil, fmt.Errorf("%w: resource with ARN %q not found", ErrPolicyStoreNotFound, resourceARN)
	}

	return maps.Clone(b.resourceTags[resourceARN]), nil
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
		if !b.policyStores.Has(item.PolicyStoreID) {
			entries = append(entries, entry{err: &batchGetPolicyErrorItem{
				PolicyStoreID: item.PolicyStoreID,
				PolicyID:      item.PolicyID,
				Code:          "POLICY_STORE_NOT_FOUND",
				Message:       fmt.Sprintf("policy store %s not found", item.PolicyStoreID),
			}})

			continue
		}

		p, ok := b.policies.Get(policyKey(item.PolicyStoreID, item.PolicyID))
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

	b.mu.Lock("BatchIsAuthorized")

	if !b.policyStores.Has(policyStoreID) {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	ps := b.buildCedarPolicySet(policyStoreID)
	b.mu.Unlock()

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

	b.mu.Lock("BatchIsAuthorizedWithToken")

	if !b.policyStores.Has(policyStoreID) {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	ps := b.buildCedarPolicySet(policyStoreID)
	b.mu.Unlock()

	decisions := make([]AuthDecision, 0, len(requests))

	for _, req := range requests {
		decisions = append(decisions, evaluateCedar(ps, req))
	}

	return decisions, nil
}

// IsAuthorized evaluates a single authorization request against stored Cedar policies.
func (b *InMemoryBackend) IsAuthorized(policyStoreID string, req AuthorizationRequest) (*AuthDecision, error) {
	b.mu.Lock("IsAuthorized")

	if !b.policyStores.Has(policyStoreID) {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	ps := b.buildCedarPolicySet(policyStoreID)
	b.mu.Unlock()

	result := evaluateCedar(ps, req)

	return &result, nil
}

// IsAuthorizedWithToken evaluates a single authorization request using a token.
func (b *InMemoryBackend) IsAuthorizedWithToken(
	policyStoreID string,
	req AuthorizationRequest,
) (*AuthDecision, error) {
	b.mu.Lock("IsAuthorizedWithToken")

	if !b.policyStores.Has(policyStoreID) {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	ps := b.buildCedarPolicySet(policyStoreID)
	b.mu.Unlock()

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

	if !b.policyStores.Has(policyStoreID) {
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

	b.identitySources.Put(is)
	b.arnIndex[identitySourceARN(b.accountID, policyStoreID, id)] = arnKindIdentitySource + ":" + policyStoreID + ":" + id

	return cloneIdentitySource(is), nil
}

// GetIdentitySource returns the identity source with the given ID.
func (b *InMemoryBackend) GetIdentitySource(policyStoreID, identitySourceID string) (*IdentitySource, error) {
	b.mu.RLock("GetIdentitySource")
	defer b.mu.RUnlock()

	if !b.policyStores.Has(policyStoreID) {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	is, ok := b.identitySources.Get(identitySourceKey(policyStoreID, identitySourceID))
	if !ok {
		return nil, fmt.Errorf("%w: identity source %s not found", ErrIdentitySourceNotFound, identitySourceID)
	}

	return cloneIdentitySource(is), nil
}

// DeleteIdentitySource removes an identity source from the given policy store.
func (b *InMemoryBackend) DeleteIdentitySource(policyStoreID, identitySourceID string) error {
	b.mu.Lock("DeleteIdentitySource")
	defer b.mu.Unlock()

	if !b.policyStores.Has(policyStoreID) {
		return fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	if !b.identitySources.Has(identitySourceKey(policyStoreID, identitySourceID)) {
		return fmt.Errorf("%w: identity source %s not found", ErrIdentitySourceNotFound, identitySourceID)
	}

	delete(b.arnIndex, identitySourceARN(b.accountID, policyStoreID, identitySourceID))
	b.identitySources.Delete(identitySourceKey(policyStoreID, identitySourceID))

	return nil
}

// PutSchema creates or replaces the schema for a policy store, extracts namespaces, and returns them.
func (b *InMemoryBackend) PutSchema(policyStoreID, schema string) ([]string, error) {
	b.mu.Lock("PutSchema")
	defer b.mu.Unlock()

	if !b.policyStores.Has(policyStoreID) {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	// Validate JSON format.
	if !json.Valid([]byte(schema)) {
		return nil, fmt.Errorf("%w: schema is not valid JSON", ErrValidation)
	}

	namespaces := extractSchemaNamespaces(schema)

	now := time.Now()
	existing, ok := b.schemas.Get(policyStoreID)

	if ok {
		existing.Schema = schema
		existing.LastUpdated = now
		existing.Namespaces = namespaces
	} else {
		b.schemas.Put(&PolicyStoreSchema{
			Schema:        schema,
			CreatedDate:   now,
			LastUpdated:   now,
			Namespaces:    namespaces,
			policyStoreID: policyStoreID,
		})
	}

	return namespaces, nil
}

// GetSchema returns the schema for a policy store.
func (b *InMemoryBackend) GetSchema(policyStoreID string) (*PolicyStoreSchema, error) {
	b.mu.RLock("GetSchema")
	defer b.mu.RUnlock()

	if !b.policyStores.Has(policyStoreID) {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	s, ok := b.schemas.Get(policyStoreID)
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

// ListIdentitySources returns all identity sources for a policy store sorted
// by creation date. principalEntityTypes mirrors the wire "filters" list
// (each element's principalEntityType); when non-empty, only identity
// sources whose PrincipalEntityType matches one of them are returned (an OR
// across filters, matching AWS's ListIdentitySourcesInput.Filters semantics).
func (b *InMemoryBackend) ListIdentitySources(
	policyStoreID, nextToken string,
	maxResults int,
	principalEntityTypes []string,
) ([]IdentitySource, string, error) {
	b.mu.RLock("ListIdentitySources")
	defer b.mu.RUnlock()

	if !b.policyStores.Has(policyStoreID) {
		return nil, "", fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	sources := b.identitySourcesByStore.Get(policyStoreID)
	if len(principalEntityTypes) > 0 {
		allowed := make(map[string]bool, len(principalEntityTypes))
		for _, t := range principalEntityTypes {
			allowed[t] = true
		}

		filtered := make([]*IdentitySource, 0, len(sources))

		for _, is := range sources {
			if allowed[is.PrincipalEntityType] {
				filtered = append(filtered, is)
			}
		}

		sources = filtered
	}

	page, tok := listByPolicyStore(sources, nextToken, maxResults,
		func(is *IdentitySource) IdentitySource { return *cloneIdentitySource(is) },
		func(is IdentitySource) time.Time { return is.CreatedDate },
		func(is IdentitySource) string { return is.IdentitySourceID },
	)

	return page, tok, nil
}

// UpdateIdentitySource updates the configuration and principal entity type of an identity source.
func (b *InMemoryBackend) UpdateIdentitySource(
	policyStoreID, identitySourceID, principalEntityType string,
	cfg IdentitySourceConfig,
) (*IdentitySource, error) {
	b.mu.Lock("UpdateIdentitySource")
	defer b.mu.Unlock()

	if !b.policyStores.Has(policyStoreID) {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	is, ok := b.identitySources.Get(identitySourceKey(policyStoreID, identitySourceID))
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

// listByPolicyStore clones every entry in a policy-store-scoped store.Index
// group via cloneFn, sorts the clones by createdAtFn ascending, and paginates
// the result via idFn. It factors out the identical clone/sort/paginate shape
// shared by ListPolicyTemplates and ListIdentitySources (both: look up the
// index group for a policy store, clone into a value slice, sort by creation
// date, paginate by the resource's own ID) so the two call sites differ only
// in the type-specific clone/field accessors passed in.
func listByPolicyStore[V any](
	entries []*V,
	nextToken string,
	maxResults int,
	cloneFn func(*V) V,
	createdAtFn func(V) time.Time,
	idFn func(V) string,
) ([]V, string) {
	out := make([]V, 0, len(entries))
	for _, v := range entries {
		out = append(out, cloneFn(v))
	}

	sort.Slice(out, func(i, j int) bool {
		return createdAtFn(out[i]).Before(createdAtFn(out[j]))
	})

	return paginate(out, nextToken, maxResults, idFn)
}

// paginate slices items starting after nextToken and up to maxResults.
// Returns the page and the next continuation token (empty string if last page).
// Tokens are base64-encoded resource IDs to prevent clients from relying on their format.
func paginate[T any](items []T, nextToken string, maxResults int, idFn func(T) string) ([]T, string) {
	start := 0

	if nextToken != "" {
		rawID := decodePageToken(nextToken)

		for i, item := range items {
			if idFn(item) == rawID {
				start = i + 1

				break
			}
		}
	}

	items = items[start:]

	if maxResults > 0 && len(items) > maxResults {
		return items[:maxResults], encodePageToken(idFn(items[maxResults-1]))
	}

	return items, ""
}

// encodePageToken base64-encodes an ID to produce an opaque pagination token.
func encodePageToken(id string) string {
	return base64.StdEncoding.EncodeToString([]byte(id))
}

// decodePageToken reverses encodePageToken; returns the raw ID or the token itself on failure.
func decodePageToken(token string) string {
	b, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return token
	}

	return string(b)
}

// AddPolicyStoreInternal inserts a pre-built PolicyStore directly into the backend (for test seeding).
func (b *InMemoryBackend) AddPolicyStoreInternal(ps *PolicyStore) {
	b.mu.Lock("AddPolicyStoreInternal")
	defer b.mu.Unlock()

	b.policyStores.Put(ps)
	b.arnIndex[ps.Arn] = arnKindPolicyStore + ":" + ps.PolicyStoreID
}

// AddPolicyInternal inserts a pre-built Policy directly into the backend (for test seeding).
func (b *InMemoryBackend) AddPolicyInternal(p *Policy) {
	b.mu.Lock("AddPolicyInternal")
	defer b.mu.Unlock()

	b.policies.Put(p)
	b.arnIndex[policyARN(b.accountID, p.PolicyStoreID, p.PolicyID)] =
		arnKindPolicy + ":" + p.PolicyStoreID + ":" + p.PolicyID
}

// AddPolicyTemplateInternal inserts a pre-built PolicyTemplate directly into the backend (for test seeding).
func (b *InMemoryBackend) AddPolicyTemplateInternal(pt *PolicyTemplate) {
	b.mu.Lock("AddPolicyTemplateInternal")
	defer b.mu.Unlock()

	b.policyTemplates.Put(pt)
	b.arnIndex[policyTemplateARN(b.accountID, pt.PolicyStoreID, pt.PolicyTemplateID)] =
		arnKindPolicyTemplate + ":" + pt.PolicyStoreID + ":" + pt.PolicyTemplateID
}

// AddIdentitySourceInternal inserts a pre-built IdentitySource directly into the backend (for test seeding).
func (b *InMemoryBackend) AddIdentitySourceInternal(is *IdentitySource) {
	b.mu.Lock("AddIdentitySourceInternal")
	defer b.mu.Unlock()

	b.identitySources.Put(is)
	b.arnIndex[identitySourceARN(b.accountID, is.PolicyStoreID, is.IdentitySourceID)] =
		arnKindIdentitySource + ":" + is.PolicyStoreID + ":" + is.IdentitySourceID
}
