package cloudfront

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrNotFound is returned when a requested distribution does not exist.
	ErrNotFound = awserr.New("NoSuchDistribution", awserr.ErrNotFound)
	// ErrOAINotFound is returned when a requested OAI does not exist.
	ErrOAINotFound = awserr.New("NoSuchCloudFrontOriginAccessIdentity", awserr.ErrNotFound)
	// ErrCachePolicyNotFound is returned when a requested cache policy does not exist.
	ErrCachePolicyNotFound = awserr.New("NoSuchCachePolicy", awserr.ErrNotFound)
	// ErrAnycastIPListNotFound is returned when a requested anycast IP list does not exist.
	ErrAnycastIPListNotFound = awserr.New("NoSuchAnycastIPList", awserr.ErrNotFound)
	// ErrConnectionFunctionNotFound is returned when a connection function does not exist.
	ErrConnectionFunctionNotFound = awserr.New("NoSuchConnectionFunction", awserr.ErrNotFound)
	// ErrConnectionGroupNotFound is returned when a connection group does not exist.
	ErrConnectionGroupNotFound = awserr.New("NoSuchConnectionGroup", awserr.ErrNotFound)
	// ErrContinuousDeploymentPolicyNotFound is returned when a continuous deployment policy does not exist.
	ErrContinuousDeploymentPolicyNotFound = awserr.New("NoSuchContinuousDeploymentPolicy", awserr.ErrNotFound)
	// ErrInvalidationNotFound is returned when a requested invalidation does not exist.
	ErrInvalidationNotFound = awserr.New("NoSuchInvalidation", awserr.ErrNotFound)
	// ErrOACNotFound is returned when a requested origin access control does not exist.
	ErrOACNotFound = awserr.New("NoSuchOriginAccessControl", awserr.ErrNotFound)
	// ErrResponseHeadersPolicyNotFound is returned when a requested response headers policy does not exist.
	ErrResponseHeadersPolicyNotFound = awserr.New("NoSuchResponseHeadersPolicy", awserr.ErrNotFound)
	// ErrFunctionNotFound is returned when a requested CloudFront function does not exist.
	ErrFunctionNotFound = awserr.New("NoSuchFunctionExists", awserr.ErrNotFound)
	// ErrOriginRequestPolicyNotFound is returned when a requested origin request policy does not exist.
	ErrOriginRequestPolicyNotFound = awserr.New("NoSuchOriginRequestPolicy", awserr.ErrNotFound)
	// ErrValidation is returned when request parameters fail validation.
	ErrValidation = awserr.New("InvalidArgument", awserr.ErrInvalidParameter)
	// ErrAlreadyExists is returned when a resource with the same identifier already exists.
	ErrAlreadyExists = awserr.New("DistributionAlreadyExists", awserr.ErrAlreadyExists)
)

const (
	// idChars are the uppercase alphanumeric characters used for CloudFront IDs.
	idChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	// idLen is the length of generated CloudFront IDs.
	idLen = 14
)

// generateID generates a random uppercase alphanumeric ID of length 14.
func generateID() string {
	b := make([]byte, idLen)
	for i := range b {
		b[i] = idChars[rand.IntN(len(idChars))] //nolint:gosec // mock service, not security-sensitive
	}

	return string(b)
}

// Distribution represents a CloudFront distribution.
type Distribution struct {
	Tags            map[string]string `json:"tags,omitempty"`
	ID              string            `json:"id"`
	ARN             string            `json:"arn"`
	DomainName      string            `json:"domainName"`
	Status          string            `json:"status"`
	ETag            string            `json:"eTag"`
	CallerReference string            `json:"callerReference"`
	Comment         string            `json:"comment,omitempty"`
	RawConfig       []byte            `json:"rawConfig,omitempty"` // raw DistributionConfig XML from request
	Enabled         bool              `json:"enabled"`
}

// OriginAccessIdentity represents a CloudFront Origin Access Identity.
type OriginAccessIdentity struct {
	ID                string `json:"id"`
	ARN               string `json:"arn"`
	S3CanonicalUserID string `json:"s3CanonicalUserId"`
	ETag              string `json:"eTag"`
	CallerReference   string `json:"callerReference"`
	Comment           string `json:"comment,omitempty"`
}

// Invalidation represents a CloudFront cache invalidation.
type Invalidation struct {
	CreateTime time.Time `json:"createTime"`
	ID         string    `json:"id"`
	Status     string    `json:"status"`
	CallerRef  string    `json:"callerRef,omitempty"`
	Paths      []string  `json:"paths,omitempty"`
}

// AnycastIPList represents a CloudFront Anycast IP list.
type AnycastIPList struct {
	ID      string `json:"id"`
	ARN     string `json:"arn"`
	Name    string `json:"name"`
	Status  string `json:"status"`
	IPCount int32  `json:"ipCount"`
}

// CachePolicy represents a CloudFront cache policy.
type CachePolicy struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Comment    string `json:"comment,omitempty"`
	DefaultTTL int64  `json:"defaultTtl"`
	MaxTTL     int64  `json:"maxTtl"`
	MinTTL     int64  `json:"minTtl"`
}

// ConnectionFunction represents a CloudFront connection function.
type ConnectionFunction struct {
	ARN     string `json:"arn"`
	Name    string `json:"name"`
	Comment string `json:"comment,omitempty"`
}

// ConnectionGroup represents a CloudFront connection group.
type ConnectionGroup struct {
	ID      string `json:"id"`
	ARN     string `json:"arn"`
	Name    string `json:"name"`
	Comment string `json:"comment,omitempty"`
}

// ContinuousDeploymentPolicy represents a CloudFront continuous deployment policy.
type ContinuousDeploymentPolicy struct {
	ID      string `json:"id"`
	Enabled bool   `json:"enabled"`
}

// OriginAccessControl represents a CloudFront Origin Access Control (OAC).
type OriginAccessControl struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	Description     string `json:"description,omitempty"`
	OriginType      string `json:"originType"`
	SigningBehavior string `json:"signingBehavior"`
	SigningProtocol string `json:"signingProtocol"`
	ETag            string `json:"eTag"`
}

// ResponseHeadersPolicy represents a CloudFront Response Headers Policy.
type ResponseHeadersPolicy struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Comment string `json:"comment,omitempty"`
	ETag    string `json:"eTag"`
}

// Function represents a CloudFront Function.
type Function struct {
	Name         string `json:"name"`
	Comment      string `json:"comment,omitempty"`
	Runtime      string `json:"runtime"`
	FunctionCode string `json:"functionCode"`
	Status       string `json:"status"` // UNPUBLISHED or LIVE
	ETag         string `json:"eTag"`
	ARN          string `json:"arn"`
}

// OriginRequestPolicy represents a CloudFront Origin Request Policy.
type OriginRequestPolicy struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Comment string `json:"comment,omitempty"`
	ETag    string `json:"eTag"`
}

// InMemoryBackend stores CloudFront resources in memory.
type InMemoryBackend struct {
	distributions                map[string]*Distribution
	distributionARNs             map[string]string          // ARN → distribution ID (O(1) tag lookups)
	distributionCallerRefs       map[string]string          // CallerReference → distribution ID (idempotency)
	distributionAliases          map[string][]string        // distribution ID → aliases
	distributionWebACLs          map[string]string          // distribution ID → web ACL ID
	distributionTenantWebACLs    map[string]string          // tenant ID → web ACL ID
	invalidations                map[string][]*Invalidation // distribution ID → []Invalidation
	oais                         map[string]*OriginAccessIdentity
	oaiCallerRefs                map[string]string // CallerReference → OAI ID (idempotency)
	anycastIPLists               map[string]*AnycastIPList
	cachePolicies                map[string]*CachePolicy
	cachePolicyByName            map[string]string // name → policy ID (uniqueness)
	connectionFunctions          map[string]*ConnectionFunction
	connectionGroups             map[string]*ConnectionGroup
	continuousDeploymentPolicies map[string]*ContinuousDeploymentPolicy
	originAccessControls         map[string]*OriginAccessControl
	originAccessControlByName    map[string]string // name → OAC ID (uniqueness)
	responseHeadersPolicies      map[string]*ResponseHeadersPolicy
	responseHeadersPolicyByName  map[string]string    // name → policy ID (uniqueness)
	functions                    map[string]*Function // name → function
	originRequestPolicies        map[string]*OriginRequestPolicy
	originRequestPolicyByName    map[string]string // name → policy ID (uniqueness)
	mu                           *lockmetrics.RWMutex
	accountID                    string
	region                       string
}

// NewInMemoryBackend creates a new in-memory CloudFront backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		distributions:                make(map[string]*Distribution),
		distributionARNs:             make(map[string]string),
		distributionCallerRefs:       make(map[string]string),
		distributionAliases:          make(map[string][]string),
		distributionWebACLs:          make(map[string]string),
		distributionTenantWebACLs:    make(map[string]string),
		invalidations:                make(map[string][]*Invalidation),
		oais:                         make(map[string]*OriginAccessIdentity),
		oaiCallerRefs:                make(map[string]string),
		anycastIPLists:               make(map[string]*AnycastIPList),
		cachePolicies:                make(map[string]*CachePolicy),
		cachePolicyByName:            make(map[string]string),
		connectionFunctions:          make(map[string]*ConnectionFunction),
		connectionGroups:             make(map[string]*ConnectionGroup),
		continuousDeploymentPolicies: make(map[string]*ContinuousDeploymentPolicy),
		originAccessControls:         make(map[string]*OriginAccessControl),
		originAccessControlByName:    make(map[string]string),
		responseHeadersPolicies:      make(map[string]*ResponseHeadersPolicy),
		responseHeadersPolicyByName:  make(map[string]string),
		functions:                    make(map[string]*Function),
		originRequestPolicies:        make(map[string]*OriginRequestPolicy),
		originRequestPolicyByName:    make(map[string]string),
		mu:                           lockmetrics.New("cloudfront"),
		accountID:                    accountID,
		region:                       region,
	}
}

// Reset clears all stored state, returning the backend to a pristine empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.distributions = make(map[string]*Distribution)
	b.distributionARNs = make(map[string]string)
	b.distributionCallerRefs = make(map[string]string)
	b.distributionAliases = make(map[string][]string)
	b.distributionWebACLs = make(map[string]string)
	b.distributionTenantWebACLs = make(map[string]string)
	b.invalidations = make(map[string][]*Invalidation)
	b.oais = make(map[string]*OriginAccessIdentity)
	b.oaiCallerRefs = make(map[string]string)
	b.anycastIPLists = make(map[string]*AnycastIPList)
	b.cachePolicies = make(map[string]*CachePolicy)
	b.cachePolicyByName = make(map[string]string)
	b.connectionFunctions = make(map[string]*ConnectionFunction)
	b.connectionGroups = make(map[string]*ConnectionGroup)
	b.continuousDeploymentPolicies = make(map[string]*ContinuousDeploymentPolicy)
	b.originAccessControls = make(map[string]*OriginAccessControl)
	b.originAccessControlByName = make(map[string]string)
	b.responseHeadersPolicies = make(map[string]*ResponseHeadersPolicy)
	b.responseHeadersPolicyByName = make(map[string]string)
	b.functions = make(map[string]*Function)
	b.originRequestPolicies = make(map[string]*OriginRequestPolicy)
	b.originRequestPolicyByName = make(map[string]string)
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// distributionARN builds an ARN for a CloudFront distribution.
// CloudFront ARNs have no region component.
func (b *InMemoryBackend) distributionARN(id string) string {
	return fmt.Sprintf("arn:aws:cloudfront::%s:distribution/%s", b.accountID, id)
}

// oaiARN builds an ARN for an Origin Access Identity.
func (b *InMemoryBackend) oaiARN(id string) string {
	return fmt.Sprintf("arn:aws:cloudfront::%s:origin-access-identity/cloudfront/%s", b.accountID, id)
}

// anycastIPListARN builds an ARN for an Anycast IP list.
func (b *InMemoryBackend) anycastIPListARN(id string) string {
	return fmt.Sprintf("arn:aws:cloudfront::%s:anycast-ip-list/%s", b.accountID, id)
}

// connectionGroupARN builds an ARN for a connection group.
func (b *InMemoryBackend) connectionGroupARN(id string) string {
	return fmt.Sprintf("arn:aws:cloudfront::%s:connection-group/%s", b.accountID, id)
}

// functionARN builds an ARN for a CloudFront Function.
func (b *InMemoryBackend) functionARN(name string) string {
	return fmt.Sprintf("arn:aws:cloudfront::%s:function/%s", b.accountID, name)
}

// CreateDistribution creates a new CloudFront distribution.
// If a distribution with the same CallerReference already exists, it is returned
// without creating a duplicate (idempotent).
func (b *InMemoryBackend) CreateDistribution(
	callerRef, comment string,
	enabled bool,
	rawConfig []byte,
) (*Distribution, error) {
	b.mu.Lock("CreateDistribution")
	defer b.mu.Unlock()

	if callerRef == "" {
		return nil, fmt.Errorf("%w: CallerReference must not be empty", ErrValidation)
	}

	// Idempotency: return existing distribution for the same CallerReference.
	if existingID, ok := b.distributionCallerRefs[callerRef]; ok {
		return b.copyDistribution(b.distributions[existingID]), nil
	}

	id := generateID()
	d := &Distribution{
		ID:              id,
		ARN:             b.distributionARN(id),
		DomainName:      strings.ToLower(id) + ".cloudfront.net",
		Status:          "Deployed",
		ETag:            uuid.NewString(),
		CallerReference: callerRef,
		Comment:         comment,
		Enabled:         enabled,
		RawConfig:       rawConfig,
		Tags:            make(map[string]string),
	}
	b.distributions[id] = d
	b.distributionARNs[d.ARN] = id
	b.distributionCallerRefs[callerRef] = id
	cp := b.copyDistribution(d)

	return cp, nil
}

// GetDistribution returns a distribution by ID.
func (b *InMemoryBackend) GetDistribution(id string) (*Distribution, error) {
	b.mu.RLock("GetDistribution")
	defer b.mu.RUnlock()

	d, ok := b.distributions[id]
	if !ok {
		return nil, fmt.Errorf("%w: distribution %s not found", ErrNotFound, id)
	}

	return b.copyDistribution(d), nil
}

// UpdateDistribution updates an existing distribution's config.
func (b *InMemoryBackend) UpdateDistribution(
	id, comment string,
	enabled bool,
	rawConfig []byte,
) (*Distribution, error) {
	b.mu.Lock("UpdateDistribution")
	defer b.mu.Unlock()

	d, ok := b.distributions[id]
	if !ok {
		return nil, fmt.Errorf("%w: distribution %s not found", ErrNotFound, id)
	}

	d.Comment = comment
	d.Enabled = enabled
	d.RawConfig = rawConfig
	d.ETag = uuid.NewString()
	cp := b.copyDistribution(d)

	return cp, nil
}

// DeleteDistribution deletes a distribution by ID and cleans up related state.
func (b *InMemoryBackend) DeleteDistribution(id string) error {
	b.mu.Lock("DeleteDistribution")
	defer b.mu.Unlock()

	d, ok := b.distributions[id]
	if !ok {
		return fmt.Errorf("%w: distribution %s not found", ErrNotFound, id)
	}

	delete(b.distributionARNs, b.distributionARN(id))
	delete(b.distributionCallerRefs, d.CallerReference)
	delete(b.distributions, id)
	delete(b.invalidations, id)
	delete(b.distributionAliases, id)
	delete(b.distributionWebACLs, id)

	return nil
}

// ListDistributions returns all distributions sorted by ID.
func (b *InMemoryBackend) ListDistributions() []*Distribution {
	b.mu.RLock("ListDistributions")
	defer b.mu.RUnlock()

	list := make([]*Distribution, 0, len(b.distributions))
	for _, d := range b.distributions {
		list = append(list, b.copyDistribution(d))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// CreateOAI creates a new Origin Access Identity.
// If an OAI with the same CallerReference already exists, it is returned without
// creating a duplicate (idempotent).
func (b *InMemoryBackend) CreateOAI(callerRef, comment string) (*OriginAccessIdentity, error) {
	b.mu.Lock("CreateCloudFrontOriginAccessIdentity")
	defer b.mu.Unlock()

	if callerRef == "" {
		return nil, fmt.Errorf("%w: CallerReference must not be empty", ErrValidation)
	}

	// Idempotency: return existing OAI for the same CallerReference.
	if existingID, ok := b.oaiCallerRefs[callerRef]; ok {
		cp := *b.oais[existingID]

		return &cp, nil
	}

	id := generateID()
	oai := &OriginAccessIdentity{
		ID:                id,
		ARN:               b.oaiARN(id),
		S3CanonicalUserID: uuid.NewString(),
		ETag:              uuid.NewString(),
		CallerReference:   callerRef,
		Comment:           comment,
	}
	b.oais[id] = oai
	b.oaiCallerRefs[callerRef] = id
	cp := *oai

	return &cp, nil
}

// GetOAI returns an OAI by ID.
func (b *InMemoryBackend) GetOAI(id string) (*OriginAccessIdentity, error) {
	b.mu.RLock("GetCloudFrontOriginAccessIdentity")
	defer b.mu.RUnlock()

	oai, ok := b.oais[id]
	if !ok {
		return nil, fmt.Errorf("%w: OAI %s not found", ErrOAINotFound, id)
	}
	cp := *oai

	return &cp, nil
}

// DeleteOAI deletes an OAI by ID.
func (b *InMemoryBackend) DeleteOAI(id string) error {
	b.mu.Lock("DeleteCloudFrontOriginAccessIdentity")
	defer b.mu.Unlock()

	oai, ok := b.oais[id]
	if !ok {
		return fmt.Errorf("%w: OAI %s not found", ErrOAINotFound, id)
	}

	delete(b.oaiCallerRefs, oai.CallerReference)
	delete(b.oais, id)

	return nil
}

// ListOAIs returns all OAIs sorted by ID.
func (b *InMemoryBackend) ListOAIs() []*OriginAccessIdentity {
	b.mu.RLock("ListCloudFrontOriginAccessIdentities")
	defer b.mu.RUnlock()

	list := make([]*OriginAccessIdentity, 0, len(b.oais))
	for _, oai := range b.oais {
		cp := *oai
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// TagResource adds or updates tags on a resource by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	id, ok := b.distributionARNs[resourceARN]
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	d := b.distributions[id]
	if d.Tags == nil {
		d.Tags = make(map[string]string, len(kv))
	}

	maps.Copy(d.Tags, kv)

	return nil
}

// UntagResource removes tags from a resource by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	id, ok := b.distributionARNs[resourceARN]
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	d := b.distributions[id]
	for _, k := range keys {
		delete(d.Tags, k)
	}

	return nil
}

// ListTags returns the tags for a resource by ARN.
func (b *InMemoryBackend) ListTags(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	id, ok := b.distributionARNs[resourceARN]
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	d := b.distributions[id]
	cp := make(map[string]string, len(d.Tags))
	maps.Copy(cp, d.Tags)

	return cp, nil
}

// CreateInvalidation creates a new cache invalidation for the given distribution.
func (b *InMemoryBackend) CreateInvalidation(
	distributionID, callerRef string,
	paths []string,
) (*Invalidation, error) {
	b.mu.Lock("CreateInvalidation")
	defer b.mu.Unlock()

	if _, ok := b.distributions[distributionID]; !ok {
		return nil, fmt.Errorf("%w: distribution %s not found", ErrNotFound, distributionID)
	}

	inv := &Invalidation{
		ID:         generateID(),
		Status:     "InProgress",
		CreateTime: time.Now().UTC(),
		Paths:      append([]string(nil), paths...),
		CallerRef:  callerRef,
	}
	b.invalidations[distributionID] = append(b.invalidations[distributionID], inv)
	cp := *inv
	cp.Paths = append([]string(nil), inv.Paths...)

	return &cp, nil
}

// ListInvalidations returns all invalidations for a distribution, sorted by ID.
func (b *InMemoryBackend) ListInvalidations(distributionID string) ([]*Invalidation, error) {
	b.mu.RLock("ListInvalidations")
	defer b.mu.RUnlock()

	if _, ok := b.distributions[distributionID]; !ok {
		return nil, fmt.Errorf("%w: distribution %s not found", ErrNotFound, distributionID)
	}

	src := b.invalidations[distributionID]
	out := make([]*Invalidation, 0, len(src))

	for _, inv := range src {
		cp := *inv
		cp.Paths = append([]string(nil), inv.Paths...)
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out, nil
}

// GetInvalidation returns a specific invalidation by distribution ID and invalidation ID.
func (b *InMemoryBackend) GetInvalidation(distributionID, invalidationID string) (*Invalidation, error) {
	b.mu.RLock("GetInvalidation")
	defer b.mu.RUnlock()

	if _, ok := b.distributions[distributionID]; !ok {
		return nil, fmt.Errorf("%w: distribution %s not found", ErrNotFound, distributionID)
	}

	for _, inv := range b.invalidations[distributionID] {
		if inv.ID == invalidationID {
			cp := *inv
			cp.Paths = append([]string(nil), inv.Paths...)

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: invalidation %s not found", ErrInvalidationNotFound, invalidationID)
}

// ListAliases returns the aliases for a distribution by ID.
func (b *InMemoryBackend) ListAliases(distributionID string) []string {
	b.mu.RLock("ListAliases")
	defer b.mu.RUnlock()

	aliases := b.distributionAliases[distributionID]
	if len(aliases) == 0 {
		return nil
	}

	cp := make([]string, len(aliases))
	copy(cp, aliases)

	return cp
}

// AssociateAlias associates a CNAME alias with the specified distribution.
func (b *InMemoryBackend) AssociateAlias(distributionID, alias string) error {
	b.mu.Lock("AssociateAlias")
	defer b.mu.Unlock()

	if _, ok := b.distributions[distributionID]; !ok {
		return fmt.Errorf("%w: distribution %s not found", ErrNotFound, distributionID)
	}

	if alias == "" {
		return fmt.Errorf("%w: alias must not be empty", ErrValidation)
	}

	existing := b.distributionAliases[distributionID]
	if slices.Contains(existing, alias) {
		return nil // already associated, idempotent
	}

	b.distributionAliases[distributionID] = append(existing, alias)

	return nil
}

// AssociateDistributionWebACL associates a WAF web ACL with the specified distribution.
func (b *InMemoryBackend) AssociateDistributionWebACL(distributionID, webACLID string) error {
	b.mu.Lock("AssociateDistributionWebACL")
	defer b.mu.Unlock()

	if _, ok := b.distributions[distributionID]; !ok {
		return fmt.Errorf("%w: distribution %s not found", ErrNotFound, distributionID)
	}

	b.distributionWebACLs[distributionID] = webACLID

	return nil
}

// AssociateDistributionTenantWebACL associates a WAF web ACL with a distribution tenant.
func (b *InMemoryBackend) AssociateDistributionTenantWebACL(tenantID, webACLID string) error {
	b.mu.Lock("AssociateDistributionTenantWebACL")
	defer b.mu.Unlock()

	if tenantID == "" {
		return fmt.Errorf("%w: tenantId must not be empty", ErrValidation)
	}

	b.distributionTenantWebACLs[tenantID] = webACLID

	return nil
}

// CopyDistribution creates a copy of an existing distribution.
func (b *InMemoryBackend) CopyDistribution(primaryDistID, callerRef string) (*Distribution, error) {
	b.mu.Lock("CopyDistribution")
	defer b.mu.Unlock()

	src, ok := b.distributions[primaryDistID]
	if !ok {
		return nil, fmt.Errorf("%w: distribution %s not found", ErrNotFound, primaryDistID)
	}

	if callerRef == "" {
		return nil, fmt.Errorf("%w: CallerReference must not be empty", ErrValidation)
	}

	id := generateID()
	rawCopy := make([]byte, len(src.RawConfig))
	copy(rawCopy, src.RawConfig)

	d := &Distribution{
		ID:              id,
		ARN:             b.distributionARN(id),
		DomainName:      strings.ToLower(id) + ".cloudfront.net",
		Status:          "Deployed",
		ETag:            uuid.NewString(),
		CallerReference: callerRef,
		Comment:         src.Comment,
		Enabled:         src.Enabled,
		RawConfig:       rawCopy,
		Tags:            make(map[string]string),
	}

	b.distributions[id] = d
	b.distributionARNs[d.ARN] = id

	return b.copyDistribution(d), nil
}

// CreateAnycastIPList creates a new Anycast IP list.
func (b *InMemoryBackend) CreateAnycastIPList(name string, ipCount int32) (*AnycastIPList, error) {
	b.mu.Lock("CreateAnycastIpList")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if ipCount <= 0 {
		return nil, fmt.Errorf("%w: IpCount must be greater than 0", ErrValidation)
	}

	id := generateID()
	list := &AnycastIPList{
		ID:      id,
		ARN:     b.anycastIPListARN(id),
		Name:    name,
		Status:  "Deployed",
		IPCount: ipCount,
	}
	b.anycastIPLists[id] = list
	cp := *list

	return &cp, nil
}

// CreateCachePolicy creates a new cache policy.
// Names must be unique. TTLs must satisfy: 0 ≤ MinTTL ≤ DefaultTTL ≤ MaxTTL.
func (b *InMemoryBackend) CreateCachePolicy(
	name, comment string,
	defaultTTL, maxTTL, minTTL int64,
) (*CachePolicy, error) {
	b.mu.Lock("CreateCachePolicy")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if minTTL < 0 {
		return nil, fmt.Errorf("%w: MinTTL must be >= 0", ErrValidation)
	}

	if defaultTTL < minTTL {
		return nil, fmt.Errorf("%w: DefaultTTL must be >= MinTTL", ErrValidation)
	}

	if maxTTL < defaultTTL {
		return nil, fmt.Errorf("%w: MaxTTL must be >= DefaultTTL", ErrValidation)
	}

	if _, exists := b.cachePolicyByName[name]; exists {
		return nil, fmt.Errorf("%w: cache policy with name %q already exists", ErrAlreadyExists, name)
	}

	id := generateID()
	policy := &CachePolicy{
		ID:         id,
		Name:       name,
		Comment:    comment,
		DefaultTTL: defaultTTL,
		MaxTTL:     maxTTL,
		MinTTL:     minTTL,
	}
	b.cachePolicies[id] = policy
	b.cachePolicyByName[name] = id
	cp := *policy

	return &cp, nil
}

// CreateConnectionFunction creates a new connection function.
func (b *InMemoryBackend) CreateConnectionFunction(name, comment string) (*ConnectionFunction, error) {
	b.mu.Lock("CreateConnectionFunction")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	id := generateID()
	arn := fmt.Sprintf("arn:aws:cloudfront::%s:connection-function/%s", b.accountID, id)
	fn := &ConnectionFunction{
		ARN:     arn,
		Name:    name,
		Comment: comment,
	}
	b.connectionFunctions[id] = fn
	cp := *fn

	return &cp, nil
}

// CreateConnectionGroup creates a new connection group.
func (b *InMemoryBackend) CreateConnectionGroup(name, comment string) (*ConnectionGroup, error) {
	b.mu.Lock("CreateConnectionGroup")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	id := generateID()
	group := &ConnectionGroup{
		ID:      id,
		ARN:     b.connectionGroupARN(id),
		Name:    name,
		Comment: comment,
	}
	b.connectionGroups[id] = group
	cp := *group

	return &cp, nil
}

// CreateContinuousDeploymentPolicy creates a new continuous deployment policy.
func (b *InMemoryBackend) CreateContinuousDeploymentPolicy(enabled bool) (*ContinuousDeploymentPolicy, error) {
	b.mu.Lock("CreateContinuousDeploymentPolicy")
	defer b.mu.Unlock()

	id := generateID()
	policy := &ContinuousDeploymentPolicy{
		ID:      id,
		Enabled: enabled,
	}
	b.continuousDeploymentPolicies[id] = policy
	cp := *policy

	return &cp, nil
}

func (b *InMemoryBackend) copyDistribution(d *Distribution) *Distribution {
	cp := *d
	rawCopy := make([]byte, len(d.RawConfig))
	copy(rawCopy, d.RawConfig)
	cp.RawConfig = rawCopy

	tagsCopy := make(map[string]string, len(d.Tags))
	maps.Copy(tagsCopy, d.Tags)
	cp.Tags = tagsCopy

	return &cp
}

// --- Cache Policy CRUD ---

// GetCachePolicy returns a cache policy by ID.
func (b *InMemoryBackend) GetCachePolicy(id string) (*CachePolicy, error) {
	b.mu.RLock("GetCachePolicy")
	defer b.mu.RUnlock()

	p, ok := b.cachePolicies[id]
	if !ok {
		return nil, fmt.Errorf("%w: cache policy %s not found", ErrCachePolicyNotFound, id)
	}

	cp := *p

	return &cp, nil
}

// ListCachePolicies returns all cache policies sorted by ID.
func (b *InMemoryBackend) ListCachePolicies() []*CachePolicy {
	b.mu.RLock("ListCachePolicies")
	defer b.mu.RUnlock()

	list := make([]*CachePolicy, 0, len(b.cachePolicies))
	for _, p := range b.cachePolicies {
		cp := *p
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// UpdateCachePolicy updates an existing cache policy.
func (b *InMemoryBackend) UpdateCachePolicy(
	id, name, comment string,
	defaultTTL, maxTTL, minTTL int64,
) (*CachePolicy, error) {
	b.mu.Lock("UpdateCachePolicy")
	defer b.mu.Unlock()

	p, ok := b.cachePolicies[id]
	if !ok {
		return nil, fmt.Errorf("%w: cache policy %s not found", ErrCachePolicyNotFound, id)
	}

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if minTTL < 0 {
		return nil, fmt.Errorf("%w: MinTTL must be >= 0", ErrValidation)
	}

	if defaultTTL < minTTL {
		return nil, fmt.Errorf("%w: DefaultTTL must be >= MinTTL", ErrValidation)
	}

	if maxTTL < defaultTTL {
		return nil, fmt.Errorf("%w: MaxTTL must be >= DefaultTTL", ErrValidation)
	}

	// If name changed, ensure uniqueness and update index.
	if name != p.Name {
		if _, exists := b.cachePolicyByName[name]; exists {
			return nil, fmt.Errorf("%w: cache policy with name %q already exists", ErrAlreadyExists, name)
		}

		delete(b.cachePolicyByName, p.Name)
		b.cachePolicyByName[name] = id
	}

	p.Name = name
	p.Comment = comment
	p.DefaultTTL = defaultTTL
	p.MaxTTL = maxTTL
	p.MinTTL = minTTL

	cp := *p

	return &cp, nil
}

// DeleteCachePolicy deletes a cache policy by ID.
func (b *InMemoryBackend) DeleteCachePolicy(id string) error {
	b.mu.Lock("DeleteCachePolicy")
	defer b.mu.Unlock()

	p, ok := b.cachePolicies[id]
	if !ok {
		return fmt.Errorf("%w: cache policy %s not found", ErrCachePolicyNotFound, id)
	}

	delete(b.cachePolicyByName, p.Name)
	delete(b.cachePolicies, id)

	return nil
}

// --- Origin Access Control CRUD ---

// CreateOriginAccessControl creates a new Origin Access Control.
func (b *InMemoryBackend) CreateOriginAccessControl(
	name, description, originType, signingBehavior, signingProtocol string,
) (*OriginAccessControl, error) {
	b.mu.Lock("CreateOriginAccessControl")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if _, exists := b.originAccessControlByName[name]; exists {
		return nil, fmt.Errorf("%w: origin access control with name %q already exists", ErrAlreadyExists, name)
	}

	id := generateID()
	oac := &OriginAccessControl{
		ID:              id,
		Name:            name,
		Description:     description,
		OriginType:      originType,
		SigningBehavior: signingBehavior,
		SigningProtocol: signingProtocol,
		ETag:            uuid.NewString(),
	}
	b.originAccessControls[id] = oac
	b.originAccessControlByName[name] = id
	cp := *oac

	return &cp, nil
}

// GetOriginAccessControl returns an OAC by ID.
func (b *InMemoryBackend) GetOriginAccessControl(id string) (*OriginAccessControl, error) {
	b.mu.RLock("GetOriginAccessControl")
	defer b.mu.RUnlock()

	oac, ok := b.originAccessControls[id]
	if !ok {
		return nil, fmt.Errorf("%w: origin access control %s not found", ErrOACNotFound, id)
	}

	cp := *oac

	return &cp, nil
}

// ListOriginAccessControls returns all OACs sorted by ID.
func (b *InMemoryBackend) ListOriginAccessControls() []*OriginAccessControl {
	b.mu.RLock("ListOriginAccessControls")
	defer b.mu.RUnlock()

	list := make([]*OriginAccessControl, 0, len(b.originAccessControls))
	for _, oac := range b.originAccessControls {
		cp := *oac
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// UpdateOriginAccessControl updates an existing OAC.
func (b *InMemoryBackend) UpdateOriginAccessControl(
	id, name, description, originType, signingBehavior, signingProtocol string,
) (*OriginAccessControl, error) {
	b.mu.Lock("UpdateOriginAccessControl")
	defer b.mu.Unlock()

	oac, ok := b.originAccessControls[id]
	if !ok {
		return nil, fmt.Errorf("%w: origin access control %s not found", ErrOACNotFound, id)
	}

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if name != oac.Name {
		if _, exists := b.originAccessControlByName[name]; exists {
			return nil, fmt.Errorf("%w: origin access control with name %q already exists", ErrAlreadyExists, name)
		}

		delete(b.originAccessControlByName, oac.Name)
		b.originAccessControlByName[name] = id
	}

	oac.Name = name
	oac.Description = description
	oac.OriginType = originType
	oac.SigningBehavior = signingBehavior
	oac.SigningProtocol = signingProtocol
	oac.ETag = uuid.NewString()
	cp := *oac

	return &cp, nil
}

// DeleteOriginAccessControl deletes an OAC by ID.
func (b *InMemoryBackend) DeleteOriginAccessControl(id string) error {
	b.mu.Lock("DeleteOriginAccessControl")
	defer b.mu.Unlock()

	oac, ok := b.originAccessControls[id]
	if !ok {
		return fmt.Errorf("%w: origin access control %s not found", ErrOACNotFound, id)
	}

	delete(b.originAccessControlByName, oac.Name)
	delete(b.originAccessControls, id)

	return nil
}

// --- Response Headers Policy CRUD ---

// CreateResponseHeadersPolicy creates a new Response Headers Policy.
func (b *InMemoryBackend) CreateResponseHeadersPolicy(name, comment string) (*ResponseHeadersPolicy, error) {
	b.mu.Lock("CreateResponseHeadersPolicy")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if _, exists := b.responseHeadersPolicyByName[name]; exists {
		return nil, fmt.Errorf("%w: response headers policy with name %q already exists", ErrAlreadyExists, name)
	}

	id := generateID()
	p := &ResponseHeadersPolicy{
		ID:      id,
		Name:    name,
		Comment: comment,
		ETag:    uuid.NewString(),
	}
	b.responseHeadersPolicies[id] = p
	b.responseHeadersPolicyByName[name] = id
	cp := *p

	return &cp, nil
}

// GetResponseHeadersPolicy returns a Response Headers Policy by ID.
func (b *InMemoryBackend) GetResponseHeadersPolicy(id string) (*ResponseHeadersPolicy, error) {
	b.mu.RLock("GetResponseHeadersPolicy")
	defer b.mu.RUnlock()

	p, ok := b.responseHeadersPolicies[id]
	if !ok {
		return nil, fmt.Errorf("%w: response headers policy %s not found", ErrResponseHeadersPolicyNotFound, id)
	}

	cp := *p

	return &cp, nil
}

// ListResponseHeadersPolicies returns all Response Headers Policies sorted by ID.
func (b *InMemoryBackend) ListResponseHeadersPolicies() []*ResponseHeadersPolicy {
	b.mu.RLock("ListResponseHeadersPolicies")
	defer b.mu.RUnlock()

	list := make([]*ResponseHeadersPolicy, 0, len(b.responseHeadersPolicies))
	for _, p := range b.responseHeadersPolicies {
		cp := *p
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// UpdateResponseHeadersPolicy updates an existing Response Headers Policy.
func (b *InMemoryBackend) UpdateResponseHeadersPolicy(id, name, comment string) (*ResponseHeadersPolicy, error) {
	b.mu.Lock("UpdateResponseHeadersPolicy")
	defer b.mu.Unlock()

	p, ok := b.responseHeadersPolicies[id]
	if !ok {
		return nil, fmt.Errorf("%w: response headers policy %s not found", ErrResponseHeadersPolicyNotFound, id)
	}

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if name != p.Name {
		if _, exists := b.responseHeadersPolicyByName[name]; exists {
			return nil, fmt.Errorf("%w: response headers policy with name %q already exists", ErrAlreadyExists, name)
		}

		delete(b.responseHeadersPolicyByName, p.Name)
		b.responseHeadersPolicyByName[name] = id
	}

	p.Name = name
	p.Comment = comment
	p.ETag = uuid.NewString()
	cp := *p

	return &cp, nil
}

// DeleteResponseHeadersPolicy deletes a Response Headers Policy by ID.
func (b *InMemoryBackend) DeleteResponseHeadersPolicy(id string) error {
	b.mu.Lock("DeleteResponseHeadersPolicy")
	defer b.mu.Unlock()

	p, ok := b.responseHeadersPolicies[id]
	if !ok {
		return fmt.Errorf("%w: response headers policy %s not found", ErrResponseHeadersPolicyNotFound, id)
	}

	delete(b.responseHeadersPolicyByName, p.Name)
	delete(b.responseHeadersPolicies, id)

	return nil
}

// --- CloudFront Function CRUD ---

// CreateFunction creates a new CloudFront Function.
func (b *InMemoryBackend) CreateFunction(name, comment, runtime, functionCode string) (*Function, error) {
	b.mu.Lock("CreateFunction")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if _, exists := b.functions[name]; exists {
		return nil, fmt.Errorf("%w: function with name %q already exists", ErrAlreadyExists, name)
	}

	fn := &Function{
		Name:         name,
		Comment:      comment,
		Runtime:      runtime,
		FunctionCode: functionCode,
		Status:       "UNPUBLISHED",
		ETag:         uuid.NewString(),
		ARN:          b.functionARN(name),
	}
	b.functions[name] = fn
	cp := *fn

	return &cp, nil
}

// GetFunction returns a CloudFront Function by name.
func (b *InMemoryBackend) GetFunction(name string) (*Function, error) {
	b.mu.RLock("GetFunction")
	defer b.mu.RUnlock()

	fn, ok := b.functions[name]
	if !ok {
		return nil, fmt.Errorf("%w: function %s not found", ErrFunctionNotFound, name)
	}

	cp := *fn

	return &cp, nil
}

// ListFunctions returns all CloudFront Functions sorted by name.
func (b *InMemoryBackend) ListFunctions() []*Function {
	b.mu.RLock("ListFunctions")
	defer b.mu.RUnlock()

	list := make([]*Function, 0, len(b.functions))
	for _, fn := range b.functions {
		cp := *fn
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// PublishFunction publishes (promotes to LIVE) a CloudFront Function.
func (b *InMemoryBackend) PublishFunction(name string) (*Function, error) {
	b.mu.Lock("PublishFunction")
	defer b.mu.Unlock()

	fn, ok := b.functions[name]
	if !ok {
		return nil, fmt.Errorf("%w: function %s not found", ErrFunctionNotFound, name)
	}

	fn.Status = "LIVE"
	fn.ETag = uuid.NewString()
	cp := *fn

	return &cp, nil
}

// UpdateFunction updates an existing CloudFront Function.
func (b *InMemoryBackend) UpdateFunction(name, comment, runtime, functionCode string) (*Function, error) {
	b.mu.Lock("UpdateFunction")
	defer b.mu.Unlock()

	fn, ok := b.functions[name]
	if !ok {
		return nil, fmt.Errorf("%w: function %s not found", ErrFunctionNotFound, name)
	}

	fn.Comment = comment
	fn.Runtime = runtime
	fn.FunctionCode = functionCode
	fn.Status = "UNPUBLISHED"
	fn.ETag = uuid.NewString()
	cp := *fn

	return &cp, nil
}

// DeleteFunction deletes a CloudFront Function by name.
func (b *InMemoryBackend) DeleteFunction(name string) error {
	b.mu.Lock("DeleteFunction")
	defer b.mu.Unlock()

	if _, ok := b.functions[name]; !ok {
		return fmt.Errorf("%w: function %s not found", ErrFunctionNotFound, name)
	}

	delete(b.functions, name)

	return nil
}

// --- Origin Request Policy CRUD ---

// CreateOriginRequestPolicy creates a new Origin Request Policy.
func (b *InMemoryBackend) CreateOriginRequestPolicy(name, comment string) (*OriginRequestPolicy, error) {
	b.mu.Lock("CreateOriginRequestPolicy")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if _, exists := b.originRequestPolicyByName[name]; exists {
		return nil, fmt.Errorf("%w: origin request policy with name %q already exists", ErrAlreadyExists, name)
	}

	id := generateID()
	p := &OriginRequestPolicy{
		ID:      id,
		Name:    name,
		Comment: comment,
		ETag:    uuid.NewString(),
	}
	b.originRequestPolicies[id] = p
	b.originRequestPolicyByName[name] = id
	cp := *p

	return &cp, nil
}

// GetOriginRequestPolicy returns an Origin Request Policy by ID.
func (b *InMemoryBackend) GetOriginRequestPolicy(id string) (*OriginRequestPolicy, error) {
	b.mu.RLock("GetOriginRequestPolicy")
	defer b.mu.RUnlock()

	p, ok := b.originRequestPolicies[id]
	if !ok {
		return nil, fmt.Errorf("%w: origin request policy %s not found", ErrOriginRequestPolicyNotFound, id)
	}

	cp := *p

	return &cp, nil
}

// ListOriginRequestPolicies returns all Origin Request Policies sorted by ID.
func (b *InMemoryBackend) ListOriginRequestPolicies() []*OriginRequestPolicy {
	b.mu.RLock("ListOriginRequestPolicies")
	defer b.mu.RUnlock()

	list := make([]*OriginRequestPolicy, 0, len(b.originRequestPolicies))
	for _, p := range b.originRequestPolicies {
		cp := *p
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// UpdateOriginRequestPolicy updates an existing Origin Request Policy.
func (b *InMemoryBackend) UpdateOriginRequestPolicy(id, name, comment string) (*OriginRequestPolicy, error) {
	b.mu.Lock("UpdateOriginRequestPolicy")
	defer b.mu.Unlock()

	p, ok := b.originRequestPolicies[id]
	if !ok {
		return nil, fmt.Errorf("%w: origin request policy %s not found", ErrOriginRequestPolicyNotFound, id)
	}

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if name != p.Name {
		if _, exists := b.originRequestPolicyByName[name]; exists {
			return nil, fmt.Errorf("%w: origin request policy with name %q already exists", ErrAlreadyExists, name)
		}

		delete(b.originRequestPolicyByName, p.Name)
		b.originRequestPolicyByName[name] = id
	}

	p.Name = name
	p.Comment = comment
	p.ETag = uuid.NewString()
	cp := *p

	return &cp, nil
}

// DeleteOriginRequestPolicy deletes an Origin Request Policy by ID.
func (b *InMemoryBackend) DeleteOriginRequestPolicy(id string) error {
	b.mu.Lock("DeleteOriginRequestPolicy")
	defer b.mu.Unlock()

	p, ok := b.originRequestPolicies[id]
	if !ok {
		return fmt.Errorf("%w: origin request policy %s not found", ErrOriginRequestPolicyNotFound, id)
	}

	delete(b.originRequestPolicyByName, p.Name)
	delete(b.originRequestPolicies, id)

	return nil
}
