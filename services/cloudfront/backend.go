package cloudfront

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
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
	// ErrValidation is returned when request parameters fail validation.
	ErrValidation = awserr.New("InvalidArgument", awserr.ErrInvalidParameter)
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

// InMemoryBackend stores CloudFront resources in memory.
type InMemoryBackend struct {
	distributions                map[string]*Distribution
	distributionARNs             map[string]string          // ARN → distribution ID (O(1) tag lookups)
	distributionAliases          map[string][]string        // distribution ID → aliases
	distributionWebACLs          map[string]string          // distribution ID → web ACL ID
	distributionTenantWebACLs    map[string]string          // tenant ID → web ACL ID
	invalidations                map[string][]*Invalidation // distribution ID → []Invalidation
	oais                         map[string]*OriginAccessIdentity
	anycastIPLists               map[string]*AnycastIPList
	cachePolicies                map[string]*CachePolicy
	connectionFunctions          map[string]*ConnectionFunction
	connectionGroups             map[string]*ConnectionGroup
	continuousDeploymentPolicies map[string]*ContinuousDeploymentPolicy
	mu                           *lockmetrics.RWMutex
	accountID                    string
	region                       string
}

// NewInMemoryBackend creates a new in-memory CloudFront backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		distributions:                make(map[string]*Distribution),
		distributionARNs:             make(map[string]string),
		distributionAliases:          make(map[string][]string),
		distributionWebACLs:          make(map[string]string),
		distributionTenantWebACLs:    make(map[string]string),
		invalidations:                make(map[string][]*Invalidation),
		oais:                         make(map[string]*OriginAccessIdentity),
		anycastIPLists:               make(map[string]*AnycastIPList),
		cachePolicies:                make(map[string]*CachePolicy),
		connectionFunctions:          make(map[string]*ConnectionFunction),
		connectionGroups:             make(map[string]*ConnectionGroup),
		continuousDeploymentPolicies: make(map[string]*ContinuousDeploymentPolicy),
		mu:                           lockmetrics.New("cloudfront"),
		accountID:                    accountID,
		region:                       region,
	}
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

// CreateDistribution creates a new CloudFront distribution.
func (b *InMemoryBackend) CreateDistribution(
	callerRef, comment string,
	enabled bool,
	rawConfig []byte,
) (*Distribution, error) {
	b.mu.Lock("CreateDistribution")
	defer b.mu.Unlock()

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

// DeleteDistribution deletes a distribution by ID.
func (b *InMemoryBackend) DeleteDistribution(id string) error {
	b.mu.Lock("DeleteDistribution")
	defer b.mu.Unlock()

	if _, ok := b.distributions[id]; !ok {
		return fmt.Errorf("%w: distribution %s not found", ErrNotFound, id)
	}
	distributionARN := b.distributionARN(id)
	delete(b.distributionARNs, distributionARN)
	delete(b.distributions, id)
	delete(b.invalidations, id)

	return nil
}

// ListDistributions returns all distributions.
func (b *InMemoryBackend) ListDistributions() []*Distribution {
	b.mu.RLock("ListDistributions")
	defer b.mu.RUnlock()

	list := make([]*Distribution, 0, len(b.distributions))
	for _, d := range b.distributions {
		list = append(list, b.copyDistribution(d))
	}

	return list
}

// CreateOAI creates a new Origin Access Identity.
func (b *InMemoryBackend) CreateOAI(callerRef, comment string) (*OriginAccessIdentity, error) {
	b.mu.Lock("CreateOAI")
	defer b.mu.Unlock()

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
	cp := *oai

	return &cp, nil
}

// GetOAI returns an OAI by ID.
func (b *InMemoryBackend) GetOAI(id string) (*OriginAccessIdentity, error) {
	b.mu.RLock("GetOAI")
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
	b.mu.Lock("DeleteOAI")
	defer b.mu.Unlock()

	if _, ok := b.oais[id]; !ok {
		return fmt.Errorf("%w: OAI %s not found", ErrOAINotFound, id)
	}
	delete(b.oais, id)

	return nil
}

// ListOAIs returns all OAIs.
func (b *InMemoryBackend) ListOAIs() []*OriginAccessIdentity {
	b.mu.RLock("ListOAIs")
	defer b.mu.RUnlock()

	list := make([]*OriginAccessIdentity, 0, len(b.oais))
	for _, oai := range b.oais {
		cp := *oai
		list = append(list, &cp)
	}

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

// ListInvalidations returns all invalidations for a distribution.
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

	return nil, fmt.Errorf("%w: invalidation %s not found", ErrNotFound, invalidationID)
}

// anycastIPListARN builds an ARN for an Anycast IP list.
func (b *InMemoryBackend) anycastIPListARN(id string) string {
	return fmt.Sprintf("arn:aws:cloudfront::%s:anycast-ip-list/%s", b.accountID, id)
}

// connectionGroupARN builds an ARN for a connection group.
func (b *InMemoryBackend) connectionGroupARN(id string) string {
	return fmt.Sprintf("arn:aws:cloudfront::%s:connection-group/%s", b.accountID, id)
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
	b.mu.Lock("CreateAnycastIPList")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
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
func (b *InMemoryBackend) CreateCachePolicy(
	name, comment string,
	defaultTTL, maxTTL, minTTL int64,
) (*CachePolicy, error) {
	b.mu.Lock("CreateCachePolicy")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
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

	arn := fmt.Sprintf("arn:aws:cloudfront::%s:connection-function/%s", b.accountID, name)
	fn := &ConnectionFunction{
		ARN:     arn,
		Name:    name,
		Comment: comment,
	}
	b.connectionFunctions[name] = fn
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
