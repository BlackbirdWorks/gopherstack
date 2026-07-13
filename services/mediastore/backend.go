package mediastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

var (
	// ErrContainerNotFound is returned when a container does not exist.
	ErrContainerNotFound = awserr.New(
		"container not found",
		awserr.ErrNotFound,
	)
	// ErrContainerAlreadyExists is returned when a container already exists.
	ErrContainerAlreadyExists = awserr.New(
		"container already exists",
		awserr.ErrAlreadyExists,
	)
	// ErrPolicyNotFound is returned when no container policy has been set.
	ErrPolicyNotFound = awserr.New(
		"no policy found for container",
		awserr.ErrNotFound,
	)
	// ErrCorsPolicyNotFound is returned when no CORS policy has been set.
	ErrCorsPolicyNotFound = awserr.New(
		"no CORS policy found for container",
		awserr.ErrNotFound,
	)
	// ErrLifecyclePolicyNotFound is returned when no lifecycle policy has been set.
	ErrLifecyclePolicyNotFound = awserr.New(
		"no lifecycle policy found for container",
		awserr.ErrNotFound,
	)
	// ErrMetricPolicyNotFound is returned when no metric policy has been set.
	ErrMetricPolicyNotFound = awserr.New(
		"no metric policy found for container",
		awserr.ErrNotFound,
	)
	// ErrMissingContainerName is returned when the container name is missing.
	ErrMissingContainerName = errors.New("ContainerName is required")
	// ErrInvalidContainerName is returned when the container name is invalid.
	//
	// The message intentionally does NOT repeat the "ValidationException:" type
	// prefix: writeBackendError already carries that in the response envelope's
	// __type field (see JSONErrorResponse), so baking it into the message text
	// too would double it up on the wire (e.g. "ValidationException:
	// ValidationException: ...", as every AWS SDK formats client-visible errors
	// as "api error <Type>: <message>").
	ErrInvalidContainerName = errors.New(
		"container name must be 1-255 characters" +
			" and contain only letters, numbers, hyphens, and underscores",
	)
	// ErrInvalidPolicy is returned when a policy string is not valid JSON.
	ErrInvalidPolicy = errors.New("policy must be valid JSON")
	// ErrCorsRuleInvalid is returned when a CORS rule is missing required fields.
	ErrCorsRuleInvalid = errors.New(
		"each CORS rule must have at least one AllowedOrigin and one AllowedHeader",
	)
	// ErrInvalidMetricPolicy is returned when ContainerLevelMetrics has an invalid value.
	ErrInvalidMetricPolicy = errors.New(
		"ContainerLevelMetrics must be ENABLED or DISABLED",
	)
	// ErrTooManyMetricRules is returned when more than 5 metric policy rules are provided.
	ErrTooManyMetricRules = errors.New(
		"metric policy may have at most 5 rules",
	)
	// ErrEmptyTagKey is returned when a tag with an empty key is provided.
	ErrEmptyTagKey = errors.New("tag key must not be empty")

	// containerNameRE is the allowed character set for container names.
	containerNameRE = regexp.MustCompile(`^[a-zA-Z0-9\-_]+$`)
)

const (
	// containerStatusActive is the status for a ready container.
	containerStatusActive = "ACTIVE"
	// defaultEndpointFormat is the format for the container endpoint.
	defaultEndpointFormat = "https://%s.data.mediastore.%s.amazonaws.com"
	// maxContainerNameLen is the maximum allowed length of a container name.
	maxContainerNameLen = 255
	// maxMetricPolicyRules is the maximum number of metric policy rules.
	maxMetricPolicyRules = 5
	// defaultListLimit is the default page size for ListContainers.
	defaultListLimit = 100
)

// StorageBackend is the interface for the MediaStore in-memory backend.
//
// All methods take a context.Context whose resolved AWS region (stored under
// regionContextKey) partitions state: containers created in one region are
// invisible to every other region.
type StorageBackend interface {
	// Snapshot and Restore implement persistence.Persistable. Handler
	// delegates to them (see persistence.go) so cli.go's generic
	// setupPersistence picks MediaStore up.
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error

	CreateContainer(ctx context.Context, accountID, name string, tags map[string]string) (*Container, error)
	DeleteContainer(ctx context.Context, name string) error
	DescribeContainer(ctx context.Context, name string) (*Container, error)
	ListContainers(ctx context.Context, nextToken string, maxResults int) ([]*Container, string, error)
	PutContainerPolicy(ctx context.Context, name, policy string) error
	GetContainerPolicy(ctx context.Context, name string) (string, error)
	DeleteContainerPolicy(ctx context.Context, name string) error
	PutCorsPolicy(ctx context.Context, name string, rules []CorsRule) error
	GetCorsPolicy(ctx context.Context, name string) ([]CorsRule, error)
	DeleteCorsPolicy(ctx context.Context, name string) error
	PutLifecyclePolicy(ctx context.Context, name, policy string) error
	GetLifecyclePolicy(ctx context.Context, name string) (string, error)
	DeleteLifecyclePolicy(ctx context.Context, name string) error
	PutMetricPolicy(ctx context.Context, name string, policy MetricPolicy) error
	GetMetricPolicy(ctx context.Context, name string) (MetricPolicy, error)
	DeleteMetricPolicy(ctx context.Context, name string) error
	StartAccessLogging(ctx context.Context, name string) error
	StopAccessLogging(ctx context.Context, name string) error
	TagResource(ctx context.Context, resourceARN string, tags map[string]string) error
	UntagResource(ctx context.Context, resourceARN string, tagKeys []string) error
	ListTagsForResource(ctx context.Context, resourceARN string) (map[string]string, error)
}

// InMemoryBackend is the in-memory implementation of StorageBackend.
//
// State is partitioned by region: containers[region] gives the store.Table of
// containers stored within that region (see store_setup.go for why this is a
// per-region map rather than a single flat table).
type InMemoryBackend struct {
	containers       map[string]*store.Table[Container]
	mu               *lockmetrics.RWMutex
	paginationSecret string
}

// NewInMemoryBackend creates a new in-memory MediaStore backend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		containers:       make(map[string]*store.Table[Container]),
		paginationSecret: uuid.NewString(),
		mu:               lockmetrics.New("mediastore"),
	}
}

// regionFromContext resolves the AWS region for the current request from ctx,
// falling back to the default region when none is present.
func regionFromContext(ctx context.Context) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return config.DefaultRegion
}

// containersStore returns the per-region container [store.Table] for region,
// creating it on first use. Because creation mutates b.containers, callers
// must already hold b.mu for writing (Lock) -- read paths that only need to
// look up an already-existing region must use [InMemoryBackend.containerRegion]
// instead so a lazy allocation never races under a shared RLock.
func (b *InMemoryBackend) containersStore(region string) *store.Table[Container] {
	if b.containers[region] == nil {
		b.containers[region] = store.New(containerKeyFn)
	}

	return b.containers[region]
}

// containerRegion returns the per-region container [store.Table] for region
// without creating it, or nil if region has no containers yet. Safe to call
// under either b.mu.RLock or b.mu.Lock.
func (b *InMemoryBackend) containerRegion(region string) *store.Table[Container] {
	return b.containers[region]
}

// getContainer looks up the container stored under name in region without
// creating the region's table. Safe to call under either b.mu.RLock or
// b.mu.Lock -- used by every read/mutate-in-place accessor below that only
// ever needs an already-existing container, never to create one.
func (b *InMemoryBackend) getContainer(region, name string) (*Container, bool) {
	tbl := b.containerRegion(region)
	if tbl == nil {
		return nil, false
	}

	return tbl.Get(name)
}

// validateContainerName returns an error if the container name is invalid.
func validateContainerName(name string) error {
	if name == "" || len(name) > maxContainerNameLen || !containerNameRE.MatchString(name) {
		return ErrInvalidContainerName
	}

	return nil
}

// containerNameFromARN extracts the container name from a MediaStore ARN.
// ARN format: arn:aws:mediastore:region:account:container/name.
func containerNameFromARN(resourceARN string) string {
	_, after, ok := strings.Cut(resourceARN, ":container/")
	if !ok {
		return ""
	}

	return after
}

// containerARN builds the ARN for a MediaStore container.
func containerARN(region, accountID, name string) string {
	return arn.Build("mediastore", region, accountID, "container/"+name)
}

// containerEndpoint returns the data plane endpoint for a container.
func containerEndpoint(name, region string) string {
	return fmt.Sprintf(defaultEndpointFormat, name, region)
}

// CreateContainer creates a new MediaStore container in the ctx region.
func (b *InMemoryBackend) CreateContainer(
	ctx context.Context,
	accountID, name string,
	tags map[string]string,
) (*Container, error) {
	if err := validateContainerName(name); err != nil {
		return nil, err
	}

	region := regionFromContext(ctx)

	b.mu.Lock("CreateContainer")
	defer b.mu.Unlock()

	tbl := b.containersStore(region)

	if tbl.Has(name) {
		return nil, ErrContainerAlreadyExists
	}

	now := time.Now().UTC()

	t := make(map[string]string)
	maps.Copy(t, tags)

	c := &Container{
		Name:         name,
		ARN:          containerARN(region, accountID, name),
		Endpoint:     containerEndpoint(name, region),
		Status:       containerStatusActive,
		CreationTime: &now,
		Tags:         t,
	}

	tbl.Put(c)

	return copyContainer(c), nil
}

// DeleteContainer removes a container from the ctx region.
func (b *InMemoryBackend) DeleteContainer(ctx context.Context, name string) error {
	region := regionFromContext(ctx)

	b.mu.Lock("DeleteContainer")
	defer b.mu.Unlock()

	tbl := b.containerRegion(region)

	if tbl == nil || !tbl.Has(name) {
		return ErrContainerNotFound
	}

	tbl.Delete(name)

	return nil
}

// DescribeContainer returns details about a container in the ctx region.
func (b *InMemoryBackend) DescribeContainer(ctx context.Context, name string) (*Container, error) {
	region := regionFromContext(ctx)

	b.mu.RLock("DescribeContainer")
	defer b.mu.RUnlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return nil, ErrContainerNotFound
	}

	return copyContainer(c), nil
}

// ListContainers returns paginated containers in the ctx region sorted by name.
func (b *InMemoryBackend) ListContainers(
	ctx context.Context,
	nextToken string,
	maxResults int,
) ([]*Container, string, error) {
	region := regionFromContext(ctx)

	b.mu.RLock("ListContainers")
	defer b.mu.RUnlock()

	tbl := b.containerRegion(region)

	var all []*Container
	if tbl != nil {
		items := tbl.All()
		all = make([]*Container, 0, len(items))

		for _, c := range items {
			all = append(all, copyContainer(c))
		}
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Name < all[j].Name
	})

	p := page.NewHMAC(all, nextToken, b.paginationSecret, maxResults, defaultListLimit)

	return p.Data, p.Next, nil
}

// PutContainerPolicy stores a container access policy.
func (b *InMemoryBackend) PutContainerPolicy(ctx context.Context, name, policy string) error {
	if !json.Valid([]byte(policy)) {
		return ErrInvalidPolicy
	}

	region := regionFromContext(ctx)

	b.mu.Lock("PutContainerPolicy")
	defer b.mu.Unlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return ErrContainerNotFound
	}

	c.ContainerPolicy = policy

	return nil
}

// GetContainerPolicy retrieves the container access policy.
func (b *InMemoryBackend) GetContainerPolicy(ctx context.Context, name string) (string, error) {
	region := regionFromContext(ctx)

	b.mu.RLock("GetContainerPolicy")
	defer b.mu.RUnlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return "", ErrContainerNotFound
	}

	if c.ContainerPolicy == "" {
		return "", ErrPolicyNotFound
	}

	return c.ContainerPolicy, nil
}

// DeleteContainerPolicy removes the container access policy.
func (b *InMemoryBackend) DeleteContainerPolicy(ctx context.Context, name string) error {
	region := regionFromContext(ctx)

	b.mu.Lock("DeleteContainerPolicy")
	defer b.mu.Unlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return ErrContainerNotFound
	}

	if c.ContainerPolicy == "" {
		return ErrPolicyNotFound
	}

	c.ContainerPolicy = ""

	return nil
}

// PutCorsPolicy stores a CORS policy for a container.
func (b *InMemoryBackend) PutCorsPolicy(ctx context.Context, name string, rules []CorsRule) error {
	for _, r := range rules {
		if len(r.AllowedOrigins) == 0 || len(r.AllowedHeaders) == 0 {
			return ErrCorsRuleInvalid
		}
	}

	region := regionFromContext(ctx)

	b.mu.Lock("PutCorsPolicy")
	defer b.mu.Unlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return ErrContainerNotFound
	}

	ptrs := make([]*CorsRule, len(rules))
	for i := range rules {
		r := rules[i]
		ptrs[i] = &r
	}

	c.CorsPolicy = ptrs

	return nil
}

// GetCorsPolicy retrieves the CORS policy for a container.
func (b *InMemoryBackend) GetCorsPolicy(ctx context.Context, name string) ([]CorsRule, error) {
	region := regionFromContext(ctx)

	b.mu.RLock("GetCorsPolicy")
	defer b.mu.RUnlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return nil, ErrContainerNotFound
	}

	if c.CorsPolicy == nil {
		return nil, ErrCorsPolicyNotFound
	}

	rules := make([]CorsRule, len(c.CorsPolicy))
	for i, p := range c.CorsPolicy {
		rules[i] = *p
	}

	return rules, nil
}

// DeleteCorsPolicy removes the CORS policy from a container.
func (b *InMemoryBackend) DeleteCorsPolicy(ctx context.Context, name string) error {
	region := regionFromContext(ctx)

	b.mu.Lock("DeleteCorsPolicy")
	defer b.mu.Unlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return ErrContainerNotFound
	}

	if c.CorsPolicy == nil {
		return ErrCorsPolicyNotFound
	}

	c.CorsPolicy = nil

	return nil
}

// PutLifecyclePolicy stores a lifecycle policy for a container.
func (b *InMemoryBackend) PutLifecyclePolicy(ctx context.Context, name, policy string) error {
	if !json.Valid([]byte(policy)) {
		return ErrInvalidPolicy
	}

	region := regionFromContext(ctx)

	b.mu.Lock("PutLifecyclePolicy")
	defer b.mu.Unlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return ErrContainerNotFound
	}

	c.LifecyclePolicy = policy

	return nil
}

// GetLifecyclePolicy retrieves the lifecycle policy for a container.
func (b *InMemoryBackend) GetLifecyclePolicy(ctx context.Context, name string) (string, error) {
	region := regionFromContext(ctx)

	b.mu.RLock("GetLifecyclePolicy")
	defer b.mu.RUnlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return "", ErrContainerNotFound
	}

	if c.LifecyclePolicy == "" {
		return "", ErrLifecyclePolicyNotFound
	}

	return c.LifecyclePolicy, nil
}

// DeleteLifecyclePolicy removes the lifecycle policy from a container.
func (b *InMemoryBackend) DeleteLifecyclePolicy(ctx context.Context, name string) error {
	region := regionFromContext(ctx)

	b.mu.Lock("DeleteLifecyclePolicy")
	defer b.mu.Unlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return ErrContainerNotFound
	}

	if c.LifecyclePolicy == "" {
		return ErrLifecyclePolicyNotFound
	}

	c.LifecyclePolicy = ""

	return nil
}

// PutMetricPolicy stores a metric policy for a container.
func (b *InMemoryBackend) PutMetricPolicy(ctx context.Context, name string, policy MetricPolicy) error {
	if policy.ContainerLevelMetrics != "ENABLED" && policy.ContainerLevelMetrics != "DISABLED" {
		return ErrInvalidMetricPolicy
	}

	if len(policy.MetricPolicyRules) > maxMetricPolicyRules {
		return ErrTooManyMetricRules
	}

	region := regionFromContext(ctx)

	b.mu.Lock("PutMetricPolicy")
	defer b.mu.Unlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return ErrContainerNotFound
	}

	p := policy
	c.MetricPolicy = &p

	return nil
}

// GetMetricPolicy retrieves the metric policy for a container.
func (b *InMemoryBackend) GetMetricPolicy(ctx context.Context, name string) (MetricPolicy, error) {
	region := regionFromContext(ctx)

	b.mu.RLock("GetMetricPolicy")
	defer b.mu.RUnlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return MetricPolicy{}, ErrContainerNotFound
	}

	if c.MetricPolicy == nil {
		return MetricPolicy{}, ErrMetricPolicyNotFound
	}

	return *c.MetricPolicy, nil
}

// DeleteMetricPolicy removes the metric policy from a container.
func (b *InMemoryBackend) DeleteMetricPolicy(ctx context.Context, name string) error {
	region := regionFromContext(ctx)

	b.mu.Lock("DeleteMetricPolicy")
	defer b.mu.Unlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return ErrContainerNotFound
	}

	if c.MetricPolicy == nil {
		return ErrMetricPolicyNotFound
	}

	c.MetricPolicy = nil

	return nil
}

// StartAccessLogging enables access logging for a container.
func (b *InMemoryBackend) StartAccessLogging(ctx context.Context, name string) error {
	region := regionFromContext(ctx)

	b.mu.Lock("StartAccessLogging")
	defer b.mu.Unlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return ErrContainerNotFound
	}

	c.AccessLoggingEnabled = true

	return nil
}

// StopAccessLogging disables access logging for a container.
func (b *InMemoryBackend) StopAccessLogging(ctx context.Context, name string) error {
	region := regionFromContext(ctx)

	b.mu.Lock("StopAccessLogging")
	defer b.mu.Unlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return ErrContainerNotFound
	}

	c.AccessLoggingEnabled = false

	return nil
}

// TagResource adds or updates tags on a container identified by ARN.
func (b *InMemoryBackend) TagResource(ctx context.Context, resourceARN string, tags map[string]string) error {
	for k := range tags {
		if k == "" {
			return ErrEmptyTagKey
		}
	}

	region := regionFromContext(ctx)

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	name := containerNameFromARN(resourceARN)
	c, ok := b.getContainer(region, name)
	if !ok {
		return ErrContainerNotFound
	}

	if c.Tags == nil {
		c.Tags = make(map[string]string)
	}

	maps.Copy(c.Tags, tags)

	return nil
}

// UntagResource removes tags from a container identified by ARN.
func (b *InMemoryBackend) UntagResource(ctx context.Context, resourceARN string, tagKeys []string) error {
	region := regionFromContext(ctx)

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	name := containerNameFromARN(resourceARN)
	c, ok := b.getContainer(region, name)
	if !ok {
		return ErrContainerNotFound
	}

	for _, k := range tagKeys {
		delete(c.Tags, k)
	}

	return nil
}

// ListTagsForResource returns tags for a container identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(
	ctx context.Context,
	resourceARN string,
) (map[string]string, error) {
	region := regionFromContext(ctx)

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	name := containerNameFromARN(resourceARN)
	c, ok := b.getContainer(region, name)
	if !ok {
		return nil, ErrContainerNotFound
	}

	result := make(map[string]string, len(c.Tags))
	maps.Copy(result, c.Tags)

	return result, nil
}

// copyContainer returns a copy of the Container, copying Tags, CreationTime, CorsPolicy, and MetricPolicy.
// CorsPolicy uses a shallow pointer-slice copy; MetricPolicy is copied by value.
// Rules are immutable after storage so pointer sharing is safe.
func copyContainer(c *Container) *Container {
	if c == nil {
		return nil
	}

	cp := *c

	if c.CreationTime != nil {
		t := *c.CreationTime
		cp.CreationTime = &t
	}

	if c.Tags != nil {
		cp.Tags = make(map[string]string, len(c.Tags))
		maps.Copy(cp.Tags, c.Tags)
	}

	if c.CorsPolicy != nil {
		cp.CorsPolicy = make([]*CorsRule, len(c.CorsPolicy))
		copy(cp.CorsPolicy, c.CorsPolicy)
	}

	if c.MetricPolicy != nil {
		p := *c.MetricPolicy
		if c.MetricPolicy.MetricPolicyRules != nil {
			p.MetricPolicyRules = make([]MetricPolicyRule, len(c.MetricPolicy.MetricPolicyRules))
			copy(p.MetricPolicyRules, c.MetricPolicy.MetricPolicyRules)
		}

		cp.MetricPolicy = &p
	}

	return &cp
}
