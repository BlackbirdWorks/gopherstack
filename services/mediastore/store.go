package mediastore

import (
	"context"
	"maps"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
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
