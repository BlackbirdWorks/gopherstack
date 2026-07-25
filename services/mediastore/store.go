package mediastore

import (
	"context"
	"maps"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	// containerStatusActive is the status for a ready container.
	containerStatusActive = "ACTIVE"
	// containerStatusCreating is the transient status a container holds
	// between CreateContainer and the end of its (simulated) activation
	// delay -- see InMemoryBackend.activationDelay / SetActivationDelay.
	containerStatusCreating = "CREATING"
	// containerStatusDeleting is the transient status a container holds
	// between DeleteContainer and the end of its (simulated) deletion
	// delay, after which it is actually removed from the store.
	containerStatusDeleting = "DELETING"
	// defaultEndpointFormat is the format for the container endpoint.
	defaultEndpointFormat = "https://%s.data.mediastore.%s.amazonaws.com"
	// maxContainerNameLen is the maximum allowed length of a container name.
	maxContainerNameLen = 255
	// maxMetricPolicyRules is the maximum number of metric policy rules.
	maxMetricPolicyRules = 5
	// maxObjectGroupLen is the maximum length of MetricPolicyRule.ObjectGroup,
	// per the ObjectGroup shape's `max: 900` trait in the MediaStore botocore
	// model (models/apis/mediastore/2017-09-01/api-2.json).
	maxObjectGroupLen = 900
	// maxObjectGroupNameLen is the maximum length of
	// MetricPolicyRule.ObjectGroupName, per the ObjectGroupName shape's
	// `max: 30` trait in the same model.
	maxObjectGroupNameLen = 30
	// maxCorsPolicyRules is the maximum number of rules in a CORS policy, per
	// the CorsPolicy list shape's `max: 100` trait in the same model.
	maxCorsPolicyRules = 100
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

// containerTransition describes a scheduled lifecycle-status change for a
// single container. Transitions are stored out-of-band from the Container
// struct (in InMemoryBackend.containerTransitions) so they never leak into
// cloned/persisted Container values, matching the pattern used by
// services/redshift's clusterTransition.
type containerTransition struct {
	// effectiveAt is the wall-clock time at or after which the transition
	// applies.
	effectiveAt time.Time
	// status is the target status to set once the transition fires. Ignored
	// when remove is true.
	status string
	// remove indicates the container should be deleted (not restatused) when
	// the transition fires -- used to model asynchronous DeleteContainer.
	remove bool
}

// InMemoryBackend is the in-memory implementation of StorageBackend.
//
// State is partitioned by region: containers[region] gives the store.Table of
// containers stored within that region (see store_setup.go for why this is a
// per-region map rather than a single flat table).
type InMemoryBackend struct {
	containers           map[string]*store.Table[Container]
	containerTransitions map[string]*containerTransition
	mu                   *lockmetrics.RWMutex
	paginationSecret     string
	// activationDelay controls how long CreateContainer/DeleteContainer stay
	// in the transient CREATING/DELETING status before completing, simulating
	// real AWS's asynchronous container lifecycle. It defaults to zero
	// (transitions apply synchronously), matching this repo's house
	// convention for lightweight, fast-provisioning resources -- see
	// services/efs's fsActivationDelay and services/redshift's
	// clusterActivationDelay -- of keeping async-lifecycle simulation an
	// explicit opt-in via SetActivationDelay rather than default-on, since
	// default-on would slow down every caller that creates a container.
	activationDelay time.Duration
}

// NewInMemoryBackend creates a new in-memory MediaStore backend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		containers:           make(map[string]*store.Table[Container]),
		containerTransitions: make(map[string]*containerTransition),
		paginationSecret:     uuid.NewString(),
		mu:                   lockmetrics.New("mediastore"),
	}
}

// SetActivationDelay configures the transient CREATING/DELETING window
// simulated by CreateContainer/DeleteContainer (see InMemoryBackend.
// activationDelay). A zero delay (the default) makes container-lifecycle
// transitions synchronous, matching this backend's previous always-instant
// behavior; a positive delay makes them genuinely observable, matching real
// AWS, so DescribeContainer/ListContainers polling loops (SDK waiters) see
// the transient state until the delay elapses.
func (b *InMemoryBackend) SetActivationDelay(d time.Duration) {
	b.mu.Lock("SetActivationDelay")
	defer b.mu.Unlock()
	b.activationDelay = d
}

// containerTransitionKey builds the per-region transition map key for a
// container. Containers are unique only within a region (see
// store_setup.go), so the key must include region to avoid collisions
// between same-named containers in different regions.
func containerTransitionKey(region, name string) string { return region + "|" + name }

// scheduleContainerTransitionLocked records a pending lifecycle transition
// for a container. Callers MUST hold b.mu for writing. A later transition
// for the same container overwrites any earlier pending one (e.g. a delete
// supersedes a pending creating->active transition), matching AWS's
// last-write-wins lifecycle.
func (b *InMemoryBackend) scheduleContainerTransitionLocked(region, name string, tr *containerTransition) {
	b.containerTransitions[containerTransitionKey(region, name)] = tr
}

// advanceContainerStates applies every container transition whose effective
// time has passed. It first scans under a read lock and only upgrades to a
// write lock when there is work to do, keeping the common (no-delay-
// configured) case cheap. It is safe for concurrent use and is called at the
// top of every read/mutate path that returns container Status
// (DescribeContainer, ListContainers, CreateContainer, DeleteContainer) so
// SDK waiters always observe the true state -- there is no background
// goroutine driving this (see the leaks note in PARITY.md), only lazy
// advancement on access.
func (b *InMemoryBackend) advanceContainerStates(now time.Time) {
	b.mu.RLock("advanceContainerStates.scan")

	due := false

	for _, tr := range b.containerTransitions {
		if !now.Before(tr.effectiveAt) {
			due = true

			break
		}
	}

	b.mu.RUnlock()

	if !due {
		return
	}

	b.mu.Lock("advanceContainerStates.apply")
	defer b.mu.Unlock()

	b.applyDueContainerTransitionsLocked(now)
}

// applyDueContainerTransitionsLocked mutates container state for all
// transitions that are due. Callers MUST hold b.mu for writing. Re-checking
// effectiveAt under the write lock makes the operation idempotent when
// multiple callers race to advance the same due transition.
func (b *InMemoryBackend) applyDueContainerTransitionsLocked(now time.Time) {
	for key, tr := range b.containerTransitions {
		if now.Before(tr.effectiveAt) {
			continue
		}

		region, name, ok := splitContainerTransitionKey(key)
		if !ok {
			delete(b.containerTransitions, key)

			continue
		}

		tbl := b.containerRegion(region)
		if tbl != nil {
			if c, exists := tbl.Get(name); exists {
				if tr.remove {
					tbl.Delete(name)
				} else {
					c.Status = tr.status
				}
			}
		}

		delete(b.containerTransitions, key)
	}
}

// splitContainerTransitionKey reverses containerTransitionKey. Region names
// never contain '|', so the first separator unambiguously divides region
// from container name.
func splitContainerTransitionKey(key string) (string, string, bool) {
	for i := range key {
		if key[i] == '|' {
			return key[:i], key[i+1:], true
		}
	}

	return "", "", false
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
