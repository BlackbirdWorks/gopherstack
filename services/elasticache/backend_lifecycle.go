package elasticache

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// ----------------------------------------
// Lifecycle / intermediate-state support
// ----------------------------------------
//
// Real ElastiCache resources move through observable intermediate states
// (creating -> available, modifying, deleting, snapshotting, ...) before
// settling on a terminal state. SDK waiters (e.g. WaitUntilCacheClusterAvailable)
// poll Describe* until they observe the terminal state, so an emulator that
// jumps straight to "available" gives waiters nothing to observe.
//
// The mechanism here is intentionally goroutine-free: a resource records a
// transient PendingStatus plus an AvailableAt deadline. Every read path overlays
// the transient status until the wall clock (backend clock, injectable for
// deterministic tests) passes AvailableAt, after which the terminal status is
// reported. "deleting" resources vanish from Describe once their deadline passes
// and are reaped lazily by the next write op — no background workers, no leaks.

// Transient lifecycle status strings (lower-case, matching the AWS query API).
const (
	statusCreating    = "creating"
	statusModifying   = "modifying"
	statusDeleting    = "deleting"
	statusRebooting   = "rebooting cache cluster nodes"
	statusFailingOver = "modifying"
)

// now returns the backend's current time, honouring an injected clock.
func (b *InMemoryBackend) now() time.Time {
	if b.clock != nil {
		return b.clock()
	}

	return time.Now()
}

// pendingUntil returns the deadline for a freshly-started transition, or the
// zero time when no lifecycle delay is configured (transitions complete
// instantly, preserving the default fast behaviour existing tests rely on).
func (b *InMemoryBackend) pendingUntil() time.Time {
	if b.lifecycleDelay <= 0 {
		return time.Time{}
	}

	return b.now().Add(b.lifecycleDelay)
}

// SetLifecycleDelay configures how long resources dwell in an intermediate
// state before reaching their terminal state. Zero (the default) means
// transitions are instant. It is safe for concurrent use.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	b.lifecycleDelay = d
	b.mu.Unlock()
}

// SetClock overrides the backend clock. Primarily for deterministic tests that
// need to advance time past a transition deadline without sleeping. Passing nil
// restores time.Now. Safe for concurrent use.
func (b *InMemoryBackend) SetClock(clock func() time.Time) {
	b.mu.Lock("SetClock")
	b.clock = clock
	b.mu.Unlock()
}

// overlayStatus returns the observable status for a resource: the transient
// pending status while the deadline is in the future, otherwise the terminal
// status.
func overlayStatus(now time.Time, terminal, pending string, until time.Time) string {
	if pending != "" && now.Before(until) {
		return pending
	}

	return terminal
}

// isReaped reports whether a "deleting" resource has passed its deletion
// deadline and should be treated as gone (hidden from Describe, reaped on the
// next write).
func isReaped(now time.Time, pending string, until time.Time) bool {
	return pending == statusDeleting && !now.Before(until)
}

// lifecycleAccess exposes the transient-status fields of a resource so a single
// generic finalize routine can serve every lifecycle type.
type lifecycleAccess[T any] struct {
	get func(*T) (status, pending string, until time.Time)
	set func(*T, string)
}

func clusterAccess() lifecycleAccess[Cluster] {
	return lifecycleAccess[Cluster]{
		get: func(c *Cluster) (string, string, time.Time) { return c.Status, c.PendingStatus, c.AvailableAt },
		set: func(c *Cluster, s string) { c.Status = s },
	}
}

func replicationGroupAccess() lifecycleAccess[ReplicationGroup] {
	return lifecycleAccess[ReplicationGroup]{
		get: func(r *ReplicationGroup) (string, string, time.Time) { return r.Status, r.PendingStatus, r.AvailableAt },
		set: func(r *ReplicationGroup, s string) { r.Status = s },
	}
}

func snapshotAccess() lifecycleAccess[CacheSnapshot] {
	return lifecycleAccess[CacheSnapshot]{
		get: func(c *CacheSnapshot) (string, string, time.Time) { return c.Status, c.PendingStatus, c.AvailableAt },
		set: func(c *CacheSnapshot, s string) { c.Status = s },
	}
}

func serverlessCacheAccess() lifecycleAccess[ServerlessCache] {
	return lifecycleAccess[ServerlessCache]{
		get: func(s *ServerlessCache) (string, string, time.Time) { return s.Status, s.PendingStatus, s.AvailableAt },
		set: func(s *ServerlessCache, v string) { s.Status = v },
	}
}

// finalizeLifecyclePage hides reaped resources, applies the status overlay, and
// maps a single-item lookup of a reaped resource to notFound.
func finalizeLifecyclePage[T any](
	now time.Time, id string, p page.Page[T], err, notFound error, acc lifecycleAccess[T],
) (page.Page[T], error) {
	if err != nil {
		return p, err
	}

	if id != "" && len(p.Data) == 1 {
		_, pending, until := acc.get(&p.Data[0])
		if isReaped(now, pending, until) {
			return page.Page[T]{}, notFound
		}
	}

	kept := p.Data[:0]
	for i := range p.Data {
		item := &p.Data[i]
		status, pending, until := acc.get(item)
		if isReaped(now, pending, until) {
			continue
		}
		acc.set(item, overlayStatus(now, status, pending, until))
		kept = append(kept, *item)
	}
	p.Data = kept

	return p, nil
}

func (b *InMemoryBackend) finalizeClusterPage(
	id string, p page.Page[Cluster], err error,
) (page.Page[Cluster], error) {
	return finalizeLifecyclePage(b.now(), id, p, err, ErrClusterNotFound, clusterAccess())
}

func (b *InMemoryBackend) finalizeReplicationGroupPage(
	id string, p page.Page[ReplicationGroup], err error,
) (page.Page[ReplicationGroup], error) {
	return finalizeLifecyclePage(b.now(), id, p, err, ErrReplicationGroupNotFound, replicationGroupAccess())
}

func (b *InMemoryBackend) finalizeSnapshotPage(
	id string, p page.Page[CacheSnapshot], err error,
) (page.Page[CacheSnapshot], error) {
	return finalizeLifecyclePage(b.now(), id, p, err, ErrSnapshotNotFound, snapshotAccess())
}

func (b *InMemoryBackend) finalizeServerlessCachePage(
	id string, p page.Page[ServerlessCache], err error,
) (page.Page[ServerlessCache], error) {
	return finalizeLifecyclePage(b.now(), id, p, err, ErrServerlessCacheNotFound, serverlessCacheAccess())
}

// clusterView returns a copy of c with its observable (overlaid) status, used
// for the synchronous responses of create/modify/reboot operations.
func (b *InMemoryBackend) clusterView(c *Cluster) *Cluster {
	cp := *c
	cp.Status = overlayStatus(b.now(), c.Status, c.PendingStatus, c.AvailableAt)

	return &cp
}

// replicationGroupView returns a copy of rg with its observable status.
func (b *InMemoryBackend) replicationGroupView(rg *ReplicationGroup) *ReplicationGroup {
	cp := *rg
	cp.Status = overlayStatus(b.now(), rg.Status, rg.PendingStatus, rg.AvailableAt)

	return &cp
}

// serverlessCacheView returns a copy of sc with its observable status.
func (b *InMemoryBackend) serverlessCacheView(sc *ServerlessCache) *ServerlessCache {
	cp := *sc
	cp.Status = overlayStatus(b.now(), sc.Status, sc.PendingStatus, sc.AvailableAt)

	return &cp
}

// globalReplicationGroupView returns a copy of grg with its observable status.
func (b *InMemoryBackend) globalReplicationGroupView(grg *GlobalReplicationGroup) *GlobalReplicationGroup {
	cp := *grg
	cp.Status = overlayStatus(b.now(), grg.Status, grg.PendingStatus, grg.AvailableAt)

	return &cp
}

// markCreatingLocked records a "creating" transition on the given status/deadline
// fields when a lifecycle delay is configured. Must hold b.mu.
func (b *InMemoryBackend) markCreatingLocked(pending *string, until *time.Time) {
	if d := b.pendingUntil(); !d.IsZero() {
		*pending = statusCreating
		*until = d
	}
}

// markTransitionLocked records an in-place transition (modifying/snapshotting/…)
// on the given fields when a lifecycle delay is configured. Must hold b.mu.
func (b *InMemoryBackend) markTransitionLocked(pending *string, until *time.Time, status string) {
	if d := b.pendingUntil(); !d.IsZero() {
		*pending = status
		*until = d
	}
}

// pruneRegionLocked reaps resources whose "deleting" deadline has elapsed,
// releasing their engines/ports/tags/DNS registrations. Must hold b.mu.
// This keeps tombstones from accumulating without any background goroutine.
func (b *InMemoryBackend) pruneRegionLocked(region string) {
	now := b.now()

	for id, c := range b.clusters[region] {
		if isReaped(now, c.PendingStatus, c.AvailableAt) {
			b.releaseClusterLocked(c)
			delete(b.clusters[region], id)
		}
	}
	for id, rg := range b.replicationGroups[region] {
		if isReaped(now, rg.PendingStatus, rg.AvailableAt) {
			rg.Tags.Close()
			delete(b.replicationGroups[region], id)
		}
	}
	for id, sc := range b.serverlessCaches[region] {
		if isReaped(now, sc.PendingStatus, sc.AvailableAt) {
			sc.Tags.Close()
			delete(b.serverlessCaches[region], id)
		}
	}
	for id, snap := range b.snapshots[region] {
		if isReaped(now, snap.PendingStatus, snap.AvailableAt) {
			snap.Tags.Close()
			delete(b.snapshots[region], id)
		}
	}
	for id, grg := range b.globalReplicationGroups {
		if isReaped(now, grg.PendingStatus, grg.AvailableAt) {
			grg.Tags.Close()
			b.deleteGlobalReplicationGroup(id)
		}
	}
}

// releaseClusterLocked frees a cluster's engine, port, tags, and DNS name.
// Must hold b.mu.
func (b *InMemoryBackend) releaseClusterLocked(c *Cluster) {
	if b.dnsRegistrar != nil && c.Endpoint != "" {
		b.dnsRegistrar.Deregister(c.Endpoint)
	}
	if c.mini != nil {
		c.mini.Close()
	}
	if b.allocator != nil && c.AllocatedPort > 0 {
		_ = b.allocator.Release(c.AllocatedPort)
	}
	if c.Tags != nil {
		c.Tags.Close()
	}
}
