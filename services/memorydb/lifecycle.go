package memorydb

import "time"

// -- Lifecycle / intermediate-state support ---------------------------------
//
// Real MemoryDB clusters move through an observable "creating" state before
// settling on "available" (SDK waiters such as WaitUntilClusterAvailable poll
// DescribeClusters until they observe the terminal state), but this backend
// previously jumped straight to "available" on CreateCluster, giving waiters
// nothing to observe.
//
// The mechanism here is intentionally goroutine-free, mirroring
// services/elasticache/lifecycle.go: CreateCluster records a transient
// PendingStatus plus an AvailableAt deadline on the Cluster. Every read path
// overlays the transient status until the wall clock (backend clock,
// injectable for deterministic tests) passes AvailableAt, after which the
// terminal Status is reported. By default lifecycleDelay is zero, so
// transitions are instant -- the pre-existing behavior every current test
// relies on is unchanged unless a test opts in via SetLifecycleDelay.

// statusCreating is the transient status string for a cluster mid-creation,
// matching the real MemoryDB Cluster.Status enum value.
const statusCreating = "creating"

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

// SetLifecycleDelay configures how long a newly created cluster dwells in the
// "creating" state before reaching "available". Zero (the default) means the
// transition is instant. Safe for concurrent use.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lifecycleDelay = d
}

// SetClock overrides the backend clock. Primarily for deterministic tests
// that need to advance time past a transition deadline without sleeping.
// Passing nil restores time.Now. Safe for concurrent use.
func (b *InMemoryBackend) SetClock(clock func() time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.clock = clock
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

// markCreatingLocked records a "creating" transition on c when a lifecycle
// delay is configured. Must hold b.mu.
func (b *InMemoryBackend) markCreatingLocked(c *Cluster) {
	if d := b.pendingUntil(); !d.IsZero() {
		c.PendingStatus = statusCreating
		c.AvailableAt = d
	}
}

// clusterView returns a clone of c with its observable (overlaid) status,
// used for every read/write response that surfaces a Cluster on the wire.
func (b *InMemoryBackend) clusterView(c *Cluster) *Cluster {
	cp := cloneCluster(c)
	cp.Status = overlayStatus(b.now(), c.Status, c.PendingStatus, c.AvailableAt)

	return cp
}
