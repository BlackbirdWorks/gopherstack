package redshift

import (
	"context"
	"time"
)

// Cluster lifecycle status constants used by the reconciler state machine.
const (
	clusterStatusCreating  = "creating"
	clusterStatusDeleting  = "deleting"
	clusterStatusModifying = "modifying"

	// defaultReconcileInterval is how often the background reconciler wakes to
	// advance due cluster-state transitions when no explicit interval is set.
	defaultReconcileInterval = 250 * time.Millisecond
)

// clusterTransition describes a scheduled state change for a single cluster.
//
// Transitions are stored out-of-band from the Cluster struct (in
// InMemoryBackend.clusterTransitions) so they never leak into cloned/serialized
// cluster values, and so a single managed reconciler can advance every cluster
// without a goroutine per cluster.
type clusterTransition struct {
	// effectiveAt is the wall-clock time at or after which the transition applies.
	effectiveAt time.Time
	// status is the target status to set once the transition fires. Ignored
	// when remove is true.
	status string
	// remove indicates the cluster should be deleted (not restatused) when the
	// transition fires — used to model asynchronous DeleteCluster.
	remove bool
}

// scheduleClusterTransitionLocked records a pending state change for a cluster.
// Callers MUST hold b.mu for writing. A later transition for the same cluster
// overwrites any earlier pending one (e.g. a delete supersedes a pending
// creating→available flip), matching AWS's last-write-wins lifecycle.
func (b *InMemoryBackend) scheduleClusterTransitionLocked(id string, tr *clusterTransition) {
	if b.clusterTransitions == nil {
		b.clusterTransitions = make(map[string]*clusterTransition)
	}

	b.clusterTransitions[id] = tr
}

// advanceClusterStates applies every cluster transition whose effective time has
// passed. It first scans under a read lock and only upgrades to a write lock when
// there is work to do, keeping the common (no-op) case cheap. It is safe for
// concurrent use and is called both lazily on reads (so SDK waiters always observe
// the true state) and periodically by the background reconciler.
func (b *InMemoryBackend) advanceClusterStates(now time.Time) {
	b.mu.RLock("advanceClusterStates.scan")

	due := false

	for _, tr := range b.clusterTransitions {
		if !now.Before(tr.effectiveAt) {
			due = true

			break
		}
	}

	b.mu.RUnlock()

	if !due {
		return
	}

	b.mu.Lock("advanceClusterStates.apply")
	defer b.mu.Unlock()

	b.applyDueTransitionsLocked(now)
}

// applyDueTransitionsLocked mutates cluster state for all transitions that are
// due. Callers MUST hold b.mu for writing. Re-checking effectiveAt under the
// write lock makes the operation idempotent when both a lazy read and the
// reconciler race to advance the same cluster.
func (b *InMemoryBackend) applyDueTransitionsLocked(now time.Time) {
	for id, tr := range b.clusterTransitions {
		if now.Before(tr.effectiveAt) {
			continue
		}

		delete(b.clusterTransitions, id)

		c, ok := b.clusters.Get(id)
		if !ok {
			continue
		}

		if tr.remove {
			c.Tags.Close()
			b.clusters.Delete(id)

			if b.dnsRegistrar != nil {
				b.dnsRegistrar.Deregister(c.Endpoint)
			}

			continue
		}

		c.Status = tr.status
	}
}

// StartReconciler starts the managed background reconciler that advances cluster
// lifecycle transitions (creating→available, modifying→available, deleting→gone).
// It replaces the previous per-cluster unmanaged goroutine with a single goroutine
// owning a stop channel and WaitGroup, so it can be cancelled deterministically via
// ctx or StopReconciler. Calling it while already running is a no-op.
//
// ctx originates from the service framework's background-worker lifecycle, so no
// context.Background() is introduced here.
func (b *InMemoryBackend) StartReconciler(ctx context.Context) {
	b.reconcileMu.Lock()
	defer b.reconcileMu.Unlock()

	if b.reconcileOn {
		return
	}

	if b.reconcileInterval <= 0 {
		b.reconcileInterval = defaultReconcileInterval
	}

	stop := make(chan struct{})
	b.reconcileStop = stop
	b.reconcileOn = true

	b.reconcileWG.Add(1)

	go b.reconcileLoop(ctx, stop, b.reconcileInterval)
}

// reconcileLoop is the reconciler goroutine body. It exits cleanly when either the
// context is cancelled or the stop channel is closed, then signals the WaitGroup.
func (b *InMemoryBackend) reconcileLoop(ctx context.Context, stop <-chan struct{}, interval time.Duration) {
	defer b.reconcileWG.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-ticker.C:
			b.advanceClusterStates(time.Now())
		}
	}
}

// StopReconciler signals the reconciler to stop and blocks until the goroutine has
// exited, guaranteeing no leaked goroutine survives shutdown. It is idempotent.
func (b *InMemoryBackend) StopReconciler() {
	b.reconcileMu.Lock()

	if !b.reconcileOn {
		b.reconcileMu.Unlock()

		return
	}

	b.reconcileOn = false
	close(b.reconcileStop)
	b.reconcileMu.Unlock()

	b.reconcileWG.Wait()
}
