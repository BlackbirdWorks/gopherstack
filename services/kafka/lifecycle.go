package kafka

import "time"

// SetLifecycleDelay sets how long a new cluster reports CREATING and how long an update
// operation keeps the cluster UPDATING; zero (the default) settles instantly.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()
	b.lifecycleDelay = d
}

// SetClock overrides the lifecycle clock; nil restores time.Now.
func (b *InMemoryBackend) SetClock(clock func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()
	b.clock = clock
}

func (b *InMemoryBackend) now() time.Time {
	if b.clock != nil {
		return b.clock()
	}

	return time.Now()
}

// viewCluster clones c with its observable state: pending until readyAt, then ACTIVE.
func (b *InMemoryBackend) viewCluster(c *Cluster) *Cluster {
	cp := cloneCluster(c)

	switch {
	case !c.readyAt.IsZero() && b.now().Before(c.readyAt):
		cp.State = c.pendingState
		cp.ActiveOperationArn = c.pendingOpArn
	case b.lifecycleDelay > 0 && c.State == ClusterStateCreating:
		cp.State = ClusterStateActive
	}

	return cp
}

// viewOperation clones op, completing an in-progress operation once its deadline passes.
func (b *InMemoryBackend) viewOperation(op *ClusterOperation) *ClusterOperation {
	cp := cloneClusterOperation(op)

	if op.OperationState == ClusterOperationStateUpdateInProgress && !op.awaitBroker && !b.now().Before(op.doneAt) {
		cp.OperationState = ClusterOperationStateUpdateComplete
		cp.EndTime = op.doneAt.UTC().Format(time.RFC3339)
	}

	return cp
}
