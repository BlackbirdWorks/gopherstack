package ocicontainerengine

import "time"

const (
	migrateInProgress = "IN_PROGRESS"
	migratePending    = "PENDING_DECOMMISSION"
	migrateCompleted  = "COMPLETED"
)

// Sweep applies due lifecycle transitions against the backend clock.
func (b *InMemoryBackend) Sweep() {
	b.mu.Lock("Sweep")
	defer b.mu.Unlock()

	b.settleAll()
}

func due(t *time.Time, now time.Time) bool { return t != nil && !t.After(now) }

// settleAll moves every resource whose transition window has passed to its final state; callers hold the lock.
func (b *InMemoryBackend) settleAll() {
	now := b.nowUTC()

	for _, c := range b.clusters {
		b.settleCluster(c, now)
	}

	for _, np := range b.pools {
		settlePool(np, now)
	}
}

func (b *InMemoryBackend) settleCluster(c *Cluster, now time.Time) {
	if due(c.TransitionAt, now) {
		at := *c.TransitionAt
		c.TransitionAt = nil

		switch c.State {
		case stateCreating, stateUpdating:
			c.State = stateActive
			if c.MigrateState == migrateInProgress {
				c.MigrateState = migratePending
			}
		case stateDeleting:
			c.State, c.TimeDeleted = stateDeleted, &at
		}
	}

	if c.MigrateState == migratePending && due(c.MigrateScheduled, now) {
		c.MigrateState, c.Endpoints.Kubernetes = migrateCompleted, ""
	}
}

func settlePool(np *NodePool, now time.Time) {
	if due(np.TransitionAt, now) {
		np.TransitionAt = nil

		switch np.State {
		case stateCreating, stateUpdating:
			np.State = stateActive
		case stateDeleting:
			np.State = stateDeleted
		}
	}

	kept := np.Nodes[:0]

	for _, n := range np.Nodes {
		if due(n.TransitionAt, now) {
			n.TransitionAt = nil

			switch n.State {
			case stateCreating, stateUpdating:
				n.State = stateActive
			case stateDeleting:
				continue
			}
		}

		kept = append(kept, n)
	}

	clear(np.Nodes[len(kept):])
	np.Nodes = kept
}
