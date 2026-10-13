package eks

import "time"

// resumeTransitionsLocked re-arms the settle timers for resources a snapshot captured mid-transition,
// since timers are not persisted. Must be called with b.mu held.
func (b *InMemoryBackend) resumeTransitionsLocked() {
	if b.clusterEng == nil {
		for _, c := range b.clusters.All() {
			if c.Status == statusCreating && c.ConnectorConfig == nil {
				b.scheduleClusterActivation(c.Name)
			}
		}
	}

	b.resumeNodegroupsLocked()
	b.resumeAddonsLocked()

	for _, u := range b.updates.All() {
		if u.Status == statusInProgress {
			b.scheduleUpdateTransition(u.ClusterName, u.ID)
		}
	}
}

func (b *InMemoryBackend) resumeNodegroupsLocked() {
	for _, ng := range b.nodegroups.All() {
		if ng.Status != statusCreating {
			continue
		}

		key := nodegroupKey(ng.ClusterName, ng.NodegroupName)

		b.scheduleStatusFlip("NodegroupResume", nodegroupTransitionDelay, func() {
			if n, ok := b.nodegroups.Get(key); ok && n.Status == statusCreating {
				n.Status = statusActive
			}
		})
	}
}

func (b *InMemoryBackend) resumeAddonsLocked() {
	for _, a := range b.addons.All() {
		if a.Status != statusCreating {
			continue
		}

		key := addonKey(a.ClusterName, a.AddonName)

		b.scheduleStatusFlip("AddonResume", addonTransitionDelay, func() {
			if cur, ok := b.addons.Get(key); ok && cur.Status == statusCreating {
				cur.Status = statusActive
			}
		})
	}
}

func (b *InMemoryBackend) scheduleStatusFlip(component string, d time.Duration, fn func()) {
	b.work.After(component, b.dwell(d), func() {
		b.mu.Lock(component)
		defer b.mu.Unlock()

		fn()
	})
}
