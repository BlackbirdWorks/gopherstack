package vpclattice

import "time"

// SetLifecycleDelay sets how long created services, target groups,
// associations and registered targets stay in CREATE_IN_PROGRESS / INITIAL
// (and deletions in DELETE_IN_PROGRESS / DRAINING). The default 0 settles
// instantly.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.lifecycleDelay = d
}

// pendingUntil returns the settle deadline for a new transition, or the zero
// time when the delay is 0 and the resource settles instantly.
func (b *InMemoryBackend) pendingUntil() time.Time {
	if b.lifecycleDelay <= 0 {
		return time.Time{}
	}

	return time.Now().UTC().Add(b.lifecycleDelay)
}

func createStatus(until time.Time) string {
	if until.IsZero() {
		return statusActive
	}

	return statusCreateInProgress
}

func due(now, until time.Time) bool { return !until.IsZero() && !now.Before(until) }

// settleLocked advances every elapsed transition. Caller holds the write lock.
func (b *InMemoryBackend) settleLocked() {
	if b.lifecycleDelay <= 0 {
		return
	}

	now := time.Now().UTC()

	b.settleServicesLocked(now)
	b.settleTargetGroupsLocked(now)
	b.settleAssociationsLocked(now)
	b.settleTargetsLocked(now)
}

func (b *InMemoryBackend) settleServicesLocked(now time.Time) {
	for _, svc := range b.services.All() {
		switch {
		case !due(now, svc.StatusUntil):
		case svc.Status == statusDeleteInProgress:
			b.removeServiceLocked(svc)
		default:
			svc.Status, svc.StatusUntil = statusActive, time.Time{}
		}
	}
}

func (b *InMemoryBackend) settleTargetGroupsLocked(now time.Time) {
	for _, tg := range b.targetGroups.All() {
		switch {
		case !due(now, tg.StatusUntil):
		case tg.Status == statusDeleteInProgress:
			b.removeTargetGroupLocked(tg)
		default:
			tg.Status, tg.StatusUntil = tgStatusActive, time.Time{}
		}
	}
}

func (b *InMemoryBackend) settleAssociationsLocked(now time.Time) {
	for _, s := range b.snsas.All() {
		if due(now, s.StatusUntil) {
			s.Status, s.StatusUntil = statusActive, time.Time{}
		}
	}

	for _, s := range b.snvas.All() {
		if due(now, s.StatusUntil) {
			s.Status, s.StatusUntil = statusActive, time.Time{}
		}
	}
}

func (b *InMemoryBackend) settleTargetsLocked(now time.Time) {
	for tgID, targets := range b.targets {
		kept := targets[:0]

		for _, t := range targets {
			if due(now, t.StatusUntil) {
				if t.Status == targetStatusDraining {
					continue
				}

				t.Status, t.StatusUntil = targetStatusHealthy, time.Time{}
			}

			kept = append(kept, t)
		}

		b.targets[tgID] = kept
	}
}
