package vpclattice

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// ------- Target operations -------

// RegisterTargets registers targets to a target group.
func (b *InMemoryBackend) RegisterTargets(
	tgID string,
	targets []*Target,
) ([]*TargetFailure, error) {
	b.mu.Lock("RegisterTargets")
	defer b.mu.Unlock()

	id, ok := b.resolveTargetGroupID(tgID)
	if !ok {
		return nil, ErrNotFound
	}

	failures := make([]*TargetFailure, 0)
	existing := b.targets[id]

	for _, t := range targets {
		// check for duplicate
		dup := false
		for _, e := range existing {
			if e.ID == t.ID && e.Port == t.Port {
				dup = true

				break
			}
		}

		if dup {
			failures = append(failures, &TargetFailure{
				ID:      t.ID,
				Port:    t.Port,
				Code:    "TARGET_ALREADY_REGISTERED",
				Message: "Target already registered",
			})

			continue
		}

		existing = append(existing, &storedTarget{
			ID:     t.ID,
			Port:   t.Port,
			Status: targetStatusHealthy,
		})
	}

	b.targets[id] = existing

	return failures, nil
}

// targetMatches reports whether a stored target matches a deregistration
// request: same ID, and either the request is port-agnostic (Port == 0) or
// the ports match exactly.
func targetMatches(e *storedTarget, t *Target) bool {
	return e.ID == t.ID && (t.Port == 0 || e.Port == t.Port)
}

// missingTargetFailures returns a TargetFailure for every requested target
// that has no matching entry in existing.
func missingTargetFailures(existing []*storedTarget, targets []*Target) []*TargetFailure {
	failures := make([]*TargetFailure, 0)

	for _, t := range targets {
		found := false

		for _, e := range existing {
			if targetMatches(e, t) {
				found = true

				break
			}
		}

		if !found {
			failures = append(failures, &TargetFailure{
				ID:      t.ID,
				Port:    t.Port,
				Code:    "TARGET_NOT_FOUND",
				Message: "Target not registered",
			})
		}
	}

	return failures
}

// remainingTargets returns the subset of existing not matched by any
// requested target, i.e. the targets that survive deregistration.
func remainingTargets(existing []*storedTarget, targets []*Target) []*storedTarget {
	remaining := make([]*storedTarget, 0, len(existing))

	for _, e := range existing {
		remove := false

		for _, t := range targets {
			if targetMatches(e, t) {
				remove = true

				break
			}
		}

		if !remove {
			remaining = append(remaining, e)
		}
	}

	return remaining
}

// DeregisterTargets deregisters targets from a target group.
func (b *InMemoryBackend) DeregisterTargets(
	tgID string,
	targets []*Target,
) ([]*TargetFailure, error) {
	b.mu.Lock("DeregisterTargets")
	defer b.mu.Unlock()

	id, ok := b.resolveTargetGroupID(tgID)
	if !ok {
		return nil, ErrNotFound
	}

	existing := b.targets[id]
	failures := missingTargetFailures(existing, targets)
	b.targets[id] = remainingTargets(existing, targets)

	return failures, nil
}

// ListTargets lists registered targets for a target group.
func (b *InMemoryBackend) ListTargets(
	_ context.Context,
	tgID string,
	filters []Target,
	maxResults int32,
	nextToken string,
) ([]*TargetSummary, string, error) {
	b.mu.RLock("ListTargets")
	defer b.mu.RUnlock()

	id, ok := b.resolveTargetGroupID(tgID)
	if !ok {
		return nil, "", ErrNotFound
	}

	targets := b.targets[id]
	all := make([]*TargetSummary, 0, len(targets))

	for _, t := range targets {
		all = append(all, &TargetSummary{
			ID:     t.ID,
			Port:   t.Port,
			Status: t.Status,
		})
	}

	if len(filters) > 0 {
		filtered := all[:0]

		for _, t := range all {
			for _, f := range filters {
				if f.ID == t.ID && (f.Port == 0 || f.Port == t.Port) {
					filtered = append(filtered, t)

					break
				}
			}
		}

		all = filtered
	}

	p := page.New(all, nextToken, int(maxResults), defaultMaxResults)

	return p.Data, p.Next, nil
}
