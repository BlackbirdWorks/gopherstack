package cloudfront

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// validateInvalidationPaths checks that every path starts with '/', there are no more than
// maxInvalidationPaths, and wildcards are only used as trailing '/*' on a segment.
func validateInvalidationPaths(paths []string) error {
	if len(paths) > maxInvalidationPaths {
		return fmt.Errorf(
			"%w: too many invalidation paths: %d (max %d)",
			ErrValidation, len(paths), maxInvalidationPaths,
		)
	}

	for _, p := range paths {
		if !strings.HasPrefix(p, "/") {
			return fmt.Errorf("%w: invalidation path must start with '/': %q", ErrValidation, p)
		}
		// Wildcard must be the final segment and the entire segment (e.g. /foo/* is OK, /foo*bar is not).
		if strings.Contains(p, "*") {
			if !strings.HasSuffix(p, "/*") && p != "/*" {
				return fmt.Errorf(
					"%w: wildcard in invalidation path must be trailing '/*': %q",
					ErrValidation, p,
				)
			}
		}
	}

	return nil
}

// CountInProgressInvalidations returns the number of in-progress invalidations for a distribution.
func (b *InMemoryBackend) CountInProgressInvalidations(distributionID string) int {
	b.mu.RLock("CountInProgressInvalidations")
	defer b.mu.RUnlock()

	count := 0
	for _, inv := range b.invalidationsByDist.Get(distributionID) {
		if inv.Status == statusInProgress {
			count++
		}
	}

	return count
}

// CreateInvalidation creates a new cache invalidation for the given distribution.
func (b *InMemoryBackend) CreateInvalidation(
	distributionID, callerRef string,
	paths []string,
) (*Invalidation, error) {
	if callerRef == "" {
		return nil, fmt.Errorf("%w: CallerReference must not be empty", ErrValidation)
	}

	if err := validateInvalidationPaths(paths); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateInvalidation")
	defer b.mu.Unlock()

	if _, ok := b.distributions.Get(distributionID); !ok {
		return nil, fmt.Errorf("%w: distribution %s not found", ErrNotFound, distributionID)
	}

	const invalidationDelay = 100 * time.Millisecond

	now := time.Now().UTC()
	inv := &Invalidation{
		ID:         generateID(),
		Status:     statusInProgress,
		CreateTime: now,
		Paths:      append([]string(nil), paths...),
		CallerRef:  callerRef,
		distID:     distributionID,
	}
	b.invalidations.Put(inv)

	if b.invalidationReadyAt[distributionID] == nil {
		b.invalidationReadyAt[distributionID] = make(map[string]time.Time)
	}

	b.invalidationReadyAt[distributionID][inv.ID] = now.Add(invalidationDelay)

	cp := *inv
	cp.Paths = append([]string(nil), inv.Paths...)

	return &cp, nil
}

// ListInvalidations returns all invalidations for a distribution, sorted by ID.
func (b *InMemoryBackend) ListInvalidations(distributionID string) ([]*Invalidation, error) {
	b.mu.RLock("ListInvalidations")
	defer b.mu.RUnlock()

	if _, ok := b.distributions.Get(distributionID); !ok {
		return nil, fmt.Errorf("%w: distribution %s not found", ErrNotFound, distributionID)
	}

	src := b.invalidationsByDist.Get(distributionID)
	out := make([]*Invalidation, 0, len(src))

	for _, inv := range src {
		cp := *inv
		cp.Paths = append([]string(nil), inv.Paths...)
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out, nil
}

// GetInvalidation returns a specific invalidation by distribution ID and invalidation ID.
func (b *InMemoryBackend) GetInvalidation(
	distributionID, invalidationID string,
) (*Invalidation, error) {
	b.mu.RLock("GetInvalidation")
	defer b.mu.RUnlock()

	if _, ok := b.distributions.Get(distributionID); !ok {
		return nil, fmt.Errorf("%w: distribution %s not found", ErrNotFound, distributionID)
	}

	inv, ok := b.invalidations.Get(invalidationKey(distributionID, invalidationID))
	if !ok {
		return nil, fmt.Errorf("%w: invalidation %s not found", ErrInvalidationNotFound, invalidationID)
	}

	cp := *inv
	cp.Paths = append([]string(nil), inv.Paths...)

	return &cp, nil
}
