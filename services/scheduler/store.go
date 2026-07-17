package scheduler

import (
	"context"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// InMemoryBackend stores schedules and schedule groups nested by region (multiple
// regions can each have a same-named schedule/group, fully isolated from one another).
// Phase 3.3 of the datalayer refactor replaces the region-nested
// map[string]map[string]*T pair for each resource with a single flat *store.Table[T],
// keyed by the composite "region|id" string (see regionKey below), with companion
// *store.Index values grouping entries by region (replacing the old outer map
// nesting) and by "region|ARN" (replacing the old per-region ARN reverse map used by
// TagResource/UntagResource/ListTagsForResource). See store_setup.go.
//
// Schedule already carries its own exported Region field (part of the AWS wire
// shape), so schedules is registered directly off that field. ScheduleGroup has no
// such field -- its ARN always embeds the region it was created in (see
// CreateScheduleGroup/seedDefaultGroup), so scheduleGroupRegionOf derives it from
// there instead of adding a new field.
//
// Both tables are "dirty" in the store_setup.go/persistence.go sense: their Tags
// field is a live *tags.Tags (backed by a Prometheus-instrumented map), which
// persistence.go swaps for a plain map[string]string via a DTO on Snapshot/Restore
// to preserve the existing per-resource Prometheus metric naming -- so neither
// table is registered on a shared b.registry; each is built with store.New only.
type InMemoryBackend struct {
	schedules              *store.Table[Schedule]
	schedulesByRegion      *store.Index[Schedule]
	schedulesByARN         *store.Index[Schedule]
	scheduleGroups         *store.Table[ScheduleGroup]
	scheduleGroupsByRegion *store.Index[ScheduleGroup]
	scheduleGroupsByARN    *store.Index[ScheduleGroup]
	mu                     *lockmetrics.RWMutex
	accountID              string
	region                 string
}

func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("scheduler"),
	}
	registerAllTables(b)
	// Seed the built-in "default" group in the default region.
	b.ensureRegionGroupsSeeded(region)

	return b
}

// regionKey builds the composite store.Table primary key ("region|id") shared by
// both region-qualified tables registered in store_setup.go.
func regionKey(region, id string) string { return region + "|" + id }

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset clears all in-memory state and re-seeds the default schedule group
// in the backend's default region.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, s := range b.schedules.All() {
		if s.Tags != nil {
			s.Tags.Close()
		}
	}

	for _, g := range b.scheduleGroups.All() {
		if g.Tags != nil {
			g.Tags.Close()
		}
	}

	b.schedules.Reset()
	b.scheduleGroups.Reset()
	// Re-seed the built-in "default" group in the default region.
	b.ensureRegionGroupsSeeded(b.region)
}

// paginate slices a sorted list by maxResults starting after nextToken.
// keyFn extracts the opaque token key from each element.
// Returns the page and the continuation token (empty when no more results).
func paginate[T any](list []T, keyFn func(T) string, nextToken string, maxResults int) ([]T, string) {
	if nextToken != "" {
		start := 0

		for i, item := range list {
			if keyFn(item) == nextToken {
				start = i + 1

				break
			}
		}

		list = list[start:]
	}

	if maxResults <= 0 || maxResults >= len(list) {
		return list, ""
	}

	page := list[:maxResults]

	return page, keyFn(page[len(page)-1])
}

// regionFromARN extracts the region component (index 3) from an AWS ARN
// (arn:partition:service:region:account:resource), falling back to defaultRegion.
func regionFromARN(resourceARN, defaultRegion string) string {
	parts := strings.Split(resourceARN, ":")
	const regionIndex = 3
	if len(parts) > regionIndex && parts[regionIndex] != "" {
		return parts[regionIndex]
	}

	return defaultRegion
}
