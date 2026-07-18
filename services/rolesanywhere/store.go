package rolesanywhere

import (
	"context"
	"sort"
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

// InMemoryBackend implements StorageBackend using in-memory maps.
//
// trustAnchors, profiles, crls, and subjects were previously
// map[region]map[id]*T; Phase 3.3 replaces each with a flat *store.Table
// keyed by the composite "region|id" string (see regionKey), with a
// companion *store.Index grouping entries by region -- see store_setup.go's
// registerAllTables doc for the full rationale and why all four are "dirty"
// (unregistered on registry) tables. tags, attributeMappings, and
// notificationSettings remain plain region-nested maps: each holds a slice
// value ([]TagEntry / []AttributeMapping / []NotificationSetting), not a
// *T, so there is nothing for store.Table to key on.
type InMemoryBackend struct {
	mu                   *lockmetrics.RWMutex
	registry             *store.Registry
	trustAnchors         *store.Table[TrustAnchor]
	trustAnchorsByRegion *store.Index[TrustAnchor]
	profiles             *store.Table[Profile]
	profilesByRegion     *store.Index[Profile]
	crls                 *store.Table[Crl]
	crlsByRegion         *store.Index[Crl]
	subjects             *store.Table[Subject]
	subjectsByRegion     *store.Index[Subject]
	tags                 map[string]map[string][]TagEntry            // region → resourceARN → tags
	attributeMappings    map[string]map[string][]AttributeMapping    // region → profileID → mappings
	notificationSettings map[string]map[string][]NotificationSetting // region → trustAnchorID → settings
	accountID            string
	defaultRegion        string
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:                   lockmetrics.New("rolesanywhere"),
		accountID:            accountID,
		defaultRegion:        region,
		registry:             store.NewRegistry(),
		tags:                 make(map[string]map[string][]TagEntry),
		attributeMappings:    make(map[string]map[string][]AttributeMapping),
		notificationSettings: make(map[string]map[string][]NotificationSetting),
	}

	registerAllTables(b)

	return b
}

// regionKey builds the composite store.Table primary key ("region|id") used
// by trustAnchors, profiles, crls, and subjects.
func regionKey(region, id string) string { return region + "|" + id }

// ---- per-region lazy store helpers (for the maps left unconverted) ----

func (b *InMemoryBackend) tagsStore(region string) map[string][]TagEntry {
	if b.tags[region] == nil {
		b.tags[region] = make(map[string][]TagEntry)
	}

	return b.tags[region]
}

func (b *InMemoryBackend) attributeMappingsStore(region string) map[string][]AttributeMapping {
	if b.attributeMappings[region] == nil {
		b.attributeMappings[region] = make(map[string][]AttributeMapping)
	}

	return b.attributeMappings[region]
}

func (b *InMemoryBackend) notificationSettingsStore(region string) map[string][]NotificationSetting {
	if b.notificationSettings[region] == nil {
		b.notificationSettings[region] = make(map[string][]NotificationSetting)
	}

	return b.notificationSettings[region]
}

// listByRegionIndex is a generic helper for paginated listing of region-keyed
// resources held in a *store.Index. It reads idx.Get(region), copies each
// item, sorts by sortKey, then paginates.
func listByRegionIndex[T any](
	idx *store.Index[T],
	region string,
	copyFn func(*T) *T,
	sortKey func(*T) string,
	getID func(*T) string,
	pageToken string,
	maxResults int,
) ([]*T, string) {
	group := idx.Get(region)
	all := make([]*T, 0, len(group))

	for _, item := range group {
		all = append(all, copyFn(item))
	}

	sort.Slice(all, func(i, j int) bool {
		return sortKey(all[i]) < sortKey(all[j])
	})

	start, next := paginate(all, pageToken, maxResults, getID)

	return all[start:next], nextTokenFromSlice(all, next, getID)
}

// ---- Lifecycle ----

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	// trustAnchors, profiles, crls, and subjects are "dirty" tables
	// deliberately NOT on b.registry (see store_setup.go's registerAllTables
	// doc), so each needs its own Reset() call here rather than going
	// through b.registry.ResetAll().
	b.registry.ResetAll()
	b.trustAnchors.Reset()
	b.profiles.Reset()
	b.crls.Reset()
	b.subjects.Reset()
	b.tags = make(map[string]map[string][]TagEntry)
	b.attributeMappings = make(map[string]map[string][]AttributeMapping)
	b.notificationSettings = make(map[string]map[string][]NotificationSetting)
}

// Region returns the backend's default region.
func (b *InMemoryBackend) Region() string { return b.defaultRegion }

// AccountID returns the backend's account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Snapshot and Restore (implementing persistence.Persistable) live in
// persistence.go, alongside the "dirty" DTO registry they need for
// trustAnchors/profiles/crls/subjects (see store_setup.go's registerAllTables
// doc for why those four tables are dirty).

// ---- pagination helpers ----

// paginate returns the start and end indices for a page of results.
// T must be a pointer type. getID extracts the ID used as a page token.
func paginate[T any](all []T, pageToken string, maxResults int, getID func(T) string) (int, int) {
	start := 0

	if pageToken != "" {
		for i, item := range all {
			if getID(item) == pageToken {
				start = i

				break
			}
		}
	}

	end := len(all)

	if maxResults > 0 && start+maxResults < end {
		end = start + maxResults
	}

	return start, end
}

// nextTokenFromSlice returns the ID of the element at index next (the first
// item of the next page), or "" when next is at/after the end of the slice and
// there are no further pages. The page token therefore identifies the first
// item of the following page, which paginate() locates via getID.
func nextTokenFromSlice[T any](all []T, next int, getID func(T) string) string {
	if next < 0 || next >= len(all) {
		return ""
	}

	return getID(all[next])
}
