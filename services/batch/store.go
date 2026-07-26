package batch

import (
	"context"
	"fmt"
	"maps"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	statusValid = "VALID"
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

const (
	stateEnabled  = "ENABLED"
	stateDisabled = "DISABLED"

	defaultPaginationLimit = 100

	maxTagCount    = 50
	maxTagKeyLen   = 128
	maxTagValueLen = 256
)

// tagsCloneOrEmpty clones a tag map, returning an empty map for nil input.
// Use when building response copies so that JSON serialises as {} not null/absent.
func tagsCloneOrEmpty(tags map[string]string) map[string]string {
	if len(tags) == 0 {
		return map[string]string{}
	}

	return maps.Clone(tags)
}

// validateTags checks tag count and key/value length constraints.
func validateTags(tags map[string]string) error {
	if len(tags) > maxTagCount {
		return fmt.Errorf("%w: too many tags: max %d, got %d", ErrValidation, maxTagCount, len(tags))
	}

	for k, v := range tags {
		if len(k) == 0 || len(k) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key must be 1-%d characters", ErrValidation, maxTagKeyLen)
		}

		if len(v) > maxTagValueLen {
			return fmt.Errorf("%w: tag value must be 0-%d characters", ErrValidation, maxTagValueLen)
		}
	}

	return nil
}

func paginateMapKeys(keys []string, nextToken string, maxResults int32) ([]string, string) {
	p := page.NewHMAC(keys, nextToken, "batch-secret", int(maxResults), defaultPaginationLimit)

	return p.Data, p.Next
}

// describeResourcesPaginated implements the "describe by explicit
// names/ARNs, else paginate over all region-scoped entries sorted by name"
// pattern shared by DescribeComputeEnvironments, DescribeJobQueues, and
// DescribeServiceEnvironments. When names is non-empty, each is resolved via
// lookup (unresolved names are silently skipped, matching AWS's
// filter-not-error behavior for these ops) and no pagination token is
// returned. Otherwise sortedRegionKeys supplies every in-region key sorted by
// name, which is paginated via maxResults/nextToken and resolved one-by-one
// via getByKey. cloneWithTags must return a tag-cloned copy of v (see
// tagsCloneOrEmpty) so callers never leak internal map references. Caller
// must hold at least a read lock.
func describeResourcesPaginated[V any](
	names []string,
	maxResults int32,
	nextToken string,
	lookup func(nameOrARN string) (*V, bool),
	sortedRegionKeys func() []string,
	getByKey func(key string) (*V, bool),
	cloneWithTags func(*V) *V,
) ([]*V, string) {
	if len(names) > 0 {
		list := make([]*V, 0, len(names))

		for _, nameOrARN := range names {
			if v, ok := lookup(nameOrARN); ok {
				list = append(list, cloneWithTags(v))
			}
		}

		return list, ""
	}

	keys, next := paginateMapKeys(sortedRegionKeys(), nextToken, maxResults)
	out := make([]*V, 0, len(keys))

	for _, k := range keys {
		if v, ok := getByKey(k); ok {
			out = append(out, cloneWithTags(v))
		}
	}

	return out, next
}

// regionKey builds the composite store.Table primary key ("region|id") shared
// by every region-nested resource table converted in Phase 3.3 (see
// store_setup.go). Every resource type it is used with carries its own
// unexported region field, populated on create/restore, so the composite key
// and the value's region always agree.
func regionKey(region, id string) string { return region + "|" + id }

// InMemoryBackend stores AWS Batch state in memory.
//
// The eight resource collections below (computeEnvironments, jobQueues,
// jobDefinitions, jobs, consumableResources, schedulingPolicies,
// serviceEnvironments, serviceJobs) were previously nested by region (outer
// key = region, e.g. map[string]map[string]*Job); each is now a single flat
// *store.Table[T] keyed by the composite "region|id" string (see regionKey),
// with a companion *store.Index grouping entries by region for per-region
// scans/pagination -- the region-qualified-table pattern services/emr and
// services/mwaa use (Phase 3.3 of the datalayer refactor). jobs additionally
// carries a "byARN" index (replacing the old jobsByARN region-nested map) and
// a "byQueue" index (replacing jobsByQueue), both maintained automatically by
// store.Table on every Put/Delete/Restore.
//
// jobDefRevisions (a per-region name -> revision-counter map, not a *T map)
// is left as a plain region-nested map since store.Table requires pointer
// identity values; it is persisted directly, unchanged from before.
//
// cesByARN, jqsByARN, crsByARN and ceToQueues are also left as plain maps:
// the first three hold bare strings (no *T identity) and were already
// write-only/dead for reads before this refactor; ceToQueues is a
// non-*T set-of-sets used only for the CE-in-use-by-queue check. None of the
// four are region-nested (a pre-existing quirk, e.g. a same-named CE in two
// regions shares one cesByARN slot) and none are persisted -- both facts
// predate this refactor and are preserved byte-for-byte, not fixed here.
type InMemoryBackend struct {
	mu       *lockmetrics.RWMutex
	registry *store.Registry

	computeEnvironments         *store.Table[ComputeEnvironment]
	computeEnvironmentsByRegion *store.Index[ComputeEnvironment]

	jobQueues         *store.Table[JobQueue]
	jobQueuesByRegion *store.Index[JobQueue]

	jobDefinitions         *store.Table[JobDefinition]
	jobDefinitionsByRegion *store.Index[JobDefinition]

	jobs           *store.Table[Job]
	jobsByRegion   *store.Index[Job]
	jobsByARN      *store.Index[Job]
	jobsByQueueIdx *store.Index[Job]

	consumableResources         *store.Table[ConsumableResource]
	consumableResourcesByRegion *store.Index[ConsumableResource]

	schedulingPolicies         *store.Table[SchedulingPolicy]
	schedulingPoliciesByRegion *store.Index[SchedulingPolicy]
	schedulingPoliciesByName   *store.Index[SchedulingPolicy]

	serviceEnvironments         *store.Table[ServiceEnvironment]
	serviceEnvironmentsByRegion *store.Index[ServiceEnvironment]

	serviceJobs         *store.Table[ServiceJob]
	serviceJobsByRegion *store.Index[ServiceJob]

	quotaShares         *store.Table[QuotaShare]
	quotaSharesByRegion *store.Index[QuotaShare]

	jobDefRevisions map[string]map[string]int32

	cesByARN   map[string]string
	jqsByARN   map[string]string
	crsByARN   map[string]string
	ceToQueues map[string]map[string]struct{}

	region    string
	accountID string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:        store.NewRegistry(),
		jobDefRevisions: make(map[string]map[string]int32),
		cesByARN:        make(map[string]string),
		jqsByARN:        make(map[string]string),
		crsByARN:        make(map[string]string),
		ceToQueues:      make(map[string]map[string]struct{}),
		mu:              lockmetrics.New("batch"),
		accountID:       accountID,
		region:          region,
	}
	registerAllTables(b)

	return b
}

// --- lazy per-region store helper (callers must hold b.mu) ---

// jobDefRevisionsStore is the sole surviving lazy per-region map helper: see
// the InMemoryBackend doc comment for why jobDefRevisions was left as a plain
// map rather than converted to a store.Table.
func (b *InMemoryBackend) jobDefRevisionsStore(region string) map[string]int32 {
	if b.jobDefRevisions[region] == nil {
		b.jobDefRevisions[region] = make(map[string]int32)
	}

	return b.jobDefRevisions[region]
}

// Reset clears all state from the backend.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.jobDefRevisions = make(map[string]map[string]int32)
	b.cesByARN = make(map[string]string)
	b.jqsByARN = make(map[string]string)
	b.crsByARN = make(map[string]string)
	b.ceToQueues = make(map[string]map[string]struct{})
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// sortedNames extracts and sorts a name/id from each value in group. It
// reproduces the deterministic key ordering the old per-region *Idx sorted
// slices provided (see the InMemoryBackend doc comment), now derived at read
// time from a store.Index group instead of maintained incrementally on every
// write.
func sortedNames[V any](group []*V, name func(*V) string) []string {
	out := make([]string, len(group))
	for i, v := range group {
		out[i] = name(v)
	}

	sort.Strings(out)

	return out
}
