// Package databrew implements an in-memory AWS Glue DataBrew service backend.
package databrew

import (
	"context"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
// DataBrew resources are isolated per region: every backend operation resolves the
// caller's region from the request context and operates only on that region's
// nested store.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// InMemoryBackend stores DataBrew state in memory.
//
// All resource collections are nested by region (outer key = region) so that
// same-named resources are isolated across regions. datasets/recipes/
// projects/jobs/rulesets/schedules each hold one *store.Table[T] per region,
// created lazily via the *Table accessors in store_setup.go — mirroring the
// lazy map creation the hand-rolled *Store helpers did before the Phase 3.3
// pkgs/store conversion (see store_setup.go's package doc for why jobRuns is
// NOT converted). Callers must hold b.mu while accessing any of them.
type InMemoryBackend struct {
	svcCtx    context.Context
	schedules map[string]*store.Table[Schedule]
	projects  map[string]*store.Table[Project]
	jobs      map[string]*store.Table[Job]
	// jobRuns holds one map[jobName][]*JobRun per region. It is left as a
	// plain (non-store.Table) map: each job's run history is an
	// ORDER-SENSITIVE slice (chronological append order; ListJobRuns reverses
	// it), and store.Table/store.Index do not preserve insertion order — see
	// pkgs/store's package doc and .claude/memories on this rollout.
	jobRuns map[string]map[string][]*JobRun
	// recipeVersions holds one map[recipeName][]*Recipe (published version
	// snapshots, oldest first) per region, for the same reason jobRuns is a
	// plain map: PublishRecipe APPENDS a new numbered snapshot, and
	// ListRecipeVersions/DescribeRecipe(RecipeVersion=LATEST_PUBLISHED) need
	// the highest (most recently appended) entry -- an order-sensitive
	// operation store.Table/store.Index can't express. The "recipes" table
	// itself (b.recipes) holds only the LATEST_WORKING draft, one row per
	// recipe name; see recipes.go.
	recipeVersions map[string]map[string][]*Recipe
	// registry is the lifecycle registry for every *store.Table this backend
	// owns; Reset/Snapshot/Restore drive it via ResetAll/SnapshotAll/RestoreAll
	// instead of hand-written per-map boilerplate. See store_setup.go.
	registry      *store.Registry
	rulesets      map[string]*store.Table[Ruleset]
	datasets      map[string]*store.Table[Dataset]
	mu            *lockmetrics.RWMutex
	recipes       map[string]*store.Table[Recipe]
	cancel        context.CancelFunc
	accountID     string
	defaultRegion string
	wg            sync.WaitGroup
}

// NewInMemoryBackend creates a new in-memory DataBrew backend with a background
// lifecycle context.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return NewInMemoryBackendWithContext(context.Background(), accountID, region)
}

// NewInMemoryBackendWithContext creates a new in-memory DataBrew backend whose
// delayed lifecycle goroutines are tied to svcCtx. When svcCtx (or the backend's
// Shutdown) is cancelled, in-flight transition goroutines exit promptly.
// If svcCtx is nil, [context.Background] is used.
func NewInMemoryBackendWithContext(
	svcCtx context.Context,
	accountID, region string,
) *InMemoryBackend {
	if svcCtx == nil {
		svcCtx = context.Background()
	}
	ctx, cancel := context.WithCancel(svcCtx)

	return &InMemoryBackend{
		registry:       store.NewRegistry(),
		datasets:       make(map[string]*store.Table[Dataset]),
		recipes:        make(map[string]*store.Table[Recipe]),
		projects:       make(map[string]*store.Table[Project]),
		jobs:           make(map[string]*store.Table[Job]),
		jobRuns:        make(map[string]map[string][]*JobRun),
		recipeVersions: make(map[string]map[string][]*Recipe),
		rulesets:       make(map[string]*store.Table[Ruleset]),
		schedules:      make(map[string]*store.Table[Schedule]),
		mu:             lockmetrics.New("databrew"),
		accountID:      accountID,
		defaultRegion:  region,
		svcCtx:         ctx,
		cancel:         cancel,
	}
}

// jobRunsStore returns the per-region jobName -> runs map, lazily creating it.
// Callers must hold b.mu.
func (b *InMemoryBackend) jobRunsStore(region string) map[string][]*JobRun {
	if b.jobRuns[region] == nil {
		b.jobRuns[region] = make(map[string][]*JobRun)
	}

	return b.jobRuns[region]
}

// jobRunsStoreRO returns the per-region jobName -> runs map for region
// without mutating the outer map. Safe to call while holding only
// b.mu.RLock(): if the region has not been observed yet, it returns a fresh,
// unregistered, empty map instead of lazily creating (and persisting) an
// entry.
func (b *InMemoryBackend) jobRunsStoreRO(region string) map[string][]*JobRun {
	if v := b.jobRuns[region]; v != nil {
		return v
	}

	return map[string][]*JobRun{}
}

// recipeVersionsStore returns the per-region recipeName -> published version
// snapshots map, lazily creating it. Callers must hold b.mu.
func (b *InMemoryBackend) recipeVersionsStore(region string) map[string][]*Recipe {
	if b.recipeVersions[region] == nil {
		b.recipeVersions[region] = make(map[string][]*Recipe)
	}

	return b.recipeVersions[region]
}

// recipeVersionsStoreRO returns the per-region recipeName -> published
// version snapshots map for region without mutating the outer map. Safe to
// call while holding only b.mu.RLock(): if the region has not been observed
// yet, it returns a fresh, unregistered, empty map instead of lazily
// creating (and persisting) an entry.
func (b *InMemoryBackend) recipeVersionsStoreRO(region string) map[string][]*Recipe {
	if v := b.recipeVersions[region]; v != nil {
		return v
	}

	return map[string][]*Recipe{}
}

// runDelayed schedules fn to run after delay on a tracked goroutine. The
// goroutine exits without invoking fn if the backend's lifecycle context is
// cancelled (Shutdown) before the delay elapses, preventing leaks and
// post-Shutdown state mutation.
func (b *InMemoryBackend) runDelayed(delay time.Duration, fn func()) {
	b.wg.Go(func() {
		t := time.NewTimer(delay)
		defer t.Stop()

		select {
		case <-b.svcCtx.Done():
			return
		case <-t.C:
		}
		fn()
	})
}

// Shutdown cancels the backend's lifecycle context and waits for in-flight
// delayed goroutines to finish, bounded by ctx. After Shutdown the backend
// no longer schedules state transitions.
func (b *InMemoryBackend) Shutdown(ctx context.Context) {
	if b.cancel != nil {
		b.cancel()
	}

	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}
}

func (b *InMemoryBackend) Region() string    { return b.defaultRegion }
func (b *InMemoryBackend) AccountID() string { return b.accountID }

func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()
	b.registry.ResetAll()
	b.jobRuns = make(map[string]map[string][]*JobRun)
	b.recipeVersions = make(map[string]map[string][]*Recipe)
}
