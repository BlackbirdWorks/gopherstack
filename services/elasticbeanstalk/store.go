package elasticbeanstalk

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
// Elastic Beanstalk resources are isolated per region: every backend operation resolves
// the caller's region from the request context and operates only on that region's store.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// InMemoryBackend stores AWS Elastic Beanstalk state in memory.
//
// applications, environments, appVersions, configTemplates, and
// platformVersions were previously nested by region (outer key = region,
// e.g. map[string]map[string]*Application). Phase 3.3 of the datalayer
// refactor replaces each with a flat *store.Table, keyed by the composite
// "region|id" string (see regionKey), with companion *store.Index instances
// for per-region scans and ARN/name/CNAME reverse lookups -- see
// store_setup.go for the full rationale and every keyFn.
// managedActionHistory, events, and envCounters are deliberately NOT
// converted: they are slice- or scalar-valued maps with no single-value-
// per-key shape to model as a store.Table, so they remain plain region-nested
// maps, unchanged by this refactor.
type InMemoryBackend struct {
	applications             *store.Table[Application]
	applicationsByRegion     *store.Index[Application]
	applicationsByARN        *store.Index[Application]
	environments             *store.Table[Environment]
	environmentsByRegion     *store.Index[Environment]
	environmentsByARN        *store.Index[Environment]
	environmentsByName       *store.Index[Environment]
	environmentsByCNAME      *store.Index[Environment]
	appVersions              *store.Table[ApplicationVersion]
	appVersionsByRegion      *store.Index[ApplicationVersion]
	appVersionsByARN         *store.Index[ApplicationVersion]
	configTemplates          *store.Table[ConfigurationTemplate]
	configTemplatesByRegion  *store.Index[ConfigurationTemplate]
	platformVersions         *store.Table[PlatformVersion]
	platformVersionsByRegion *store.Index[PlatformVersion]
	registry                 *store.Registry
	managedActionHistory     map[string]map[string][]*ManagedActionHistory // region → envName → history items
	events                   map[string][]*EventRecord                     // region → events
	envCounters              map[string]int                                // region → counter
	mu                       *lockmetrics.RWMutex
	accountID                string
	region                   string // default region
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		managedActionHistory: make(map[string]map[string][]*ManagedActionHistory),
		events:               make(map[string][]*EventRecord),
		envCounters:          make(map[string]int),
		accountID:            accountID,
		region:               region,
		mu:                   lockmetrics.New("elasticbeanstalk"),
		registry:             store.NewRegistry(),
	}
	registerAllTables(b)
	b.initRegion(region)

	return b
}

// initRegion pre-initializes the raw (non-store.Table) per-region sub-map so
// that managedActionHistoryStore never writes under an RLock (which would
// race with parallel readers). The five store.Table-backed collections need
// no such pre-initialization: their composite keys are created lazily and
// safely by store.Table itself.
func (b *InMemoryBackend) initRegion(region string) {
	if b.managedActionHistory[region] == nil {
		b.managedActionHistory[region] = make(map[string][]*ManagedActionHistory)
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// regionKey builds the composite store.Table primary key ("region|id") shared
// by every region-qualified table registered in store_setup.go.
func regionKey(region, id string) string { return region + "|" + id }

// Reset clears all backend state, resetting to an empty store.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.managedActionHistory = make(map[string]map[string][]*ManagedActionHistory)
	b.events = make(map[string][]*EventRecord)
	b.envCounters = make(map[string]int)
	b.initRegion(b.region)
}
