package emr

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

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

// InMemoryBackend stores EMR state in memory.
//
// The resource collections below were previously nested by region (outer key
// = region, e.g. map[string]map[string]*Cluster) so that same-named resources
// in different regions were fully isolated. Phase 3.3 of the datalayer
// refactor replaces each of those with a flat *store.Table, keyed by the
// composite "region|id" string (see regionKey), with a companion *store.Index
// grouping entries by region for per-region scans -- the same
// region-qualified-table pattern services/neptune and services/mwaa use. The
// block-public-access configuration/metadata are account-level (one per
// region in AWS, not per-resource), so each is a *store.Table keyed directly
// by region with no secondary index needed. arnIndex
// (map[string]map[string]string) is deliberately NOT converted: store.Table
// requires a *V value with its own identity, but each entry here is a bare
// string with no identifier of its own; it remains a plain region-nested map.
type InMemoryBackend struct {
	clusters                      *store.Table[Cluster]
	clustersByRegion              *store.Index[Cluster]
	arnIndex                      map[string]map[string]string
	securityConfigs               *store.Table[SecurityConfiguration]
	securityConfigsByRegion       *store.Index[SecurityConfiguration]
	studios                       *store.Table[Studio]
	studiosByRegion               *store.Index[Studio]
	studioSessionMappings         *store.Table[StudioSessionMapping]
	studioSessionMappingsByRegion *store.Index[StudioSessionMapping]
	persistentAppUIs              *store.Table[PersistentAppUI]
	notebookExecutions            *store.Table[NotebookExecution]
	notebookExecutionsByRegion    *store.Index[NotebookExecution]
	blockPublicAccess             *store.Table[BlockPublicAccessConfiguration]
	blockPublicAccessMeta         *store.Table[blockPublicAccessMeta]
	registry                      *store.Registry
	mu                            *lockmetrics.RWMutex
	accountID                     string
	region                        string
	counter                       atomic.Int64
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		arnIndex:  make(map[string]map[string]string),
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("emr"),
		registry:  store.NewRegistry(),
	}
	registerAllTables(b)

	return b
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// regionKey builds the composite store.Table primary key ("region|id") shared
// by every region-qualified table registered in store_setup.go.
func regionKey(region, id string) string { return region + "|" + id }

func (b *InMemoryBackend) arnIndexStore(region string) map[string]string {
	if b.arnIndex[region] == nil {
		b.arnIndex[region] = make(map[string]string)
	}

	return b.arnIndex[region]
}

func (b *InMemoryBackend) nextID() string {
	n := b.counter.Add(1)

	return fmt.Sprintf("j-%013d", n)
}

func (b *InMemoryBackend) nextFleetID() string {
	n := b.counter.Add(1)

	return fmt.Sprintf("if-%013d", n)
}

func (b *InMemoryBackend) nextStepID() string {
	n := b.counter.Add(1)

	return fmt.Sprintf("s-%013d", n)
}

func (b *InMemoryBackend) nextStudioID() string {
	n := b.counter.Add(1)

	return fmt.Sprintf("es-%013d", n)
}

func (b *InMemoryBackend) nextPersistentAppUIID() string {
	n := b.counter.Add(1)

	return fmt.Sprintf("pau-%013d", n)
}

// Reset clears all in-memory state from the backend.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.arnIndex = make(map[string]map[string]string)
	b.counter.Store(0)
}

// nextNotebookExecID generates a unique notebook execution ID.
func (b *InMemoryBackend) nextNotebookExecID() string {
	n := b.counter.Add(1)

	return fmt.Sprintf("ex-%013d", n)
}

// nextSessionID generates a unique interactive session ID.
func (b *InMemoryBackend) nextSessionID() string {
	n := b.counter.Add(1)

	return fmt.Sprintf("sess-%013d", n)
}
