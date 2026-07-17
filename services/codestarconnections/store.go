// Package codestarconnections provides an in-memory implementation of the AWS CodeStar Connections service.
package codestarconnections

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

// InMemoryBackend is a thread-safe in-memory store for CodeStar Connections resources.
//
// connections and hosts are "clean" store.Table collections (see
// store_setup.go): each is keyed directly by its own ARN, which already
// embeds its region, so region isolation falls out of the byRegion/byName
// indexes with no hidden fields needed. repositoryLinks, syncConfigurations,
// repositorySyncStatuses, resourceSyncStatuses, and syncBlockers are "dirty":
// their identity (RepositoryLinkID; ResourceName+SyncType;
// RepositoryLinkID+Branch+SyncType; ResourceName+SyncType; ID) carries no
// region of its own, and lookups for the first four are scoped by the
// caller's context region (not by any embedded ARN region), so each type
// carries an unexported region-qualifying field and is registered with a
// composite "region|id" key. connections/hosts are registered on registry;
// the five dirty tables are NOT (store.New only), so they are excluded from
// registry.ResetAll() and must be reset explicitly -- see Reset below and
// persistence.go's mixed clean/dirty Snapshot/Restore.
type InMemoryBackend struct {
	mu       *lockmetrics.RWMutex
	registry *store.Registry

	connections         *store.Table[Connection]
	connectionsByRegion *store.Index[Connection]
	connectionsByName   *store.Index[Connection]

	hosts         *store.Table[Host]
	hostsByRegion *store.Index[Host]
	hostsByName   *store.Index[Host]

	repositoryLinks         *store.Table[RepositoryLink]
	repositoryLinksByRegion *store.Index[RepositoryLink]

	syncConfigurations         *store.Table[SyncConfiguration]
	syncConfigurationsByRegion *store.Index[SyncConfiguration]

	repositorySyncStatuses *store.Table[RepositorySyncStatus]

	resourceSyncStatuses *store.Table[ResourceSyncStatus]

	syncBlockers           *store.Table[SyncBlocker]
	syncBlockersByResource *store.Index[SyncBlocker]

	accountID     string
	defaultRegion string
}

// NewInMemoryBackend creates a new backend for the given account and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		accountID:     accountID,
		defaultRegion: region,
		mu:            lockmetrics.New("codestarconnections"),
		registry:      store.NewRegistry(),
	}

	registerAllTables(b)

	return b
}

// Reset clears all state in the backend.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	// The five dirty tables (see store_setup.go's registerAllTables doc) are
	// deliberately NOT on b.registry, so each needs its own Reset() call here.
	b.repositoryLinks.Reset()
	b.syncConfigurations.Reset()
	b.repositorySyncStatuses.Reset()
	b.resourceSyncStatuses.Reset()
	b.syncBlockers.Reset()
}

// Region returns the default region for this backend instance.
func (b *InMemoryBackend) Region() string { return b.defaultRegion }

// AccountID returns the account ID for this backend instance.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// AddConnectionInternal seeds a connection directly for testing.
func (b *InMemoryBackend) AddConnectionInternal(conn *Connection) {
	b.mu.Lock("AddConnectionInternal")
	defer b.mu.Unlock()

	b.connections.Put(conn)
}

// AddHostInternal seeds a host directly for testing.
func (b *InMemoryBackend) AddHostInternal(host *Host) {
	b.mu.Lock("AddHostInternal")
	defer b.mu.Unlock()

	b.hosts.Put(host)
}

// AddRepositoryLinkInternal seeds a repository link directly for testing.
func (b *InMemoryBackend) AddRepositoryLinkInternal(ctx context.Context, link *RepositoryLink) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("AddRepositoryLinkInternal")
	defer b.mu.Unlock()

	link.region = region
	b.repositoryLinks.Put(link)
}
