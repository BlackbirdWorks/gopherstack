package codeconnections

import (
	"context"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
// CodeConnections resources are isolated per region: every backend operation resolves
// the caller's region from the request context and operates only on that region's
// resources.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// regionFromARN extracts the region component (index 3) from an AWS ARN
// (arn:partition:service:region:account:resource), returning "" if the ARN is
// malformed (which then simply fails to match any real region in every
// caller below -- see e.g. GetConnection). Connection.ConnectionArn and
// Host.HostArn are always built via arn.Build with the same region the
// resource was created in (see CreateConnection/CreateHost), so this is
// equivalent to -- and replaces -- the old outer "region" map key those two
// resource families used to be nested under.
func regionFromARN(resourceARN string) string {
	parts := strings.Split(resourceARN, ":")
	const regionIndex = 3
	if len(parts) > regionIndex {
		return parts[regionIndex]
	}

	return ""
}

// regionKey returns the composite store.Table primary key ("region|id") used
// by repositoryLinks/syncConfigurations (see store_setup.go). Neither has an
// ARN of its own to derive a region from (unlike Connection/Host), so each
// carries an unexported region field set at creation time and combined with
// its own identity via this helper.
func regionKey(region, id string) string {
	return region + "|" + id
}

// InMemoryBackend is the in-memory store for AWS CodeConnections resources.
//
// connections and hosts are "clean" store.Table collections (see
// store_setup.go): each is keyed directly by its own ARN, which already
// embeds its region, so region isolation for Get/Delete/List falls out of
// the byRegion secondary index below, which derives its group key from the
// ARN. Both are registered directly on registry. repositoryLinks and
// syncConfigurations are "dirty": their own
// identity (RepositoryLinkID; ResourceName+SyncType) carries no region of its
// own, and lookups are scoped by the caller's context region rather than by
// any ARN, so each carries an unexported region-qualifying field and is
// registered with a composite "region|id" key (see regionKey). They are built
// with store.New only -- deliberately NOT store.Register-ed onto registry --
// so registry.ResetAll()/SnapshotAll()/RestoreAll() never touch them
// directly; see Reset below and persistence.go's mixed clean/dirty
// Snapshot/Restore.
type InMemoryBackend struct {
	mu       *lockmetrics.RWMutex
	registry *store.Registry

	connections         *store.Table[Connection]
	connectionsByRegion *store.Index[Connection]

	hosts         *store.Table[Host]
	hostsByRegion *store.Index[Host]

	repositoryLinks         *store.Table[RepositoryLink]
	repositoryLinksByRegion *store.Index[RepositoryLink]

	syncConfigurations         *store.Table[SyncConfiguration]
	syncConfigurationsByRegion *store.Index[SyncConfiguration]

	syncBlockers           *store.Table[SyncBlocker]
	syncBlockersByResource *store.Index[SyncBlocker]

	accountID     string
	defaultRegion string
}

// NewInMemoryBackend creates a new in-memory CodeConnections backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		accountID:     accountID,
		defaultRegion: region,
		mu:            lockmetrics.New("codeconnections"),
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
	// repositoryLinks/syncConfigurations/syncBlockers (see store_setup.go's
	// registerAllTables doc) are deliberately NOT on b.registry, so each needs
	// its own Reset() call.
	b.repositoryLinks.Reset()
	b.syncConfigurations.Reset()
	b.syncBlockers.Reset()
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.defaultRegion }
