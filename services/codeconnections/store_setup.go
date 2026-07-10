package codeconnections

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]map[string]*T resource field on InMemoryBackend (nested
// region -> key -> value, to isolate same-named resources across regions) is
// replaced by a *store.Table[T].
//
// connections and hosts are "clean": Connection.ConnectionArn and
// Host.HostArn already embed their own region
// (arn:partition:service:region:account:resource, see regionFromARN), so
// each is keyed directly by its own ARN with no hidden field needed --
// region isolation for List*/duplicate-name checks falls out of the
// byRegion/byName secondary indexes below, which derive their group key from
// the ARN. Get/Delete/Update additionally re-check regionFromARN(arn) against
// the caller's context region (see backend.go) to preserve the old per-region
// map's strict isolation (an ARN created in one region must not resolve from
// another). Both are registered directly on b.registry.
//
// repositoryLinks and syncConfigurations are "dirty": their own identity
// (RepositoryLinkID; ResourceName+SyncType) carries no region of its own,
// AND (unlike Connection/Host) their Get/Delete/Update lookups are scoped by
// the caller's context region rather than by any ARN -- see e.g.
// GetRepositoryLink/GetSyncConfiguration in backend.go. Each therefore
// carries an unexported region-qualifying field, is keyed by a composite
// "region|id" string (see regionKey in backend.go), and is built with
// store.New only -- deliberately NOT store.Register-ed onto b.registry, so
// registry.SnapshotAll/RestoreAll/ResetAll never touch them directly.
// persistence.go instead builds an ephemeral DTO registry (the
// services/codestarconnections regionalDTO pattern) that gives the hidden
// region field a real JSON tag, and InMemoryBackend.Reset resets each of the
// two explicitly.
//
// connectionsByName/hostsByName -- the old ARN reverse-lookup maps -- are
// replaced by secondary *store.Index values (byName) kept consistent
// automatically by store.Table's Put/Delete/Restore; they need no
// persistence of their own.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func connectionKeyFn(v *Connection) string { return v.ConnectionArn }

func connectionRegionIndexKeyFn(v *Connection) string { return regionFromARN(v.ConnectionArn) }

func connectionNameIndexKeyFn(v *Connection) string {
	return regionKey(regionFromARN(v.ConnectionArn), v.ConnectionName)
}

func hostKeyFn(v *Host) string { return v.HostArn }

func hostRegionIndexKeyFn(v *Host) string { return regionFromARN(v.HostArn) }

func hostNameIndexKeyFn(v *Host) string {
	return regionKey(regionFromARN(v.HostArn), v.Name)
}

func repositoryLinkKeyFn(v *RepositoryLink) string { return regionKey(v.region, v.RepositoryLinkID) }

func repositoryLinkRegionIndexKeyFn(v *RepositoryLink) string { return v.region }

func syncConfigurationKeyFn(v *SyncConfiguration) string {
	return regionKey(v.region, syncConfigKey(v.ResourceName, v.SyncType))
}

func syncConfigurationRegionIndexKeyFn(v *SyncConfiguration) string { return v.region }

// registerAllTables registers connections/hosts on b.registry and
// constructs the two dirty tables (store.New only -- see the file doc
// comment above for why they are deliberately not store.Register-ed). It
// must be called during construction only (immediately after b.registry is
// created -- see NewInMemoryBackend), never on every Reset(): store.Register
// panics on a duplicate name, so runtime resets go through
// b.registry.ResetAll() plus explicit Reset() calls on the two dirty tables
// instead (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.connections = store.Register(b.registry, "connections", store.New(connectionKeyFn))
	b.connectionsByRegion = b.connections.AddIndex("byRegion", connectionRegionIndexKeyFn)
	b.connectionsByName = b.connections.AddIndex("byName", connectionNameIndexKeyFn)

	b.hosts = store.Register(b.registry, "hosts", store.New(hostKeyFn))
	b.hostsByRegion = b.hosts.AddIndex("byRegion", hostRegionIndexKeyFn)
	b.hostsByName = b.hosts.AddIndex("byName", hostNameIndexKeyFn)

	b.repositoryLinks = store.New(repositoryLinkKeyFn)
	b.repositoryLinksByRegion = b.repositoryLinks.AddIndex("byRegion", repositoryLinkRegionIndexKeyFn)

	b.syncConfigurations = store.New(syncConfigurationKeyFn)
	b.syncConfigurationsByRegion = b.syncConfigurations.AddIndex("byRegion", syncConfigurationRegionIndexKeyFn)
}
