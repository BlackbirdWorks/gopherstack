package codestarconnections

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
// the ARN. This mirrors services/kafka's conversion (ARN already globally
// unique, so the outer region layer of the old map was redundant). Both are
// registered directly on b.registry.
//
// repositoryLinks, syncConfigurations, repositorySyncStatuses,
// resourceSyncStatuses, and syncBlockers are "dirty": their own identity
// (RepositoryLinkID; ResourceName+SyncType; RepositoryLinkID+Branch+SyncType;
// ResourceName+SyncType; ID) carries no region of its own, AND (unlike
// Connection/Host) their Get/Delete/Update lookups are scoped by the
// caller's context region rather than by any ARN -- see e.g.
// GetRepositoryLink/UpdateSyncBlocker in backend.go. Each therefore carries
// an unexported region-qualifying field, is keyed by a composite
// "region|id" string (see regionKey in backend.go), and is built with
// store.New only -- deliberately NOT store.Register-ed onto b.registry, so
// registry.SnapshotAll/RestoreAll/ResetAll never touch them directly.
// persistence.go instead builds an ephemeral DTO registry (the
// services/neptune regionalDTO / services/codecommit dirty-table pattern)
// that gives each hidden field a real JSON tag, and InMemoryBackend.Reset
// resets each of the five explicitly.
//
// connectionsByName/hostsByName/syncBlockersByResource -- the old ARN/ID
// reverse-lookup maps -- are replaced by secondary *store.Index values
// (byName, byResource) kept consistent automatically by store.Table's
// Put/Delete/Restore; they need no persistence of their own.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func connectionKeyFn(v *Connection) string { return v.ConnectionArn }

func connectionRegionIndexKeyFn(v *Connection) string { return regionFromARN(v.ConnectionArn, "") }

func connectionNameIndexKeyFn(v *Connection) string {
	return regionKey(regionFromARN(v.ConnectionArn, ""), v.ConnectionName)
}

func hostKeyFn(v *Host) string { return v.HostArn }

func hostRegionIndexKeyFn(v *Host) string { return regionFromARN(v.HostArn, "") }

func hostNameIndexKeyFn(v *Host) string {
	return regionKey(regionFromARN(v.HostArn, ""), v.Name)
}

func repositoryLinkKeyFn(v *RepositoryLink) string { return regionKey(v.region, v.RepositoryLinkID) }

func repositoryLinkRegionIndexKeyFn(v *RepositoryLink) string { return v.region }

func syncConfigurationKeyFn(v *SyncConfiguration) string {
	return regionKey(v.region, syncConfigKey(v.ResourceName, v.SyncType))
}

func syncConfigurationRegionIndexKeyFn(v *SyncConfiguration) string { return v.region }

func repositorySyncStatusKeyFn(v *RepositorySyncStatus) string {
	return regionKey(v.region, repositorySyncStatusKey(v.repositoryLinkID, v.branch, v.syncType))
}

func resourceSyncStatusKeyFn(v *ResourceSyncStatus) string {
	return regionKey(v.region, syncConfigKey(v.resourceName, v.syncType))
}

func syncBlockerKeyFn(v *SyncBlocker) string { return v.ID }

func syncBlockerResourceIndexKeyFn(v *SyncBlocker) string {
	return regionKey(v.region, syncConfigKey(v.ResourceName, v.SyncType))
}

// registerAllTables registers connections/hosts on b.registry and
// constructs the five dirty tables (store.New only -- see the file doc
// comment above for why they are deliberately not store.Register-ed).
// It must be called during construction only (immediately after b.registry
// is created -- see NewInMemoryBackend), never on every Reset(): store.
// Register panics on a duplicate name, so runtime resets go through
// b.registry.ResetAll() plus explicit Reset() calls on the five dirty
// tables instead (see InMemoryBackend.Reset in backend.go).
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

	b.repositorySyncStatuses = store.New(repositorySyncStatusKeyFn)

	b.resourceSyncStatuses = store.New(resourceSyncStatusKeyFn)

	b.syncBlockers = store.New(syncBlockerKeyFn)
	b.syncBlockersByResource = b.syncBlockers.AddIndex("byResource", syncBlockerResourceIndexKeyFn)
}
