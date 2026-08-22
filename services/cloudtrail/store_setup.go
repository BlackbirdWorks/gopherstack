package cloudtrail

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is replaced with a
// *store.Table[T], following the pattern established by services/codebuild
// (data-driven, direct registration -- see pkgs/store's package doc for the
// underlying primitive).
//
// Every convertible value type here already carries its own identity as a
// real (non-json:"-") field -- Trail.Name, Channel.ChannelID,
// Dashboard.DashboardID, EventDataStore.EventDataStoreID, Query.QueryID,
// ResourcePolicy.ResourceARN, Import.ImportID -- so all seven tables below
// are "clean" tables registered directly on b.registry, and persistence.go
// drives every one of them through b.registry.SnapshotAll() / RestoreAll().
//
// The four former ARN/name reverse-lookup maps (trailsByARN, channelsByARN,
// channelsByName, dashboardsByARN, dashboardsByName, edsByARN, edsByName) are
// replaced by companion *store.Index values on the owning table, kept
// consistent automatically by store.Table.Put/Delete/Restore.
//
// Left as a raw field (not store.Table-backed): events ([]Event) -- it is an
// append-only log, not a map[string]*T, so it does not fit store.Table's
// keyed-by-identity-value shape; it continues to persist as a plain slice
// field on backendSnapshot. See persistence.go.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func trailKeyFn(v *Trail) string    { return v.Name }
func trailARNKeyFn(v *Trail) string { return v.TrailARN }

func channelKeyFn(v *Channel) string     { return v.ChannelID }
func channelARNKeyFn(v *Channel) string  { return v.ChannelARN }
func channelNameKeyFn(v *Channel) string { return v.Name }

func dashboardKeyFn(v *Dashboard) string     { return v.DashboardID }
func dashboardARNKeyFn(v *Dashboard) string  { return v.DashboardARN }
func dashboardNameKeyFn(v *Dashboard) string { return v.Name }

func eventDataStoreKeyFn(v *EventDataStore) string     { return v.EventDataStoreID }
func eventDataStoreARNKeyFn(v *EventDataStore) string  { return v.EventDataStoreARN }
func eventDataStoreNameKeyFn(v *EventDataStore) string { return v.Name }

func queryKeyFn(v *Query) string      { return v.QueryID }
func queryAliasKeyFn(v *Query) string { return v.QueryAlias }

func resourcePolicyKeyFn(v *ResourcePolicy) string { return v.ResourceARN }

func importKeyFn(v *Import) string { return v.ImportID }

// registerAllTables registers every backend resource table (and its
// secondary indexes) exactly once. It must be called during construction
// only, immediately after b.registry is created -- store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// instead (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.trails = store.Register(b.registry, "trails", store.New(trailKeyFn))
	b.trailsByARN = b.trails.AddIndex("byARN", trailARNKeyFn)

	b.channels = store.Register(b.registry, "channels", store.New(channelKeyFn))
	b.channelsByARN = b.channels.AddIndex("byARN", channelARNKeyFn)
	b.channelsByName = b.channels.AddIndex("byName", channelNameKeyFn)

	b.dashboards = store.Register(b.registry, "dashboards", store.New(dashboardKeyFn))
	b.dashboardsByARN = b.dashboards.AddIndex("byARN", dashboardARNKeyFn)
	b.dashboardsByName = b.dashboards.AddIndex("byName", dashboardNameKeyFn)

	b.eventDataStores = store.Register(b.registry, "eventDataStores", store.New(eventDataStoreKeyFn))
	b.edsByARN = b.eventDataStores.AddIndex("byARN", eventDataStoreARNKeyFn)
	b.edsByName = b.eventDataStores.AddIndex("byName", eventDataStoreNameKeyFn)

	b.queries = store.Register(b.registry, "queries", store.New(queryKeyFn))
	b.queriesByAlias = b.queries.AddIndex("byAlias", queryAliasKeyFn)

	b.resourcePolicies = store.Register(b.registry, "resourcePolicies", store.New(resourcePolicyKeyFn))

	b.imports = store.Register(b.registry, "imports", store.New(importKeyFn))
}
