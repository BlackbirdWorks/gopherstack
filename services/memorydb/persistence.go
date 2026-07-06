package memorydb

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// memorydbSnapshotVersion identifies the shape of [backendSnapshot]. It must
// be bumped whenever a change to the set/shape of tables captured below would
// make an older snapshot unsafe to decode as the current shape. Restore
// compares this against the persisted value and discards (rather than
// attempts to partially decode) any mismatch -- see Restore below. This
// mirrors the services/sqs pilot (commit 0f09d77c) and the
// services/elasticache rollout (commit 06806317).
const memorydbSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the MemoryDB backend.
//
// Tables holds the JSON-encoded array for every table registered on
// b.registry -- multiRegionClusters, multiRegionParameterGroups, and
// serviceUpdates, the three partition-scoped (non-region-nested) resources --
// produced by [store.Registry.SnapshotAll].
//
// Every other resource is nested per-region (region -> *store.Table[T]) and
// is NOT registered on b.registry, because the set of regions is only known
// at runtime (see store_setup.go's doc comment); each is instead captured
// directly below as region -> that table's own deterministic
// [store.Table.Snapshot] slice.
type backendSnapshot struct {
	ParameterGroups map[string][]*ParameterGroup      `json:"parameterGroups"`
	Tables          map[string]json.RawMessage        `json:"tables"`
	Clusters        map[string][]*Cluster             `json:"clusters"`
	ACLs            map[string][]*ACL                 `json:"acls"`
	SubnetGroups    map[string][]*SubnetGroup         `json:"subnetGroups"`
	Users           map[string][]*User                `json:"users"`
	Snapshots       map[string][]*Snapshot            `json:"snapshots"`
	ReservedNodes   map[string][]*ReservedNode        `json:"reservedNodes"`
	ARNToResource   map[string]map[string]resourceRef `json:"arnToResource"`
	Events          map[string][]*Event               `json:"events"`
	AccountID       string                            `json:"accountID"`
	DefaultRegion   string                            `json:"defaultRegion"`
	Version         int                               `json:"version"`
}

// snapshotAllRegions captures the deterministic [store.Table.Snapshot] slice
// for every region in m.
func snapshotAllRegions[T any](m map[string]*store.Table[T]) map[string][]*T {
	out := make(map[string][]*T, len(m))
	for region, t := range m {
		out[region] = t.Snapshot()
	}

	return out
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		// The registered tables are plain JSON-friendly structs, so a marshal
		// failure here would indicate a programming error rather than bad
		// input data. Log and skip the snapshot rather than panic, matching
		// the persistence.Persistable contract (nil is skipped by the Manager).
		logger.Load(ctx).WarnContext(ctx, "memorydb: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:         memorydbSnapshotVersion,
		Tables:          tables,
		Clusters:        snapshotAllRegions(b.clusters),
		ACLs:            snapshotAllRegions(b.acls),
		SubnetGroups:    snapshotAllRegions(b.subnetGroups),
		Users:           snapshotAllRegions(b.users),
		ParameterGroups: snapshotAllRegions(b.parameterGroups),
		Snapshots:       snapshotAllRegions(b.snapshots),
		ReservedNodes:   snapshotAllRegions(b.reservedNodes),
		ARNToResource:   b.arnToResource,
		Events:          b.events,
		AccountID:       b.accountID,
		DefaultRegion:   b.defaultRegion,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "memorydb: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// restoreRegionTables resets the outer per-region map via reset, then
// restores each region's table from data using storeFn (one of the
// InMemoryBackend "*Store" lazy accessors). This mirrors the pre-conversion
// full-replace Restore semantics: a region present in the live backend but
// absent from data ends up empty, exactly as a direct `b.field = data`
// assignment would have left it.
func restoreRegionTables[T any](reset func(), storeFn func(string) *store.Table[T], data map[string][]*T) {
	reset()

	for region, items := range data {
		storeFn(region).Restore(items)
	}
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "memorydb", data, &snap); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if snap.Version != memorydbSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"memorydb: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", memorydbSnapshotVersion)

		b.resetLocked()

		return nil
	}

	fixNilTagsInSnapshot(&snap)

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return err
	}

	restoreRegionTables(func() { b.clusters = make(map[string]*store.Table[Cluster]) },
		b.clustersStore, snap.Clusters)
	restoreRegionTables(func() { b.acls = make(map[string]*store.Table[ACL]) },
		b.aclsStore, snap.ACLs)
	restoreRegionTables(func() { b.subnetGroups = make(map[string]*store.Table[SubnetGroup]) },
		b.subnetGroupsStore, snap.SubnetGroups)
	restoreRegionTables(func() { b.users = make(map[string]*store.Table[User]) },
		b.usersStore, snap.Users)
	restoreRegionTables(func() { b.parameterGroups = make(map[string]*store.Table[ParameterGroup]) },
		b.parameterGroupsStore, snap.ParameterGroups)
	restoreRegionTables(func() { b.snapshots = make(map[string]*store.Table[Snapshot]) },
		b.snapshotsStore, snap.Snapshots)
	restoreRegionTables(func() { b.reservedNodes = make(map[string]*store.Table[ReservedNode]) },
		b.reservedNodesStore, snap.ReservedNodes)

	b.fixGlobalTagsLocked()

	if snap.ARNToResource != nil {
		b.arnToResource = snap.ARNToResource
	} else {
		b.arnToResource = make(map[string]map[string]resourceRef)
	}

	if snap.Events != nil {
		b.events = snap.Events
	} else {
		b.events = make(map[string][]*Event)
	}

	b.accountID = snap.AccountID
	b.defaultRegion = snap.DefaultRegion

	return nil
}

// fixListTags calls fix on every item in every region's slice of nested,
// mutating each item in place. Used to backfill nil tag/parameter maps on
// snapshots taken before this backend guaranteed non-nil maps.
func fixListTags[T any](nested map[string][]*T, fix func(*T)) {
	for _, items := range nested {
		for _, item := range items {
			fix(item)
		}
	}
}

func ensureMemoryDBMap(m map[string]string) map[string]string {
	if m == nil {
		return make(map[string]string)
	}

	return m
}

func fixNilTagsInSnapshot(snap *backendSnapshot) {
	fixCoreResourceTags(snap)
	fixExtendedResourceTags(snap)
}

func fixCoreResourceTags(snap *backendSnapshot) {
	fixListTags(snap.Clusters, func(c *Cluster) { c.Tags = ensureMemoryDBMap(c.Tags) })
	fixListTags(snap.ACLs, func(a *ACL) { a.Tags = ensureMemoryDBMap(a.Tags) })
	fixListTags(snap.SubnetGroups, func(sg *SubnetGroup) { sg.Tags = ensureMemoryDBMap(sg.Tags) })
	fixListTags(snap.Users, func(u *User) { u.Tags = ensureMemoryDBMap(u.Tags) })
}

func fixExtendedResourceTags(snap *backendSnapshot) {
	fixListTags(snap.ParameterGroups, func(pg *ParameterGroup) {
		pg.Tags = ensureMemoryDBMap(pg.Tags)
		pg.Parameters = ensureMemoryDBMap(pg.Parameters)
	})
	fixListTags(snap.Snapshots, func(s *Snapshot) { s.Tags = ensureMemoryDBMap(s.Tags) })
}

// fixGlobalTagsLocked backfills nil tag/parameter maps on the registry-backed
// global tables (multiRegionClusters, multiRegionParameterGroups) after
// RestoreAll has populated them from a snapshot taken before this backend
// guaranteed non-nil maps. serviceUpdates carries no tags/parameters, so it
// needs no fix-up. Must hold b.mu.
func (b *InMemoryBackend) fixGlobalTagsLocked() {
	for _, mrc := range b.multiRegionClusters.All() {
		mrc.Tags = ensureMemoryDBMap(mrc.Tags)
	}

	for _, mrpg := range b.multiRegionParameterGroups.All() {
		mrpg.Tags = ensureMemoryDBMap(mrpg.Tags)
		mrpg.Parameters = ensureMemoryDBMap(mrpg.Parameters)
	}
}

func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
