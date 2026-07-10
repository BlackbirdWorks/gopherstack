package identitystore

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// identitystoreSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to the DTOs/backendSnapshot itself would
// make an older snapshot unsafe to decode as the current shape (e.g. a
// field's meaning or type changes). Restore compares this against the
// persisted value and discards (rather than attempts to partially decode)
// any mismatch -- see Restore below. This is the first version: the
// pre-Phase-3.3 backendSnapshot carried no version field at all, so it also
// fails this check and is discarded rather than misread, which is the safe
// behaviour across any snapshot-format change.
const identitystoreSnapshotVersion = 1

// regionalDTO wraps a region-nested resource for JSON round-tripping through
// store.Registry. store.Table[V].Snapshot's plain json.Marshal(V) cannot see
// V's unexported `region` field (used only for the live table's composite
// key, see store_setup.go), so it is carried alongside Value here instead --
// the same technique services/emr's regionalDTO[V] uses. ID mirrors the
// resource's own identifier so the DTO table itself has a stable composite
// key ("Region#ID") independent of Value's own (unmarshaled) fields.
type regionalDTO[V any] struct {
	Value  *V     `json:"value"`
	Region string `json:"region"`
	ID     string `json:"id"`
}

// regionalDTOKeyFn is the shared [store.Table] key function for every
// regionalDTO[V] table below; it mirrors the "region#id" composite key each
// live table uses (see regionKey in backend.go).
func regionalDTOKeyFn[V any](d *regionalDTO[V]) string { return regionKey(d.Region, d.ID) }

// backendSnapshot is the top-level on-disk shape for the Identity Store
// backend.
//
// Tables holds one JSON-encoded array per registered DTO table, produced by
// [store.Registry.SnapshotAll] -- the three converted resource collections
// (users, groups, memberships), each wrapped in a regionalDTO to carry its
// hidden `region` field through. Version guards against decoding a snapshot
// from an incompatible (older or newer) build of this backend as though it
// were the current shape; see Restore.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage `json:"tables"`
	AccountID string                     `json:"accountID"`
	Region    string                     `json:"region"`
	Counter   int                        `json:"counter"`
	Version   int                        `json:"version"`
}

// persistenceDTOTables groups every ephemeral DTO table
// buildPersistenceDTORegistry constructs, so Snapshot/Restore can pass them
// around as one value instead of three separate return values.
type persistenceDTOTables struct {
	registry    *store.Registry
	users       *store.Table[regionalDTO[User]]
	groups      *store.Table[regionalDTO[Group]]
	memberships *store.Table[regionalDTO[GroupMembership]]
}

// buildPersistenceDTORegistry constructs the ephemeral DTO registry used by
// both Snapshot and Restore. It is built fresh on every call (rather than
// reusing b.registry) because the DTO value types differ from the live table
// value types (regionalDTO[V] vs V).
func buildPersistenceDTORegistry() persistenceDTOTables {
	dtoReg := store.NewRegistry()

	return persistenceDTOTables{
		registry:    dtoReg,
		users:       store.Register(dtoReg, "users", store.New(regionalDTOKeyFn[User])),
		groups:      store.Register(dtoReg, "groups", store.New(regionalDTOKeyFn[Group])),
		memberships: store.Register(dtoReg, "memberships", store.New(regionalDTOKeyFn[GroupMembership])),
	}
}

// Snapshot serialises the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	dtos := buildPersistenceDTORegistry()

	for _, v := range b.users.Snapshot() {
		dtos.users.Put(&regionalDTO[User]{Value: v, Region: v.region, ID: v.UserID})
	}

	for _, v := range b.groups.Snapshot() {
		dtos.groups.Put(&regionalDTO[Group]{Value: v, Region: v.region, ID: v.GroupID})
	}

	for _, v := range b.memberships.Snapshot() {
		dtos.memberships.Put(&regionalDTO[GroupMembership]{Value: v, Region: v.region, ID: v.MembershipID})
	}

	tables, err := dtos.registry.SnapshotAll()
	if err != nil {
		// The DTOs above are plain JSON-friendly structs, so a marshal
		// failure here would indicate a programming error rather than bad
		// input data. Log and skip the snapshot rather than panic, matching
		// the persistence.Persistable contract (nil is skipped by the
		// Manager).
		logger.Load(ctx).WarnContext(ctx, "identitystore: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:   identitystoreSnapshotVersion,
		Tables:    tables,
		AccountID: b.accountID,
		Region:    b.region,
		Counter:   b.counter,
	}

	return persistence.MarshalSnapshot(ctx, "identitystore", snap)
}

// Restore loads backend state from a JSON snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "identitystore", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != identitystoreSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never
		// be partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"identitystore: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", identitystoreSnapshotVersion)

		b.registry.ResetAll()
		b.accountID = snap.AccountID
		b.region = snap.Region
		b.counter = 0

		return nil
	}

	if err := b.restoreResourceTables(snap.Tables); err != nil {
		return err
	}

	b.accountID = snap.AccountID
	b.region = snap.Region
	b.counter = snap.Counter

	return nil
}

// restoreResourceTables rebuilds every store.Table on b from snap's
// per-table JSON, factored out of Restore to keep Restore's own cognitive
// complexity low. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restoreResourceTables(tables map[string]json.RawMessage) error {
	dtos := buildPersistenceDTORegistry()

	if err := dtos.registry.RestoreAll(tables); err != nil {
		return fmt.Errorf("identitystore: restore snapshot tables: %w", err)
	}

	b.users.Restore(unwrapRegionalDTOs(dtos.users, func(v *User, r string) { v.region = r }))
	b.groups.Restore(unwrapRegionalDTOs(dtos.groups, func(v *Group, r string) { v.region = r }))
	b.memberships.Restore(unwrapRegionalDTOs(dtos.memberships, func(v *GroupMembership, r string) { v.region = r }))

	return nil
}

// unwrapRegionalDTOs converts every regionalDTO[V] in dtos into its live *V,
// restoring the unexported region field each carries via setRegion (a plain
// generic type parameter V has no field access, so the assignment is
// supplied by the caller, one per concrete V; see restoreResourceTables). A
// DTO whose Value is nil -- not producible by Snapshot, but a defensive
// guard against a hand-edited or corrupted snapshot -- is skipped rather
// than dereferenced.
func unwrapRegionalDTOs[V any](dtos *store.Table[regionalDTO[V]], setRegion func(*V, string)) []*V {
	items := make([]*V, 0, dtos.Len())

	for _, d := range dtos.All() {
		if d.Value == nil {
			continue
		}

		setRegion(d.Value, d.Region)
		items = append(items, d.Value)
	}

	return items
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
