package rolesanywhere

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// rolesanywhereSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to a DTO type, a registered table's value
// type, or backendSnapshot itself would make an older snapshot unsafe to
// decode as the current shape. Restore compares this against the persisted
// value and discards (registry.ResetAll plus each dirty table's own Reset,
// not a partial decode) any mismatch -- see Restore below. Version 1 was the
// first version (the pre-Phase-3.3 snapshot carried no version field at all,
// so it also failed this check and was discarded rather than misread).
// Version 2 removed TrustAnchor.Tags/Profile.Tags (a prior version wrongly
// persisted creation-time tags on the resource DTO itself, disconnected
// from the InMemoryBackend.tags ARN-keyed store that TagResource/
// UntagResource actually mutate); an old v1 snapshot's creation-time tags
// would silently vanish on restore (never having lived in the tags store),
// so v1 snapshots are discarded here too rather than partially decoded.
const rolesanywhereSnapshotVersion = 2

// regionalDTO wraps a region-nested resource whose only hidden field is
// region (TrustAnchor/Profile/Crl/Subject) for JSON round-tripping through
// the ephemeral persistence registry below -- the same technique
// services/acm's regionalDTO[V] uses. ID mirrors the resource's own
// primary-key component (its ID) so the DTO table itself has a stable
// composite key ("region|id").
type regionalDTO[V any] struct {
	Value  *V     `json:"value"`
	Region string `json:"region"`
	ID     string `json:"id"`
}

func regionalDTOKeyFn[V any](d *regionalDTO[V]) string { return regionKey(d.Region, d.ID) }

// buildPersistenceDTORegistry constructs the ephemeral DTO registry used by
// both Snapshot and Restore for the four "dirty" tables (trustAnchors,
// profiles, crls, subjects; see store_setup.go's registerAllTables doc). It
// is built fresh on every call because the DTO value types differ from the
// live table value types.
func buildPersistenceDTORegistry() (
	*store.Registry,
	*store.Table[regionalDTO[TrustAnchor]],
	*store.Table[regionalDTO[Profile]],
	*store.Table[regionalDTO[Crl]],
	*store.Table[regionalDTO[Subject]],
) {
	dtoReg := store.NewRegistry()
	taDTOs := store.Register(dtoReg, "trustAnchors", store.New(regionalDTOKeyFn[TrustAnchor]))
	profileDTOs := store.Register(dtoReg, "profiles", store.New(regionalDTOKeyFn[Profile]))
	crlDTOs := store.Register(dtoReg, "crls", store.New(regionalDTOKeyFn[Crl]))
	subjectDTOs := store.Register(dtoReg, "subjects", store.New(regionalDTOKeyFn[Subject]))

	return dtoReg, taDTOs, profileDTOs, crlDTOs, subjectDTOs
}

// backendSnapshot is the top-level on-disk shape for the Roles Anywhere
// backend.
//
// Tables holds one JSON-encoded array per DTO table name built above
// (trustAnchors, profiles, crls, subjects) -- b.registry itself registers no
// tables, since all four are dirty (see store_setup.go's registerAllTables
// doc). Tags, AttributeMappings, and NotificationSettings are non-*T value
// maps left unconverted by Phase 3.3 (see store_setup.go) and are persisted
// here directly, as before.
type backendSnapshot struct {
	Tables               map[string]json.RawMessage                  `json:"tables"`
	Tags                 map[string]map[string][]TagEntry            `json:"tags"`
	AttributeMappings    map[string]map[string][]AttributeMapping    `json:"attributeMappings"`
	NotificationSettings map[string]map[string][]NotificationSetting `json:"notificationSettings"`
	Version              int                                         `json:"version"`
}

// Snapshot serializes backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "rolesanywhere: snapshot table marshal failed", "error", err)

		return nil
	}

	dtoReg, taDTOs, profileDTOs, crlDTOs, subjectDTOs := buildPersistenceDTORegistry()

	for _, v := range b.trustAnchors.Snapshot() {
		taDTOs.Put(&regionalDTO[TrustAnchor]{Value: v, Region: v.region, ID: v.TrustAnchorID})
	}

	for _, v := range b.profiles.Snapshot() {
		profileDTOs.Put(&regionalDTO[Profile]{Value: v, Region: v.region, ID: v.ProfileID})
	}

	for _, v := range b.crls.Snapshot() {
		crlDTOs.Put(&regionalDTO[Crl]{Value: v, Region: v.region, ID: v.CrlID})
	}

	for _, v := range b.subjects.Snapshot() {
		subjectDTOs.Put(&regionalDTO[Subject]{Value: v, Region: v.region, ID: v.SubjectID})
	}

	dtoTables, err := dtoReg.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "rolesanywhere: snapshot DTO table marshal failed", "error", err)

		return nil
	}

	maps.Copy(tables, dtoTables)

	snap := backendSnapshot{
		Version:              rolesanywhereSnapshotVersion,
		Tables:               tables,
		Tags:                 b.tags,
		AttributeMappings:    b.attributeMappings,
		NotificationSettings: b.notificationSettings,
	}

	return persistence.MarshalSnapshot(ctx, "rolesanywhere", snap)
}

// Restore deserializes backend state from JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "rolesanywhere", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != rolesanywhereSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"rolesanywhere: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", rolesanywhereSnapshotVersion)

		b.registry.ResetAll()
		b.trustAnchors.Reset()
		b.profiles.Reset()
		b.crls.Reset()
		b.subjects.Reset()
		b.tags = make(map[string]map[string][]TagEntry)
		b.attributeMappings = make(map[string]map[string][]AttributeMapping)
		b.notificationSettings = make(map[string]map[string][]NotificationSetting)

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("rolesanywhere: restore snapshot tables: %w", err)
	}

	if err := b.restoreResourceTables(snap.Tables); err != nil {
		return err
	}

	b.restorePlainMaps(&snap)

	return nil
}

// restoreResourceTables rebuilds the four dirty tables (trustAnchors,
// profiles, crls, subjects) from tables via the ephemeral DTO registry,
// factored out of Restore to keep Restore's own cognitive complexity low.
// Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restoreResourceTables(tables map[string]json.RawMessage) error {
	dtoReg, taDTOs, profileDTOs, crlDTOs, subjectDTOs := buildPersistenceDTORegistry()
	if err := dtoReg.RestoreAll(tables); err != nil {
		return fmt.Errorf("rolesanywhere: restore snapshot resource tables: %w", err)
	}

	b.trustAnchors.Reset()

	for _, d := range taDTOs.Snapshot() {
		if d.Value == nil {
			continue
		}

		d.Value.region = d.Region
		b.trustAnchors.Put(d.Value)
	}

	b.profiles.Reset()

	for _, d := range profileDTOs.Snapshot() {
		if d.Value == nil {
			continue
		}

		d.Value.region = d.Region
		b.profiles.Put(d.Value)
	}

	b.crls.Reset()

	for _, d := range crlDTOs.Snapshot() {
		if d.Value == nil {
			continue
		}

		d.Value.region = d.Region
		b.crls.Put(d.Value)
	}

	b.subjects.Reset()

	for _, d := range subjectDTOs.Snapshot() {
		if d.Value == nil {
			continue
		}

		d.Value.region = d.Region
		b.subjects.Put(d.Value)
	}

	return nil
}

// restorePlainMaps restores every plain (non-store.Table) persisted map from
// snap, defaulting each to an empty map when absent from the snapshot (e.g.
// an older snapshot predating a field, though rolesanywhereSnapshotVersion's
// own guard makes that unreachable today). Factored out of Restore to keep
// Restore's own cognitive complexity low. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restorePlainMaps(snap *backendSnapshot) {
	b.tags = snap.Tags
	if b.tags == nil {
		b.tags = make(map[string]map[string][]TagEntry)
	}

	b.attributeMappings = snap.AttributeMappings
	if b.attributeMappings == nil {
		b.attributeMappings = make(map[string]map[string][]AttributeMapping)
	}

	b.notificationSettings = snap.NotificationSettings
	if b.notificationSettings == nil {
		b.notificationSettings = make(map[string]map[string][]NotificationSetting)
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
// Prior to Phase 3.3, Handler had no Snapshot/Restore of its own even though
// InMemoryBackend fully implemented them: the service registry's persistence
// setup (see setupPersistence in cli.go) only registers a
// service.Registerable that also satisfies an inline Snapshot/Restore
// interface, so RolesAnywhere state was silently never persisted. These two
// methods close that gap.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
