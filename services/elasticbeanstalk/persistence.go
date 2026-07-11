package elasticbeanstalk

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// elasticbeanstalkSnapshotVersion identifies the shape of [backendSnapshot].
// It must be bumped whenever a change to the DTOs/backendSnapshot itself
// would make an older snapshot unsafe to decode as the current shape (e.g. a
// field's meaning or type changes). Restore compares this against the
// persisted value and discards (rather than attempts to partially decode)
// any mismatch -- see Restore below. This is the first version: earlier
// (pre-Phase-3.3) snapshots carried no version field at all, so they also
// fail this check and are discarded rather than misread, which is the safe
// behaviour across any snapshot-format change.
const elasticbeanstalkSnapshotVersion = 1

// regionalDTO wraps a region-nested resource for JSON round-tripping through
// store.Registry. It is shared by every converted resource that hides its
// region behind an unexported field (Application, ApplicationVersion,
// ConfigurationTemplate, PlatformVersion) -- Environment needs no such
// wrapper since it already carries an exported Region field that a plain
// json.Marshal(Environment) sees on its own (see persistenceDTOTables
// below). ID mirrors the resource's own composite-key component (its own
// identifier, or the appVersionKey/configTemplateKey composite) so the DTO
// table itself has a stable composite key ("Region|ID"), matching how the
// live table is keyed (see regionKey in backend.go).
type regionalDTO[V any] struct {
	Value  *V     `json:"value"`
	Region string `json:"region"`
	ID     string `json:"id"`
}

// regionalDTOKeyFn is the shared [store.Table] key function for every
// regionalDTO[V] table below; it mirrors the "region|id" composite key each
// live table uses.
func regionalDTOKeyFn[V any](d *regionalDTO[V]) string { return regionKey(d.Region, d.ID) }

// backendSnapshot is the top-level on-disk shape for the Elastic Beanstalk
// backend.
//
// Tables holds one JSON-encoded array per registered DTO table, produced by
// [store.Registry.SnapshotAll] -- the five converted resource collections.
// managedActionHistory, events, and envCounters remain plain region-nested
// maps (see store_setup.go for why they were not converted to store.Table)
// and are persisted directly, as before. Version guards against decoding a
// snapshot from an incompatible (older or newer) build of this backend as
// though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables               map[string]json.RawMessage                    `json:"tables"`
	ManagedActionHistory map[string]map[string][]*ManagedActionHistory `json:"managedActionHistory,omitempty"`
	Events               map[string][]*EventRecord                     `json:"events,omitempty"`
	EnvCounters          map[string]int                                `json:"envCounters,omitempty"`
	AccountID            string                                        `json:"accountID"`
	Region               string                                        `json:"region"`
	Version              int                                           `json:"version"`
}

// persistenceDTOTables groups every ephemeral DTO table buildPersistenceDTORegistry
// constructs, so Snapshot/Restore can pass them around as one value instead of
// five separate return values.
type persistenceDTOTables struct {
	registry         *store.Registry
	applications     *store.Table[regionalDTO[Application]]
	environments     *store.Table[Environment]
	appVersions      *store.Table[regionalDTO[ApplicationVersion]]
	configTemplates  *store.Table[regionalDTO[ConfigurationTemplate]]
	platformVersions *store.Table[regionalDTO[PlatformVersion]]
}

// buildPersistenceDTORegistry constructs the ephemeral DTO registry used by
// both Snapshot and Restore. It is built fresh on every call (rather than
// reusing b.registry) because four of the five DTO value types differ from
// the live table value types (regionalDTO[V] vs V, to carry the hidden
// region field through JSON). environments is the one exception: Environment
// already carries an exported Region field, so its DTO table reuses the same
// live keyFn and value type directly -- no wrapper needed.
func buildPersistenceDTORegistry() persistenceDTOTables {
	dtoReg := store.NewRegistry()

	return persistenceDTOTables{
		registry:         dtoReg,
		applications:     store.Register(dtoReg, "applications", store.New(regionalDTOKeyFn[Application])),
		environments:     store.Register(dtoReg, "environments", store.New(environmentKeyFn)),
		appVersions:      store.Register(dtoReg, "appVersions", store.New(regionalDTOKeyFn[ApplicationVersion])),
		configTemplates:  store.Register(dtoReg, "configTemplates", store.New(regionalDTOKeyFn[ConfigurationTemplate])),
		platformVersions: store.Register(dtoReg, "platformVersions", store.New(regionalDTOKeyFn[PlatformVersion])),
	}
}

// Snapshot serialises the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	dtos := buildPersistenceDTORegistry()

	for _, v := range b.applications.Snapshot() {
		dtos.applications.Put(&regionalDTO[Application]{Region: v.region, ID: v.ApplicationName, Value: v})
	}

	for _, v := range b.environments.Snapshot() {
		dtos.environments.Put(v)
	}

	for _, v := range b.appVersions.Snapshot() {
		dtos.appVersions.Put(&regionalDTO[ApplicationVersion]{
			Region: v.region, ID: appVersionKey(v.ApplicationName, v.VersionLabel), Value: v,
		})
	}

	for _, v := range b.configTemplates.Snapshot() {
		dtos.configTemplates.Put(&regionalDTO[ConfigurationTemplate]{
			Region: v.region, ID: configTemplateKey(v.ApplicationName, v.TemplateName), Value: v,
		})
	}

	for _, v := range b.platformVersions.Snapshot() {
		dtos.platformVersions.Put(&regionalDTO[PlatformVersion]{Region: v.region, ID: v.PlatformArn, Value: v})
	}

	tables, err := dtos.registry.SnapshotAll()
	if err != nil {
		// The DTOs above are plain JSON-friendly structs, so a marshal
		// failure here would indicate a programming error rather than bad
		// input data. Log and skip the snapshot rather than panic, matching
		// the persistence.Persistable contract (nil is skipped by the
		// Manager).
		logger.Load(ctx).WarnContext(ctx, "elasticbeanstalk: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:              elasticbeanstalkSnapshotVersion,
		Tables:               tables,
		ManagedActionHistory: b.managedActionHistory,
		Events:               b.events,
		EnvCounters:          b.envCounters,
		AccountID:            b.accountID,
		Region:               b.region,
	}

	return persistence.MarshalSnapshot(ctx, "elasticbeanstalk", snap)
}

// Restore loads backend state from a JSON snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "elasticbeanstalk", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != elasticbeanstalkSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never
		// be partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"elasticbeanstalk: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", elasticbeanstalkSnapshotVersion)

		b.registry.ResetAll()
		b.managedActionHistory = make(map[string]map[string][]*ManagedActionHistory)
		b.events = make(map[string][]*EventRecord)
		b.envCounters = make(map[string]int)
		b.accountID = snap.AccountID
		b.region = snap.Region
		b.initRegion(b.region)

		return nil
	}

	if err := b.restoreResourceTables(snap.Tables); err != nil {
		return err
	}

	if snap.ManagedActionHistory == nil {
		snap.ManagedActionHistory = make(map[string]map[string][]*ManagedActionHistory)
	}

	if snap.Events == nil {
		snap.Events = make(map[string][]*EventRecord)
	}

	if snap.EnvCounters == nil {
		snap.EnvCounters = make(map[string]int)
	}

	b.managedActionHistory = snap.ManagedActionHistory
	b.events = snap.Events
	b.envCounters = snap.EnvCounters
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// unwrapRegionalDTOs converts every regionalDTO[V] in dtos into its live *V,
// restoring the unexported region field each carries via setRegion (a plain
// generic type parameter V has no field access, so the region assignment is
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

// restoreResourceTables rebuilds every store.Table on b from snap's
// per-table JSON, factored out of Restore to keep Restore's own cognitive
// complexity low. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restoreResourceTables(tables map[string]json.RawMessage) error {
	dtos := buildPersistenceDTORegistry()

	if err := dtos.registry.RestoreAll(tables); err != nil {
		return fmt.Errorf("elasticbeanstalk: restore snapshot tables: %w", err)
	}

	b.applications.Restore(unwrapRegionalDTOs(dtos.applications, func(v *Application, r string) { v.region = r }))
	b.environments.Restore(dtos.environments.Snapshot())
	b.appVersions.Restore(unwrapRegionalDTOs(
		dtos.appVersions, func(v *ApplicationVersion, r string) { v.region = r },
	))
	b.configTemplates.Restore(unwrapRegionalDTOs(
		dtos.configTemplates, func(v *ConfigurationTemplate, r string) { v.region = r },
	))
	b.platformVersions.Restore(unwrapRegionalDTOs(
		dtos.platformVersions, func(v *PlatformVersion, r string) { v.region = r },
	))

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
