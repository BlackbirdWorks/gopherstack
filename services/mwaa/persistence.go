package mwaa

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// Snapshot implements persistence.Persistable by delegating to the backend.
//
// h.Backend is the StorageBackend interface, which already declares
// Snapshot(ctx context.Context) []byte (see interfaces.go) with a shape
// matching persistence.Persistable exactly, so this can call it directly --
// no local type assertion needed. But interface membership alone does not
// help: h.Backend is a named field, not an embedded one, so InMemoryBackend's
// methods are never promoted onto *Handler. Without this delegation, cli.go's
// setupPersistence type-asserts the registered service.Registerable (this
// *Handler) against persistence.Persistable, fails silently, and never
// registers mwaa for snapshot/restore despite the backend being fully
// capable. Mirrors services/securityhub's Handler-level delegation.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}

// Compile-time proof Handler satisfies the persistence layer's contract.
var _ persistence.Persistable = (*Handler)(nil)

// mwaaSnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to regionalDTO/backendSnapshot itself would make an
// older snapshot unsafe to decode as the current shape (e.g. a field's
// meaning or type changes). Restore compares this against the persisted value
// and discards (rather than attempts to partially decode) any mismatch -- see
// Restore below. This is the first version: earlier (pre-Phase-3.3) snapshots
// carried no version field at all, so they also fail this check and are
// discarded rather than misread, which is the safe behaviour across any
// snapshot-format change.
const mwaaSnapshotVersion = 1

// regionalDTO wraps the region-nested Environment resource for JSON
// round-tripping through store.Registry. Table[Environment].Snapshot's plain
// json.Marshal(Environment) cannot see the unexported `region` field it
// carries (used only for the live table's composite key -- see
// store_setup.go), so it is carried alongside Value here instead. ID mirrors
// the environment's own Name so the DTO table itself has a stable composite
// key ("region|Name") independent of the concrete resource type.
type regionalDTO[V any] struct {
	Value  *V     `json:"value"`
	Region string `json:"region"`
	ID     string `json:"id"`
}

// regionalDTOKeyFn is the [store.Table] key function for the regionalDTO
// table below; it mirrors the "region|id" composite key the live table uses
// (see regionKey in backend.go).
func regionalDTOKeyFn[V any](d *regionalDTO[V]) string { return regionKey(d.Region, d.ID) }

// backendSnapshot is the top-level on-disk shape for the MWAA backend.
//
// Tables holds one JSON-encoded array per registered DTO table, produced by
// [store.Registry.SnapshotAll] -- here just "environments", wrapped in a
// regionalDTO to carry its region field through. Metrics is still a
// region-nested plain map (see store_setup.go for why it was not converted to
// store.Table) and is persisted directly, as before. Version guards against
// decoding a snapshot from an incompatible (older or newer) build of this
// backend as though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage          `json:"tables"`
	Metrics   map[string]map[string][]MetricDatum `json:"metrics"`
	AccountID string                              `json:"accountID"`
	Region    string                              `json:"region"`
	Version   int                                 `json:"version"`
}

// buildPersistenceDTORegistry constructs the ephemeral DTO registry used by
// both Snapshot and Restore. It is built fresh on every call (rather than
// reusing b.registry) because the DTO value type (regionalDTO[Environment])
// differs from the live table value type (Environment).
func buildPersistenceDTORegistry() (*store.Registry, *store.Table[regionalDTO[Environment]]) {
	dtoReg := store.NewRegistry()
	envDTOs := store.Register(dtoReg, "environments", store.New(regionalDTOKeyFn[Environment]))

	return dtoReg, envDTOs
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	dtoReg, envDTOs := buildPersistenceDTORegistry()

	for _, v := range b.environments.Snapshot() {
		envDTOs.Put(&regionalDTO[Environment]{Region: v.region, ID: v.Name, Value: v})
	}

	tables, err := dtoReg.SnapshotAll()
	if err != nil {
		// The DTOs above are plain JSON-friendly structs, so a marshal
		// failure here would indicate a programming error rather than bad
		// input data. Log and skip the snapshot rather than panic, matching
		// the persistence.Persistable contract (nil is skipped by the Manager).
		logger.Load(ctx).WarnContext(ctx,
			"mwaa: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:   mwaaSnapshotVersion,
		Tables:    tables,
		Metrics:   b.metrics,
		AccountID: b.accountID,
		Region:    b.region,
	}

	return persistence.MarshalSnapshot(ctx, "mwaa", snap)
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "mwaa", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != mwaaSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"mwaa: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", mwaaSnapshotVersion)

		b.registry.ResetAll()
		b.metrics = make(map[string]map[string][]MetricDatum)
		b.accountID = snap.AccountID
		b.region = snap.Region

		return nil
	}

	if err := b.restoreResourceTables(snap.Tables); err != nil {
		return err
	}

	if snap.Metrics == nil {
		snap.Metrics = make(map[string]map[string][]MetricDatum)
	}
	b.metrics = snap.Metrics

	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// restoreResourceTables rebuilds the environments store.Table from snap's
// per-table JSON, factored out of Restore to keep Restore's own cognitive
// complexity low. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restoreResourceTables(tables map[string]json.RawMessage) error {
	dtoReg, envDTOs := buildPersistenceDTORegistry()

	if err := dtoReg.RestoreAll(tables); err != nil {
		return fmt.Errorf("mwaa: restore snapshot tables: %w", err)
	}

	items := make([]*Environment, 0, envDTOs.Len())

	for _, d := range envDTOs.All() {
		if d.Value == nil {
			// Not producible by Snapshot, but a defensive guard against a
			// hand-edited or corrupted snapshot.
			continue
		}

		d.Value.region = d.Region

		if d.Value.Tags == nil {
			d.Value.Tags = make(map[string]string)
		}

		items = append(items, d.Value)
	}

	b.environments.Restore(items)

	return nil
}
