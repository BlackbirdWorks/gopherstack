package s3tables

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// s3tablesSnapshotVersion identifies the shape of [backendSnapshot]. It must
// be bumped whenever a change to backendSnapshot (or a value type held by
// one of the registered tables) would make an older snapshot unsafe to
// decode as the current shape. Restore compares this against the persisted
// value and discards (Reset, not a partial decode) any mismatch -- see
// Restore. The pre-Phase-3.3 snapshot format had no version field at all,
// so an old snapshot decodes with Version == 0, which is guaranteed to
// mismatch s3tablesSnapshotVersion and is discarded the same way any other
// incompatible snapshot is.
const s3tablesSnapshotVersion = 1

// arnDTO wraps a value type that carries no identity of its own (see
// BucketReplicationConfig and TableRecordExpiryConfig's doc comments in
// backend.go) for JSON round-tripping through the ephemeral persistence
// registry below. ARN mirrors the resource's own json:"-" identity field so
// the DTO table itself has a stable key independent of Value's (unmarshaled)
// fields.
type arnDTO[V any] struct {
	Value *V     `json:"value"`
	ARN   string `json:"arn"`
}

func arnDTOKeyFn[V any](d *arnDTO[V]) string { return d.ARN }

// persistenceDTOTables groups the two ephemeral DTO tables
// buildPersistenceDTORegistry constructs, so Snapshot/Restore can pass them
// around as one value.
type persistenceDTOTables struct {
	registry          *store.Registry
	bucketReplication *store.Table[arnDTO[BucketReplicationConfig]]
	tableRecordExpiry *store.Table[arnDTO[TableRecordExpiryConfig]]
}

// buildPersistenceDTORegistry constructs the ephemeral DTO registry used by
// both Snapshot and Restore for the two tables that are NOT on b.registry
// (see store_setup.go). It is built fresh on every call, mirroring
// services/workmail's and services/emr's buildPersistenceDTORegistry.
func buildPersistenceDTORegistry() persistenceDTOTables {
	dtoReg := store.NewRegistry()

	return persistenceDTOTables{
		registry: dtoReg,
		bucketReplication: store.Register(
			dtoReg, "bucketReplication", store.New(arnDTOKeyFn[BucketReplicationConfig]),
		),
		tableRecordExpiry: store.Register(
			dtoReg, "tableRecordExpiry", store.New(arnDTOKeyFn[TableRecordExpiryConfig]),
		),
	}
}

// backendSnapshot is the top-level on-disk shape for the S3 Tables backend.
//
// Tables holds one JSON-encoded array per table name: "tableBuckets",
// "namespaces", and "tables" produced directly by b.registry.SnapshotAll()
// (none hides any field from JSON), plus "bucketReplication" and
// "tableRecordExpiry" built by buildPersistenceDTORegistry above (each
// hides its identity field behind json:"-").
//
// TableReplication, Tags, and TableReplicationConfigs are the plain
// (non-store.Table) maps left unconverted by Phase 3.3 -- see the field
// comments on InMemoryBackend in backend.go: none holds a *T value with an
// identity of its own. All three are persisted directly here, matching
// their pre-refactor persistence. Version guards against decoding a
// snapshot from an incompatible (older or newer) build of this backend as
// though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables                  map[string]json.RawMessage   `json:"tables"`
	TableReplication        map[string]bool              `json:"tableReplication"`
	Tags                    map[string]map[string]string `json:"tags"`
	TableReplicationConfigs map[string]map[string]any    `json:"tableReplicationConfigs"`
	AccountID               string                       `json:"accountID"`
	Region                  string                       `json:"region"`
	Version                 int                          `json:"version"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.muBuckets.RLock("Snapshot")
	b.muNamespaces.RLock("Snapshot")
	b.muTables.RLock("Snapshot")
	b.muState.RLock("Snapshot")
	defer b.muBuckets.RUnlock()
	defer b.muNamespaces.RUnlock()
	defer b.muTables.RUnlock()
	defer b.muState.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "s3tables: snapshot table marshal failed", "error", err)

		return nil
	}

	dtos := buildPersistenceDTORegistry()

	for _, v := range b.bucketReplication.Snapshot() {
		dtos.bucketReplication.Put(&arnDTO[BucketReplicationConfig]{Value: v, ARN: v.TableBucketARN})
	}

	for _, v := range b.tableRecordExpiry.Snapshot() {
		dtos.tableRecordExpiry.Put(&arnDTO[TableRecordExpiryConfig]{Value: v, ARN: v.TableARN})
	}

	dtoTables, err := dtos.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "s3tables: snapshot DTO table marshal failed", "error", err)

		return nil
	}

	maps.Copy(tables, dtoTables)

	snap := backendSnapshot{
		Version:                 s3tablesSnapshotVersion,
		Tables:                  tables,
		TableReplication:        b.tableReplication,
		Tags:                    b.tags,
		TableReplicationConfigs: b.tableReplicationConfigs,
		AccountID:               b.accountID,
		Region:                  b.region,
	}

	return persistence.MarshalSnapshot(ctx, "s3tables", snap)
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "s3tables", data, &snap); err != nil {
		return err
	}

	b.muBuckets.Lock("Restore")
	b.muNamespaces.Lock("Restore")
	b.muTables.Lock("Restore")
	b.muState.Lock("Restore")
	defer b.muBuckets.Unlock()
	defer b.muNamespaces.Unlock()
	defer b.muTables.Unlock()
	defer b.muState.Unlock()

	if snap.Version != s3tablesSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"s3tables: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", s3tablesSnapshotVersion)

		b.registry.ResetAll()
		b.bucketReplication.Reset()
		b.tableRecordExpiry.Reset()
		b.tableReplication = make(map[string]bool)
		b.tags = make(map[string]map[string]string)
		b.tableReplicationConfigs = make(map[string]map[string]any)

		return nil
	}

	if err := b.restoreResourceTables(snap.Tables); err != nil {
		return err
	}

	b.restoreRawMaps(&snap)

	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// restoreResourceTables rebuilds every store.Table on b from snap's
// per-table JSON, factored out of Restore to keep Restore's own cognitive
// complexity low. Callers must hold every mutex.
func (b *InMemoryBackend) restoreResourceTables(tables map[string]json.RawMessage) error {
	if err := b.registry.RestoreAll(tables); err != nil {
		return fmt.Errorf("s3tables: restore clean tables: %w", err)
	}

	dtos := buildPersistenceDTORegistry()
	if err := dtos.registry.RestoreAll(tables); err != nil {
		return fmt.Errorf("s3tables: restore DTO tables: %w", err)
	}

	b.bucketReplication.Restore(unwrapARNDTOs(dtos.bucketReplication, func(v *BucketReplicationConfig, arn string) {
		v.TableBucketARN = arn
	}))
	b.tableRecordExpiry.Restore(unwrapARNDTOs(dtos.tableRecordExpiry, func(v *TableRecordExpiryConfig, arn string) {
		v.TableARN = arn
	}))

	return nil
}

// unwrapARNDTOs converts every arnDTO[V] in dtos into its live *V, restoring
// the identity field each carries via setARN (a plain generic type
// parameter V has no field access, so the assignment is supplied by the
// caller, one per concrete V; see restoreResourceTables). A DTO whose Value
// is nil -- not producible by Snapshot, but a defensive guard against a
// hand-edited or corrupted snapshot -- is skipped rather than dereferenced.
func unwrapARNDTOs[V any](dtos *store.Table[arnDTO[V]], setARN func(*V, string)) []*V {
	items := make([]*V, 0, dtos.Len())

	for _, d := range dtos.All() {
		if d.Value == nil {
			continue
		}

		setARN(d.Value, d.ARN)
		items = append(items, d.Value)
	}

	return items
}

// restoreRawMaps restores every plain (non-store.Table) persisted map from
// snap, defaulting each to an empty map when absent from the snapshot.
// Callers must hold b.muState.Lock (and, for consistency with Restore's
// broader critical section, every other mutex too).
func (b *InMemoryBackend) restoreRawMaps(snap *backendSnapshot) {
	b.tableReplication = snap.TableReplication
	if b.tableReplication == nil {
		b.tableReplication = make(map[string]bool)
	}

	b.tags = snap.Tags
	if b.tags == nil {
		b.tags = make(map[string]map[string]string)
	}

	b.tableReplicationConfigs = snap.TableReplicationConfigs
	if b.tableReplicationConfigs == nil {
		b.tableReplicationConfigs = make(map[string]map[string]any)
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
// Handler previously had no Snapshot/Restore of its own even though
// InMemoryBackend did -- dead wiring: cli.go's generic setupPersistence
// (which type-asserts the registered service.Registerable, i.e. the
// Handler, for a Snapshot/Restore pair) never picked S3 Tables up at all,
// so nothing was ever actually persisted despite the backend supporting it.
// This delegation (matching the cleanrooms/codecommit pattern) is what
// wires S3 Tables into persistence for the first time.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
