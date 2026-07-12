package timestreamwrite

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// Snapshot implements persistence.Persistable by delegating to the backend.
//
// h.Backend is *InMemoryBackend (a concrete type, not the StorageBackend
// interface), so no type assertion is needed here -- but its methods are
// still not promoted to Handler, since Backend is a named field rather than
// an embedded one. Without this delegation, cli.go's setupPersistence
// type-asserts the registered service.Registerable (this *Handler) against
// persistence.Persistable, fails silently, and never registers
// timestreamwrite for snapshot/restore despite the backend being fully
// capable. Mirrors services/securityhub's Handler-level delegation.
//
// InMemoryBackend.Snapshot has a different shape than persistence.Persistable
// (no ctx parameter, and it returns an error instead of logging one itself),
// so this adapts: it calls the backend's Snapshot() and logs+swallows any
// marshal error, matching the Persistable contract (a nil snapshot is skipped
// by the persistence Manager).
func (h *Handler) Snapshot(ctx context.Context) []byte {
	data, err := h.Backend.Snapshot()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "timestreamwrite: Handler snapshot failed", "error", err)

		return nil
	}

	return data
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}

// Compile-time proof Handler satisfies the persistence layer's contract.
var _ persistence.Persistable = (*Handler)(nil)

// timestreamwriteSnapshotVersion identifies the shape of [backendSnapshot].
// It must be bumped whenever a change to backendSnapshot (or a value type
// held by one of the registered tables) would make an older snapshot unsafe
// to decode as the current shape. Restore compares this against the
// persisted value and discards (registry.ResetAll, not a partial decode) any
// mismatch -- see Restore. The pre-Phase-3.3 snapshot format had no version
// field at all, so an old snapshot decodes with Version == 0, which is
// guaranteed to mismatch timestreamwriteSnapshotVersion and is discarded the
// same way any other incompatible snapshot is.
const timestreamwriteSnapshotVersion = 1

// backendSnapshot is the serialisable representation of the backend state.
//
// Tables holds one JSON-encoded array per registered store.Table name
// (databases, tables, batchLoadTasks -- see store_setup.go), produced by
// b.registry.SnapshotAll(). Records and Tags are still plain nested maps
// (see store_setup.go for why they were not converted to store.Table) and
// are persisted directly, as before. Version guards against decoding a
// snapshot from an incompatible (older or newer) build of this backend as
// though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables     map[string]json.RawMessage     `json:"tables"`
	Records    map[string]map[string][]Record `json:"records"`
	Tags       map[string]map[string]string   `json:"tags"`
	NextTaskID int                            `json:"next_task_id"`
	Version    int                            `json:"version"`
}

// Snapshot serialises the current backend state into a JSON byte slice.
//
// Holds the global write lock to serialise against WriteRecords, which only
// holds the global read lock and writes to the inner b.records map; concurrent
// map iteration here and map write there would be a fatal Go data race.
func (b *InMemoryBackend) Snapshot() ([]byte, error) {
	b.mu.Lock("Snapshot")
	defer b.mu.Unlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		return nil, fmt.Errorf("timestreamwrite: snapshot table marshal failed: %w", err)
	}

	records := make(map[string]map[string][]Record, len(b.records))

	for dbName, tblSlots := range b.records {
		records[dbName] = make(map[string][]Record, len(tblSlots))

		for tblName, slot := range tblSlots {
			out := make([]Record, len(slot.records))
			for i, r := range slot.records {
				dims := make([]Dimension, len(r.Dimensions))
				copy(dims, r.Dimensions)
				r.Dimensions = dims
				out[i] = r
			}

			records[dbName][tblName] = out
		}
	}

	tags := make(map[string]map[string]string, len(b.tags))
	for arn, tagMap := range b.tags {
		tags[arn] = make(map[string]string, len(tagMap))
		maps.Copy(tags[arn], tagMap)
	}

	snap := backendSnapshot{
		Version:    timestreamwriteSnapshotVersion,
		Tables:     tables,
		Records:    records,
		Tags:       tags,
		NextTaskID: b.nextTaskID,
	}

	// Marshal failure is returned to the caller (the Persistable wrapper logs it
	// via the context-aware logger); do not log through slog.Default() here.
	data, err := json.Marshal(snap)
	if err != nil {
		return nil, fmt.Errorf("timestreamwrite: marshal snapshot: %w", err)
	}

	return data, nil
}

// Restore replaces the backend state with the data from a previous Snapshot call.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "timestreamwrite", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	// Close all existing per-table mutexes before discarding the records map;
	// otherwise lockmetrics' global registry leaks one entry per pre-restore table.
	b.closeAllTableMutexesLocked()

	if snap.Version != timestreamwriteSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"timestreamwrite: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", timestreamwriteSnapshotVersion)

		b.registry.ResetAll()
		b.nextTaskID = 0
		b.ensureNonNilMapsLocked()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("timestreamwrite: restore snapshot tables: %w", err)
	}

	b.nextTaskID = snap.NextTaskID

	b.tags = snap.Tags
	if b.tags == nil {
		b.tags = make(map[string]map[string]string)
	}

	// Convert snapshot's flat record map back into per-table slots so that
	// each table owns its own mutex and slice independent of the enclosing maps.
	// Rebuild the dedup index from the restored records so that version-based
	// upsert works correctly after restore.
	b.records = make(map[string]map[string]*tableRecords, len(snap.Records))
	for dbName, tblRecs := range snap.Records {
		b.records[dbName] = make(map[string]*tableRecords, len(tblRecs))
		for tblName, recs := range tblRecs {
			idx := make(map[string]int, len(recs))
			for i, r := range recs {
				idx[recordKey(r)] = i
			}

			b.records[dbName][tblName] = &tableRecords{
				mu:          lockmetrics.New("timestreamwrite.table"),
				records:     recs,
				recordIndex: idx,
			}
		}
	}

	b.ensureNonNilMapsFromSnapshot()

	return nil
}

// ensureNonNilMapsFromSnapshot initialises any nil maps that may result from
// restoring a snapshot with missing fields, and ensures each table has a slot
// with an initialised mutex even if its records slice is missing.
func (b *InMemoryBackend) ensureNonNilMapsFromSnapshot() {
	if b.records == nil {
		b.records = make(map[string]map[string]*tableRecords)
	}

	if b.tags == nil {
		b.tags = make(map[string]map[string]string)
	}

	// Make sure every restored table has a corresponding records slot so
	// WriteRecords always finds an initialised *tableRecords with a mutex.
	for _, tbl := range b.tables.All() {
		if b.records[tbl.DatabaseName] == nil {
			b.records[tbl.DatabaseName] = make(map[string]*tableRecords)
		}

		if b.records[tbl.DatabaseName][tbl.TableName] == nil {
			b.records[tbl.DatabaseName][tbl.TableName] = &tableRecords{
				mu:          lockmetrics.New("timestreamwrite.table"),
				recordIndex: make(map[string]int),
			}
		} else if b.records[tbl.DatabaseName][tbl.TableName].recordIndex == nil {
			b.records[tbl.DatabaseName][tbl.TableName].recordIndex = make(map[string]int)
		}
	}
}
