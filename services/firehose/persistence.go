package firehose

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// firehoseSnapshotVersion identifies the shape of [backendSnapshot]. It must
// be bumped whenever a change to the registered streams table's value type or
// backendSnapshot itself would make an older snapshot unsafe to decode as the
// current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll, not a partial decode) any mismatch -- see
// Restore below. The pre-Phase-3.3 snapshot format had no version field at
// all, so an old snapshot decodes with Version == 0, which is guaranteed to
// mismatch firehoseSnapshotVersion and is discarded the same way any other
// incompatible snapshot is.
//
// Bumped 1 -> 2 (gopherstack-hjdd) for d83f4b5d3: MSKSourceDescription.
// ReadFromTimestamp (nested inside the registered "streams" table's value
// type, via DeliveryStream.Source) changed from string to float64, matching
// the real deserializer's epoch-seconds number. A Version-1 snapshot with a
// non-empty "ReadFromTimestamp" string no longer unmarshals into the new
// float64 field -- an outright decode error that takes down the whole
// restore, not silent loss -- so it must be discarded like any other
// shape-incompatible snapshot.
const firehoseSnapshotVersion = 2

// backendSnapshot is the top-level on-disk shape for the Firehose backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// [store.Registry.SnapshotAll] -- here just the single "streams" table (see
// store_setup.go). DeliveryStream is directly JSON-serializable (it already
// carries a proper `json:"region"` tag on the field store.Table's composite
// key is derived from), so no DTO layer is needed the way services/ses or
// services/neptune required for region-nested types with a hidden field.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage `json:"tables"`
	AccountID string                     `json:"accountID"`
	Region    string                     `json:"region"`
	Version   int                        `json:"version"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		// Snapshot has no context parameter; fall back to the default logger.
		logger.Load(ctx).WarnContext(ctx, "firehose: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:   firehoseSnapshotVersion,
		Tables:    tables,
		AccountID: b.accountID,
		Region:    b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "firehose: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "firehose", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	// Close Tags on any streams that are being replaced to prevent
	// Prometheus registry leaks.
	for _, s := range b.streams.All() {
		if s.Tags != nil {
			s.Tags.Close()
		}
	}

	if snap.Version != firehoseSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"firehose: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", firehoseSnapshotVersion)

		b.registry.ResetAll()
		b.accountID = snap.AccountID
		b.region = snap.Region

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("firehose: restore snapshot tables: %w", err)
	}

	now := time.Now()
	for _, s := range b.streams.All() {
		s.lastFlush = now

		// Recalculate bufferSizeBytes because it is not persisted (unexported field).
		// Without this, size-based flush thresholds would never fire after a restore.
		s.bufferSizeBytes = 0
		for _, rec := range s.Records {
			s.bufferSizeBytes += len(rec)
		}
	}

	b.accountID = snap.AccountID
	b.region = snap.Region

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
