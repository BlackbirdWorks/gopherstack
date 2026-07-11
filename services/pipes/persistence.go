package pipes

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// pipesSnapshotVersion identifies the shape of backendSnapshot's Tables blob
// (i.e. the set of resource tables registered on b.registry -- see
// store_setup.go). It must be bumped whenever a change there would make an
// older snapshot unsafe to decode as the current shape. Pre-Phase-3.3
// snapshots have no "version" field at all, which unmarshals as 0 -- also a
// mismatch, so they are discarded the same way. This mirrors the
// services/eventbridge, services/ssm, services/sqs (commit 0f09d77c), and
// services/ec2 (commit 12e611a4) conversions.
const pipesSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the Pipes backend.
//
// Tables holds one JSON-encoded, key-sorted array per registered
// *store.Table[V] on b.registry, produced by
// [github.com/blackbirdworks/gopherstack/pkgs/store.Registry.SnapshotAll].
// Each per-region table is registered under "<resourceName>/<region>" (pipes,
// enrichmentCounts) -- see store_setup.go's pipesTable/enrichmentCountsTable.
// Restore must pre-register every (resource, region) tuple found in Tables
// before calling registry.RestoreAll, see preRegisterSnapshotTables below.
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
		// The registered tables are plain JSON-friendly structs, so a marshal
		// failure here would indicate a programming error rather than bad
		// input data. Log and skip the snapshot rather than panic, matching
		// the persistence.Persistable contract (nil is skipped by the Manager).
		logger.Load(ctx).WarnContext(ctx, "pipes: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:   pipesSnapshotVersion,
		Tables:    tables,
		AccountID: b.accountID,
		Region:    b.region,
	}

	return persistence.MarshalSnapshot(ctx, "pipes", snap)
}

// preRegisterSnapshotTables ensures every (resource, region) pair present in
// tables is registered on b.registry before registry.RestoreAll is called.
// RestoreAll only restores tables already registered (see
// [github.com/blackbirdworks/gopherstack/pkgs/store.Registry.RestoreAll]); a
// region a fresh backend has never touched would otherwise not yet have a
// table registered under that name, and its persisted data would be silently
// dropped instead of restored. Each table name has the shape
// "<resourceName>/<region>" (see store_setup.go's pipesTable/
// enrichmentCountsTable); splitting on the first "/" recovers both parts
// (region names never contain "/").
func (b *InMemoryBackend) preRegisterSnapshotTables(tables map[string]json.RawMessage) {
	for key := range tables {
		name, region, found := strings.Cut(key, "/")
		if !found {
			continue
		}

		switch name {
		case "pipes":
			b.pipesTable(region)
		case "enrichmentCounts":
			b.enrichmentCountsTable(region)
		}
	}
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "pipes", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != pipesSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption. Mirrors the services/eventbridge, services/ssm,
		// services/sqs pilot (commit 0f09d77c), and services/ec2 conversion
		// (commit 12e611a4).
		logger.Load(ctx).WarnContext(ctx,
			"pipes: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", pipesSnapshotVersion)

		b.registry.ResetAll()

		return nil
	}

	if snap.Tables == nil {
		snap.Tables = make(map[string]json.RawMessage)
	}

	b.preRegisterSnapshotTables(snap.Tables)

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("pipes: restore snapshot tables: %w", err)
	}

	b.accountID = snap.AccountID
	b.region = snap.Region

	// The pipesByARN secondary index is rebuilt automatically by
	// store.Table.Restore (it re-adds every restored value to every index
	// registered via AddIndex), so no manual index rebuild is needed here --
	// unlike services/eventbridge's hand-maintained ruleIndex/targetsByARN,
	// which live outside store.Table and must be rebuilt explicitly.

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
