package sagemakerruntime

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// sagemakerruntimeSnapshotVersion identifies the shape of [backendSnapshot].
// It must be bumped whenever a change to backendSnapshot (or a value type
// held by one of the registered tables) would make an older snapshot unsafe
// to decode as the current shape. Restore compares this against the
// persisted value and discards (ResetAll, not a partial decode) any
// mismatch -- see Restore. The pre-Phase-3.3 snapshot format had no version
// field at all, so an old snapshot decodes with Version == 0, which is
// guaranteed to mismatch sagemakerruntimeSnapshotVersion and is discarded
// the same way any other incompatible snapshot is.
const sagemakerruntimeSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the SageMaker Runtime
// backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// b.registry.SnapshotAll() -- both store.Table-backed resource fields
// (sessions, asyncInvocations) are "clean" tables (see store_setup.go's file
// doc comment), so no ephemeral DTO registry is needed here. Invocations is
// left as a plain, order-sensitive slice (see store_setup.go) and rides
// alongside the tables. Version guards against decoding a snapshot from an
// incompatible (older or newer) build of this backend as though it were the
// current shape; see Restore.
type backendSnapshot struct {
	Tables      map[string]json.RawMessage `json:"tables"`
	Invocations []*Invocation              `json:"invocations"`
	NextID      uint64                     `json:"nextId"`
	Version     int                        `json:"version"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	invCopy := make([]*Invocation, len(b.invocations))
	for i, inv := range b.invocations {
		cp := *inv
		invCopy[i] = &cp
	}

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "sagemakerruntime: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:     sagemakerruntimeSnapshotVersion,
		Tables:      tables,
		Invocations: invCopy,
		NextID:      b.nextID,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "sagemakerruntime: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "sagemakerruntime", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != sagemakerruntimeSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"sagemakerruntime: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", sagemakerruntimeSnapshotVersion)

		b.registry.ResetAll()
		b.invocations = make([]*Invocation, 0)
		b.nextID = 0

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("sagemakerruntime: restore snapshot tables: %w", err)
	}

	if snap.Invocations == nil {
		snap.Invocations = make([]*Invocation, 0)
	}

	b.invocations = snap.Invocations
	b.nextID = snap.NextID

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
