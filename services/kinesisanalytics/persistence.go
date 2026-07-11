package kinesisanalytics

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// kinesisanalyticsSnapshotVersion identifies the shape of [backendSnapshot].
// It must be bumped whenever a change to the persisted Application shape or
// backendSnapshot itself would make an older snapshot unsafe to decode as
// the current shape. Restore discards (rather than partially decodes) any
// mismatch -- see Restore below. This is version 1: the first snapshot
// format built on top of pkgs/store's Table/Registry (the prior format
// persisted a bare region-nested map with no version field at all, so it
// always decodes as Version 0 and is treated as incompatible).
const kinesisanalyticsSnapshotVersion = 1

// backendSnapshot is the persisted form of the backend.
//
// Tables holds one JSON-encoded array per table registered on b.registry --
// currently just "apps" ([]*Application), produced by
// [github.com/blackbirdworks/gopherstack/pkgs/store.Registry.SnapshotAll].
// Application is persisted directly (no DTO) because every field, including
// the additive Region, already round-trips through encoding/json.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage `json:"tables"`
	AccountID string                     `json:"accountID"`
	Region    string                     `json:"region"`
	NextID    int64                      `json:"next_id"`
	Version   int                        `json:"version"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "kinesisanalytics: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:   kinesisanalyticsSnapshotVersion,
		Tables:    tables,
		AccountID: b.accountID,
		Region:    b.defaultRegion,
		NextID:    b.nextID,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "kinesisanalytics: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "kinesisanalytics", data, &snap); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if snap.Version != kinesisanalyticsSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields (e.g. the pre-store region-nested map format
		// this version replaced). Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition, not
		// data corruption.
		logger.Load(ctx).WarnContext(ctx,
			"kinesisanalytics: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", kinesisanalyticsSnapshotVersion)

		b.registry.ResetAll()
		b.nextID = 0

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("kinesisanalytics: restore snapshot tables: %w", err)
	}

	b.accountID = snap.AccountID
	b.defaultRegion = snap.Region
	b.nextID = snap.NextID

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	if sb, ok := h.Backend.(*InMemoryBackend); ok {
		return sb.Snapshot(ctx)
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if sb, ok := h.Backend.(*InMemoryBackend); ok {
		return sb.Restore(ctx, data)
	}

	return nil
}
