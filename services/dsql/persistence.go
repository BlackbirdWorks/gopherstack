package dsql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// ErrNoSnapshot is returned when a backend does not support snapshot/restore.
var ErrNoSnapshot = errors.New("backend does not support restore")

// Snapshottable is an optional interface a StorageBackend may implement to
// support snapshot/restore for persistence or test isolation.
type Snapshottable interface {
	Snapshot(ctx context.Context) []byte
	Restore(context.Context, []byte) error
}

// dsqlSnapshotVersion identifies the shape of [backendSnapshot]. Bump it
// whenever a change would make an older snapshot unsafe to decode as the
// current shape; Restore discards (rather than partially decodes) any mismatch.
const dsqlSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the backend. Tables
// holds one JSON-encoded array per registered table name (clusters, streams
// -- see store_setup.go), produced by b.registry.SnapshotAll().
type backendSnapshot struct {
	Tables  map[string]json.RawMessage `json:"tables"`
	Version int                        `json:"version"`
}

// Snapshot serializes backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "dsql: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{Version: dsqlSnapshotVersion, Tables: tables}

	return persistence.MarshalSnapshot(ctx, dsqlServiceName, &snap)
}

// Restore deserializes backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, dsqlServiceName, data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != dsqlSnapshotVersion {
		logger.Load(ctx).WarnContext(ctx,
			"dsql: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", dsqlSnapshotVersion)

		b.registry.ResetAll()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("dsql: restore snapshot tables: %w", err)
	}

	return nil
}

// Snapshot implements persistence by delegating to the backend if it supports it.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	s, ok := h.Backend.(Snapshottable)
	if !ok {
		return nil
	}

	return s.Snapshot(ctx)
}

// Restore implements persistence by delegating to the backend if it supports it.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	s, ok := h.Backend.(Snapshottable)
	if !ok {
		return ErrNoSnapshot
	}

	return s.Restore(ctx, data)
}
