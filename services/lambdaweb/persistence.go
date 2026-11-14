package lambdaweb

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// ErrNoSnapshot is returned when a backend does not support snapshot/restore.
var ErrNoSnapshot = errors.New("backend does not support restore")

// Snapshottable is implemented by backends that support snapshot/restore.
type Snapshottable interface {
	Snapshot(ctx context.Context) []byte
	Restore(context.Context, []byte) error
}

// snapshotVersion identifies the shape of [backendSnapshot]; Restore discards any other version.
const snapshotVersion = 1

const persistName = "lambdaweb"

type backendSnapshot struct {
	Tables  map[string]jsontext.Value `json:"tables"`
	Version int                       `json:"version"`
}

// Snapshot serializes backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "lambdaweb: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{Version: snapshotVersion, Tables: tables}

	return persistence.MarshalSnapshot(ctx, persistName, &snap)
}

// Restore deserializes backend state from a JSON snapshot. Execution environments are ephemeral and start again
// on the next request.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, persistName, data, &snap); err != nil {
		return err
	}

	b.run.stopAll()

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "lambdaweb: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.registry.ResetAll()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("lambdaweb: restore snapshot tables: %w", err)
	}

	return nil
}

// Snapshot implements persistence by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	s, ok := h.Backend.(Snapshottable)
	if !ok {
		return nil
	}

	return s.Snapshot(ctx)
}

// Restore implements persistence by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	s, ok := h.Backend.(Snapshottable)
	if !ok {
		return ErrNoSnapshot
	}

	return s.Restore(ctx, data)
}
