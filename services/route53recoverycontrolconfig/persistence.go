package route53recoverycontrolconfig

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

const snapshotVersion = 1

type backendSnapshot struct {
	Tables  map[string]json.RawMessage `json:"tables"`
	Tokens  map[string]string          `json:"tokens,omitempty"`
	Version int                        `json:"version"`
}

// Snapshot serialises the backend state. It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "route53recoverycontrolconfig: snapshot table marshal failed", "error", err)

		return nil
	}

	return persistence.MarshalSnapshot(ctx, "route53recoverycontrolconfig",
		backendSnapshot{Version: snapshotVersion, Tables: tables, Tokens: b.tokens})
}

// Restore loads a snapshot; an incompatible version starts empty. It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "route53recoverycontrolconfig", data, &snap); err != nil {
		return err
	}

	if b.mu == nil {
		b.mu = lockmetrics.New("route53recoverycontrolconfig")
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != snapshotVersion {
		b.registry.ResetAll()
		b.tokens = map[string]string{}

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("route53recoverycontrolconfig: restore snapshot tables: %w", err)
	}

	b.tokens = snap.Tokens
	if b.tokens == nil {
		b.tokens = map[string]string{}
	}

	return nil
}
