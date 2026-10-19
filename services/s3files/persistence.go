package s3files

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// s3filesSnapshotVersion: do NOT bump for additive fields; a bump discards user snapshots.
const s3filesSnapshotVersion = 1

type backendSnapshot struct {
	Tables    map[string]json.RawMessage `json:"tables"`
	AccountID string                     `json:"accountID"`
	Region    string                     `json:"region"`
	Version   int                        `json:"version"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		return nil
	}

	return persistence.MarshalSnapshot(ctx, "s3files", backendSnapshot{
		Tables: tables, AccountID: b.accountID, Region: b.region, Version: s3filesSnapshotVersion,
	})
}

// Restore loads state produced by Snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "s3files", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != s3filesSnapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "s3files: discarding incompatible snapshot version",
			"got", snap.Version, "want", s3filesSnapshotVersion)
		b.registry.ResetAll()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("s3files: restore tables: %w", err)
	}

	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}
