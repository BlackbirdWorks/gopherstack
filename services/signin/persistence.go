package signin

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// signinSnapshotVersion: do NOT bump for additive fields; a bump discards user snapshots.
const signinSnapshotVersion = 1

type backendSnapshot struct {
	Tables     map[string]json.RawMessage `json:"tables"`
	AccountID  string                     `json:"accountID"`
	Region     string                     `json:"region"`
	SigningKey string                     `json:"signingKey"`
	Version    int                        `json:"version"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		return nil
	}

	return persistence.MarshalSnapshot(ctx, "signin", backendSnapshot{
		Tables: tables, AccountID: b.accountID, Region: b.region,
		SigningKey: base64.StdEncoding.EncodeToString(b.signingKey), Version: signinSnapshotVersion,
	})
}

// Restore loads state produced by Snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "signin", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != signinSnapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "signin: discarding incompatible snapshot version",
			"got", snap.Version, "want", signinSnapshotVersion)
		b.registry.ResetAll()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("signin: restore tables: %w", err)
	}

	if key, err := base64.StdEncoding.DecodeString(snap.SigningKey); err == nil && len(key) > 0 {
		b.signingKey = key
	}

	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}
