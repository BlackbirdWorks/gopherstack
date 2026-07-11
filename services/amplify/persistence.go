package amplify

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// amplifySnapshotVersion identifies the shape of [backendSnapshot]. It must
// be bumped whenever a change to backendSnapshot (or a value type held by one
// of the registered tables) would make an older snapshot unsafe to decode as
// the current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll, not a partial decode) any mismatch -- see
// Restore below. This is the first version: Amplify had no persistence at
// all before Phase 3.3 (dead-wiring: the backend had no Snapshot/Restore and
// Handler did not delegate to any), so there is no legacy snapshot shape to
// be compatible with -- any snapshot without a matching Version (including
// one with no version field, which decodes as 0) is discarded the same way
// any other incompatible snapshot is.
const amplifySnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the Amplify backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// b.registry.SnapshotAll() -- every store.Table-backed resource field is a
// "clean" table (see store_setup.go's file doc comment), so no ephemeral DTO
// registry is needed here. Every resource field on InMemoryBackend converted
// to a store.Table (apps, branches, jobs, domains, webhooks,
// backendEnvironments, artifacts); webhooksByApp is a store.Index, not a
// separately persisted field, and is rebuilt automatically by
// store.Table.Restore from the webhooks table's AppID field. So Tables is
// the entirety of the persisted state besides AccountID/Region.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage `json:"tables"`
	AccountID string                     `json:"accountId"`
	Region    string                     `json:"region"`
	Version   int                        `json:"version"`
}

// Snapshot serialises the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "amplify: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:   amplifySnapshotVersion,
		Tables:    tables,
		AccountID: b.accountID,
		Region:    b.region,
	}

	return persistence.MarshalSnapshot(ctx, "amplify", snap)
}

// Restore loads backend state from a JSON snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "amplify", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != amplifySnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"amplify: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", amplifySnapshotVersion)

		b.registry.ResetAll()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("amplify: restore snapshot tables: %w", err)
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
