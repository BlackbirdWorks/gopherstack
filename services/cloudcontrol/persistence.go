package cloudcontrol

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// cloudcontrolSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to a registered table's value type or
// backendSnapshot itself would make an older snapshot unsafe to decode as
// the current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll plus resetting clientTokens, not a partial
// decode) any mismatch -- see Restore below. This is the first version:
// CloudControl had no persistence at all before Phase 3.3 -- neither
// InMemoryBackend nor Handler implemented persistence.Persistable, so
// cli.go's generic setupPersistence (which type-asserts each registered
// service.Registerable for a Snapshot/Restore pair) never picked this
// service up at all: dead wiring, with no persistence underneath it either.
// So there is no legacy snapshot shape to be compatible with -- any snapshot
// without a matching Version (including one with no version field, which
// decodes as 0) is discarded the same way any other incompatible snapshot
// is.
const cloudcontrolSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the CloudControl
// backend.
//
// Tables holds one JSON-encoded array per registered table name, produced
// directly by b.registry.SnapshotAll(): both converted resource collections
// (resources, requests) derive their store.Table key entirely from real,
// already-wire-visible fields on the value type -- see store_setup.go -- so
// neither needs a DTO wrapper.
//
// ClientTokens is the one remaining raw (non-store.Table) map: its value is
// a bare string (the idempotency-cached requestToken), not *T, so there is
// nothing for store.Table to key on. It is persisted directly here so a
// repeated CreateResource ClientToken remains idempotent across a restart.
type backendSnapshot struct {
	Tables       map[string]json.RawMessage `json:"tables"`
	ClientTokens map[string]string          `json:"clientTokens"`
	AccountID    string                     `json:"accountID"`
	Region       string                     `json:"region"`
	Version      int                        `json:"version"`
}

// Snapshot serializes the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "cloudcontrol: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:      cloudcontrolSnapshotVersion,
		Tables:       tables,
		ClientTokens: b.clientTokens,
		AccountID:    b.accountID,
		Region:       b.region,
	}

	return persistence.MarshalSnapshot(ctx, "cloudcontrol", snap)
}

// Restore deserializes backend state from a snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "cloudcontrol", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != cloudcontrolSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"cloudcontrol: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", cloudcontrolSnapshotVersion)

		b.registry.ResetAll()
		b.clientTokens = make(map[string]string)

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("cloudcontrol: restore snapshot tables: %w", err)
	}

	b.clientTokens = snap.ClientTokens
	if b.clientTokens == nil {
		b.clientTokens = make(map[string]string)
	}

	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
// Handler previously had no Snapshot/Restore of its own -- and neither did
// InMemoryBackend -- so cli.go's generic setupPersistence never picked
// CloudControl up at all. This delegation (matching the
// codecommit/cleanrooms pattern) is what wires CloudControl into persistence
// for the first time.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
