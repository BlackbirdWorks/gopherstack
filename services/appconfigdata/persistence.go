package appconfigdata

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// appconfigdataSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to a registered table's value type or
// backendSnapshot itself would make an older snapshot unsafe to decode as
// the current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll, not a partial decode) any mismatch -- see
// Restore below. This is the first version: AppConfigData had no
// persistence at all before Phase 3.3 (Handler had no Snapshot/Restore, so
// cli.go's generic setupPersistence never picked it up even though nothing
// on the backend implemented persistence.Persistable either -- see the
// Handler.Snapshot/Restore delegation at the bottom of this file, and the
// Snapshot/Restore doc comment on StorageBackend in backend_iface.go), so
// there is no legacy snapshot shape to be compatible with -- any snapshot
// without a matching Version (including one with no version field, which
// decodes as 0) is discarded the same way any other incompatible snapshot
// is.
const appconfigdataSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the AppConfigData
// backend.
//
// Tables holds one JSON-encoded array per registered table name (profiles,
// sessions, graceTokens), produced directly by b.registry.SnapshotAll() --
// see store_setup.go.
//
// signingKey is deliberately NOT persisted, matching the sibling AppConfig
// (control-plane) service's paginationSecret: it is regenerated fresh on
// every process start (see NewInMemoryBackend). A consequence is that any
// session token restored from a snapshot will fail its HMAC check
// (verifyTokenMAC) against the new key on first use after a restart,
// surfacing as ErrTokenCorrupted -- the same "secret doesn't survive a
// restart" limitation AppConfig already accepts for its pagination tokens,
// rather than a new one introduced here. Persisting cryptographic key
// material to disk was judged not worth the added attack surface for a
// session-token integrity check.
type backendSnapshot struct {
	Tables  map[string]json.RawMessage `json:"tables"`
	Version int                        `json:"version"`
}

// Snapshot serializes the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "appconfigdata: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version: appconfigdataSnapshotVersion,
		Tables:  tables,
	}

	return persistence.MarshalSnapshot(ctx, "appconfigdata", snap)
}

// Restore deserializes backend state from a snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "appconfigdata", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != appconfigdataSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"appconfigdata: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", appconfigdataSnapshotVersion)

		b.registry.ResetAll()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("appconfigdata: restore snapshot tables: %w", err)
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
// Handler previously had no Snapshot/Restore of its own -- and neither did
// InMemoryBackend -- so cli.go's generic setupPersistence (which
// type-asserts the registered service.Registerable, i.e. the Handler, for a
// Snapshot/Restore pair) never picked AppConfigData up at all: dead wiring,
// with no persistence underneath it either. This delegation (matching the
// cleanrooms/appconfig/codecommit pattern) is what wires AppConfigData into
// persistence for the first time.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
