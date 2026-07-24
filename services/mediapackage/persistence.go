package mediapackage

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// mediapackageSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to backendSnapshot (or a value type held
// by one of the registered tables) would make an older snapshot unsafe to
// decode as the current shape. Restore compares this against the persisted
// value and discards (registry.ResetAll, not a partial decode) any mismatch
// -- see Restore. The pre-Phase-3.3 snapshot format had no version field at
// all, so an old snapshot decodes with Version == 0, which is guaranteed to
// mismatch mediapackageSnapshotVersion and is discarded the same way any
// other incompatible snapshot is.
const mediapackageSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the MediaPackage
// backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// b.registry.SnapshotAll() (channels, originEndpoints, harvestJobs -- see
// store_setup.go). Tags is left as a plain
// field: it is a non-*T value map (map[string]map[string]string, keyed by
// ARN), which does not fit store.Table's keyed shape. Version guards against
// decoding a snapshot from an incompatible (older or newer) build of this
// backend as though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage   `json:"tables"`
	Tags      map[string]map[string]string `json:"tags"`
	AccountID string                       `json:"accountId"`
	Region    string                       `json:"region"`
	Version   int                          `json:"version"`
}

func ensureNonNilMaps(s *backendSnapshot) {
	if s.Tags == nil {
		s.Tags = make(map[string]map[string]string)
	}
}

// Snapshot returns a JSON-encoded snapshot of the backend state.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "mediapackage: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:   mediapackageSnapshotVersion,
		Tables:    tables,
		Tags:      b.tags,
		AccountID: b.accountID,
		Region:    b.region,
	}

	return persistence.MarshalSnapshot(ctx, "mediapackage", snap)
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "mediapackage", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != mediapackageSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"mediapackage: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", mediapackageSnapshotVersion)

		b.registry.ResetAll()
		b.tags = make(map[string]map[string]string)

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("mediapackage: restore snapshot tables: %w", err)
	}

	ensureNonNilMaps(&snap)

	b.tags = snap.Tags
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
