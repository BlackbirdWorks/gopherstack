package transfer

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// transferSnapshotVersion identifies the shape of [backendSnapshot]. It must
// be bumped whenever a change to backendSnapshot (or the shape any table it
// references persists) would make an older snapshot unsafe to decode as the
// current shape. Restore compares this against the persisted value and
// discards (ResetAll, not a partial decode) any mismatch -- see Restore. The
// pre-Phase-3.3 snapshot format had no version field at all, so an old
// snapshot decodes with Version == 0, which is guaranteed to mismatch
// transferSnapshotVersion and is discarded the same way any other
// incompatible snapshot is.
const transferSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the Transfer backend.
//
// Tables holds one JSON-encoded array per registered resource table,
// produced by [store.Registry.SnapshotAll] via persistTables -- e.g.
// "servers" ([]*Server), "users" ([]*User) (composite-keyed, but each User
// still carries its own ServerID/UserName so no wrapping DTO is needed).
// webAppCustomizations and sshKeyBodies are deliberately absent from Tables
// (and TagsStore is handled separately below) -- see store_setup.go and the
// field comments on InMemoryBackend for why.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage   `json:"tables"`
	TagsStore map[string]map[string]string `json:"tags_store"`
	Version   int                          `json:"version"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := persistTables(b).SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "transfer: failed to marshal snapshot", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:   transferSnapshotVersion,
		Tables:    tables,
		TagsStore: snapshotTagsStore(b.tagsStore),
	}

	return persistence.MarshalSnapshot(ctx, "transfer", snap)
}

// snapshotTagsStore clones the tags store map.
func snapshotTagsStore(src map[string]map[string]string) map[string]map[string]string {
	dst := make(map[string]map[string]string, len(src))
	for arn, tagMap := range src {
		m := make(map[string]string, len(tagMap))
		maps.Copy(m, tagMap)
		dst[arn] = m
	}

	return dst
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "transfer", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != transferSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"transfer: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", transferSnapshotVersion)

		b.registry.ResetAll()
		b.tagsStore = make(map[string]map[string]string)

		return nil
	}

	if err := persistTables(b).RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("transfer: restore snapshot tables: %w", err)
	}

	if snap.TagsStore != nil {
		b.tagsStore = snap.TagsStore
	} else {
		b.tagsStore = make(map[string]map[string]string)
	}

	// sshKeyBodies (the SSH-key-body dedup index) is intentionally NOT
	// rebuilt from the restored sshPublicKeys table here -- it never was
	// before this conversion either (the pre-Phase-3.3 Restore didn't touch
	// it, leaving whatever NewInMemoryBackend had freshly initialised). This
	// refactor preserves that pre-existing gap rather than fixing it as a
	// side effect of an unrelated datalayer swap.

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
