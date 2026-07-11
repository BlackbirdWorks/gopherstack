package detective

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// detectiveSnapshotVersion identifies the shape of [backendSnapshot]. It must
// be bumped whenever a change to backendSnapshot (or a value type held by one
// of the registered tables) would make an older snapshot unsafe to decode as
// the current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll plus the raw maps' own reset, not a partial
// decode) any mismatch -- see Restore. The pre-Phase-3.3 snapshot format had
// no version field at all, so an old snapshot decodes with Version == 0,
// which is guaranteed to mismatch detectiveSnapshotVersion and is discarded
// the same way any other incompatible snapshot is.
const detectiveSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the Detective backend.
//
// Tables holds one JSON-encoded array per registered table name (graphs,
// members, investigations), produced by b.registry.SnapshotAll() -- all
// three are "clean" tables (see store_setup.go's file doc comment), so no
// ephemeral DTO registry is needed. Tags, Datasources, and OrgConfigs remain
// plain maps (their values are not *T) and OrgAdmins remains a raw
// order-sensitive slice; all four are carried directly, matching the
// pre-Phase-3.3 snapshot shape for those fields.
type backendSnapshot struct {
	Tables      map[string]json.RawMessage   `json:"tables"`
	Tags        map[string]map[string]string `json:"tags"`
	Datasources map[string]map[string]string `json:"datasources"`
	OrgConfigs  map[string]bool              `json:"orgConfigs"`
	OrgAdmins   []*storedOrgAdmin            `json:"orgAdmins"`
	Version     int                          `json:"version"`
}

// Snapshot serializes the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "detective: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:     detectiveSnapshotVersion,
		Tables:      tables,
		Tags:        b.tags,
		Datasources: b.datasources,
		OrgConfigs:  b.orgConfigs,
		OrgAdmins:   b.orgAdmins,
	}

	return persistence.MarshalSnapshot(ctx, "detective", snap)
}

// Restore deserializes backend state from a snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "detective", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != detectiveSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"detective: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", detectiveSnapshotVersion)

		b.registry.ResetAll()
		b.tags = make(map[string]map[string]string)
		b.datasources = make(map[string]map[string]string)
		b.orgConfigs = make(map[string]bool)
		b.orgAdmins = nil

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("detective: restore snapshot tables: %w", err)
	}

	if snap.Tags == nil {
		snap.Tags = make(map[string]map[string]string)
	}

	if snap.Datasources == nil {
		snap.Datasources = make(map[string]map[string]string)
	}

	if snap.OrgConfigs == nil {
		snap.OrgConfigs = make(map[string]bool)
	}

	b.tags = snap.Tags
	b.datasources = snap.Datasources
	b.orgConfigs = snap.OrgConfigs
	b.orgAdmins = snap.OrgAdmins

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
// Without this, cli.go's generic setupPersistence (which type-asserts the
// registered service.Registerable, i.e. *Handler, for Snapshot/Restore) never
// finds a persistable Detective service even though InMemoryBackend itself
// has always implemented Snapshot/Restore -- the backend methods were dead
// wiring until Handler delegated to them.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
