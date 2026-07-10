package appmesh

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// appmeshSnapshotVersion identifies the shape of [backendSnapshot]. It must
// be bumped whenever a change to a registered table's value type or to
// backendSnapshot itself would make an older snapshot unsafe to decode as
// the current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll, not a partial decode) any mismatch -- see
// Restore below. App Mesh had no persistence at all before Phase 3.3, so
// there is no legacy snapshot shape to be compatible with: any snapshot
// without a matching Version (including one with no version field at all,
// which decodes as 0) is discarded the same way any other incompatible
// snapshot is.
const appmeshSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the App Mesh backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// [store.Registry.SnapshotAll] -- meshes, virtualNodes, virtualRouters,
// routes, virtualServices, virtualGateways, and gatewayRoutes (see
// store_setup.go). None of these need a DTO wrapper: every value type
// already carries its own composite-key components as real, wire-visible
// fields, so a plain json.Marshal/Unmarshal of the value round-trips
// everything store.Table needs to rebuild both the primary map and every
// secondary Index. Tags is left as a plain field: it is a non-*T value map
// (map[string]map[string]string, keyed by ARN), which does not fit
// store.Table's keyed shape. Version guards against decoding a snapshot from
// an incompatible (older or newer) build of this backend as though it were
// the current shape; see Restore.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage   `json:"tables"`
	Tags      map[string]map[string]string `json:"tags"`
	AccountID string                       `json:"accountID"`
	Region    string                       `json:"region"`
	Version   int                          `json:"version"`
}

// Snapshot serialises the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		// The registered tables are plain JSON-friendly structs, so a
		// marshal failure here would indicate a programming error rather
		// than bad input data. Log and skip the snapshot rather than panic,
		// matching the persistence.Persistable contract (nil is skipped by
		// the Manager).
		logger.Load(ctx).WarnContext(ctx, "appmesh: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:   appmeshSnapshotVersion,
		Tables:    tables,
		Tags:      b.tags,
		AccountID: b.accountID,
		Region:    b.region,
	}

	return persistence.MarshalSnapshot(ctx, "appmesh", snap)
}

// Restore loads backend state from a JSON snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "appmesh", data, &snap); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if snap.Version != appmeshSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never
		// be partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"appmesh: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", appmeshSnapshotVersion)

		b.registry.ResetAll()
		b.tags = make(map[string]map[string]string)
		b.accountID = snap.AccountID
		b.region = snap.Region

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("appmesh: restore snapshot tables: %w", err)
	}

	if snap.Tags == nil {
		snap.Tags = make(map[string]map[string]string)
	}
	b.tags = snap.Tags

	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
// Neither the Handler nor the backend had a Snapshot/Restore pair before
// this conversion -- App Mesh had no persistence at all -- so cli.go's
// generic setupPersistence (which discovers services via a
// persistence.Persistable type assertion on the registered Handler) never
// picked App Mesh up. This adds the delegation, matching every other
// converted Phase 3.3 service.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
