package workspaces

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// workspacesSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to backendSnapshot (or a value type held
// by one of the registered tables) would make an older snapshot unsafe to
// decode as the current shape. Restore compares this against the persisted
// value and discards (registry.ResetAll, not a partial decode) any mismatch
// -- see Restore below. This is the first version: the pre-Phase-3.3
// snapshot format (Workspaces/Tags only, no version field at all) also fails
// this check and is discarded the same way any other incompatible snapshot
// is, so no compatibility bridge is needed.
const workspacesSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the WorkSpaces backend.
//
// Tables holds one JSON-encoded array per registered table name (see
// store_setup.go's registerAllTables): the 12 "clean" tables -- workspaces,
// ipGroups, connAliases, customBundles, images, pools, poolSessions,
// connectAddIns, clientBranding, accountLinks, applications, dirSettings --
// produced by b.registry.SnapshotAll(). No ephemeral DTO registry is needed
// here, unlike services/codecommit: every value type already carries a real,
// non-json:"-" identity field.
//
// Tags is the one plain (non-store.Table) map that was already persisted
// pre-Phase-3.3 (the original backendSnapshot carried Workspaces+Tags) and
// remains so. DirectoryIpGroups, AccountConfig, and AccountModifications were
// added when the AssociateIpGroups/DisassociateIpGroups and
// DescribeAccountModifications persistence gaps were fixed (all were
// ephemeral pre-fix, see PARITY.md gaps/deferred history) -- bumping
// workspacesSnapshotVersion isn't required for these additions since an older
// snapshot simply decodes with empty/nil/zero-value fields, matching the
// prior (always-reset-after-restart) behavior exactly; no field changed
// meaning or shape. imagePermissions, clientProperties, and appAssociations
// remain NOT part of the snapshot -- still ephemeral, matching prior behavior
// (see the field comments on InMemoryBackend in backend.go). Version guards
// against decoding a snapshot from an incompatible (older or newer) build of
// this backend as though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables               map[string]json.RawMessage     `json:"tables"`
	Tags                 map[string]map[string]string   `json:"tags"`
	DirectoryIpGroups    map[string]map[string]struct{} `json:"directoryIpGroups"` //nolint:revive,staticcheck // existing.
	AccountConfig        storedAccountConfig            `json:"accountConfig"`
	AccountModifications []AccountModification          `json:"accountModifications"`
	Version              int                            `json:"version"`
}

// Snapshot serializes the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "workspaces: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:              workspacesSnapshotVersion,
		Tables:               tables,
		Tags:                 b.tags,
		DirectoryIpGroups:    b.directoryIpGroups,
		AccountConfig:        b.accountConfig,
		AccountModifications: b.accountModifications,
	}

	return persistence.MarshalSnapshot(ctx, "workspaces", snap)
}

// Restore deserializes backend state from a snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "workspaces", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != workspacesSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"workspaces: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", workspacesSnapshotVersion)

		b.registry.ResetAll()
		b.tags = make(map[string]map[string]string)
		b.directoryIpGroups = make(map[string]map[string]struct{})
		b.accountConfig = storedAccountConfig{}
		b.accountModifications = nil

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("workspaces: restore snapshot tables: %w", err)
	}

	b.tags = snap.Tags
	if b.tags == nil {
		b.tags = make(map[string]map[string]string)
	}

	b.directoryIpGroups = snap.DirectoryIpGroups
	if b.directoryIpGroups == nil {
		b.directoryIpGroups = make(map[string]map[string]struct{})
	}

	b.accountConfig = snap.AccountConfig
	b.accountModifications = snap.AccountModifications

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
// Handler previously had no Snapshot/Restore of its own, so cli.go's generic
// setupPersistence (which type-asserts the registered service.Registerable,
// i.e. the Handler, for a Snapshot/Restore pair) never picked WorkSpaces up
// even though InMemoryBackend already implemented persistence.Persistable --
// dead wiring. Adding this delegation (matching the codecommit/codepipeline/
// emr pattern) is what actually wires WorkSpaces into persistence.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
