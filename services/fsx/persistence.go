package fsx

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// fsxSnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to backendSnapshot (or a value type held by one of
// the registered tables) would make an older snapshot unsafe to decode as the
// current shape. Restore compares this against the persisted value and
// discards (ResetAll, not a partial decode) any mismatch -- see Restore. The
// pre-Phase-3.3 snapshot format had no version field at all, so an old
// snapshot decodes with Version == 0, which is guaranteed to mismatch
// fsxSnapshotVersion and is discarded the same way any other incompatible
// snapshot is.
//
// Bumped to 2 when CreateFileSystemTokens (ClientRequestToken dedup state)
// was added to the snapshot shape.
const fsxSnapshotVersion = 2

// backendSnapshot is the top-level on-disk shape for the FSx backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// b.registry.SnapshotAll() -- every store.Table-backed resource field is a
// "clean" table (see store_setup.go's file doc comment), so no ephemeral
// DTO registry is needed here. Version guards against decoding a snapshot
// from an incompatible (older or newer) build of this backend as though it
// were the current shape; see Restore.
//
// Tags, Aliases, and CreateFileSystemTokens are left as plain maps -- see
// store_setup.go's file doc comment for why they do not fit store.Table's
// keyed-by-identity-value shape.
type backendSnapshot struct {
	Tables                 map[string]json.RawMessage    `json:"tables"`
	Tags                   map[string]map[string]string  `json:"tags"`
	Aliases                map[string][]string           `json:"aliases"`
	CreateFileSystemTokens map[string]fsCreateTokenEntry `json:"createFileSystemTokens"`
	SharedVpcEnabled       string                        `json:"sharedVpcEnabled"`
	Version                int                           `json:"version"`
}

func ensureNonNilMaps(s *backendSnapshot) {
	if s.Tags == nil {
		s.Tags = make(map[string]map[string]string)
	}

	if s.Aliases == nil {
		s.Aliases = make(map[string][]string)
	}

	if s.CreateFileSystemTokens == nil {
		s.CreateFileSystemTokens = make(map[string]fsCreateTokenEntry)
	}
}

// Snapshot serializes backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "fsx: snapshot table marshal failed", "error", err)

		return nil
	}

	return persistence.MarshalSnapshot(ctx, "fsx", backendSnapshot{
		Version:                fsxSnapshotVersion,
		Tables:                 tables,
		Tags:                   b.tags,
		Aliases:                b.aliases,
		CreateFileSystemTokens: b.createFileSystemTokens,
		SharedVpcEnabled:       b.sharedVpcEnabled,
	})
}

// Restore deserializes backend state from JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var s backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "fsx", data, &s); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if s.Version != fsxSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"fsx: discarding incompatible snapshot version, starting empty",
			"gotVersion", s.Version, "wantVersion", fsxSnapshotVersion)

		b.registry.ResetAll()
		b.tags = make(map[string]map[string]string)
		b.aliases = make(map[string][]string)
		b.createFileSystemTokens = make(map[string]fsCreateTokenEntry)
		b.sharedVpcEnabled = sharedVpcDisabled

		return nil
	}

	if err := b.registry.RestoreAll(s.Tables); err != nil {
		return fmt.Errorf("fsx: restore snapshot tables: %w", err)
	}

	ensureNonNilMaps(&s)

	b.tags = s.Tags
	b.aliases = s.Aliases
	b.createFileSystemTokens = s.CreateFileSystemTokens
	b.sharedVpcEnabled = s.SharedVpcEnabled

	return nil
}
