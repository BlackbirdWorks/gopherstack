package appconfig

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// appconfigSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to a registered table's value type or
// backendSnapshot itself would make an older snapshot unsafe to decode as
// the current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll plus resetting every raw-left map, not a
// partial decode) any mismatch -- see Restore below. This is the first
// version: AppConfig had no persistence at all before Phase 3.3 (Handler
// had no Snapshot/Restore, so cli.go's generic setupPersistence never
// picked it up even though nothing on the backend implemented
// persistence.Persistable either -- see the Handler.Snapshot/Restore
// delegation at the bottom of this file, and the Snapshot/Restore doc
// comment on StorageBackend in interfaces.go), so there is no legacy
// snapshot shape to be compatible with -- any snapshot without a matching
// Version (including one with no version field, which decodes as 0) is
// discarded the same way any other incompatible snapshot is.
const appconfigSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the AppConfig backend.
//
// Tables holds one JSON-encoded array per registered table name, produced
// directly by b.registry.SnapshotAll(): every converted resource collection
// derives its store.Table key entirely from real, already-wire-visible
// fields on the value type (see store_setup.go), so none of them need a DTO
// wrapper.
//
// The remaining fields are the raw (non-store.Table) state left on
// InMemoryBackend -- see its doc comment in store.go for why each stays
// raw:
//   - TagsByArn: values are plain map[string]string, not *T.
//   - VersionCounters/DeploymentCounters: values are int32, not *T, and
//     must survive a restart to avoid reusing a version/deployment number
//     that a since-deleted resource already held.
//   - AccountSettings: a single struct, not a map at all.
type backendSnapshot struct {
	Tables             map[string]json.RawMessage   `json:"tables"`
	TagsByArn          map[string]map[string]string `json:"tagsByArn"`
	VersionCounters    map[string]map[string]int32  `json:"versionCounters"`
	DeploymentCounters map[string]map[string]int32  `json:"deploymentCounters"`
	AccountSettings    AccountSettings              `json:"accountSettings"`
	AccountID          string                       `json:"accountID"`
	Region             string                       `json:"region"`
	Version            int                          `json:"version"`
}

// Snapshot serializes the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "appconfig: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:            appconfigSnapshotVersion,
		Tables:             tables,
		TagsByArn:          b.tags,
		VersionCounters:    b.versionCounters,
		DeploymentCounters: b.deploymentCounters,
		AccountSettings:    b.accountSettings,
		AccountID:          b.accountID,
		Region:             b.region,
	}

	return persistence.MarshalSnapshot(ctx, "appconfig", snap)
}

// Restore deserializes backend state from a snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "appconfig", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != appconfigSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"appconfig: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", appconfigSnapshotVersion)

		b.registry.ResetAll()
		b.tags = make(map[string]map[string]string)
		b.versionCounters = make(map[string]map[string]int32)
		b.deploymentCounters = make(map[string]map[string]int32)
		b.accountSettings = AccountSettings{}
		b.accountID = snap.AccountID
		b.region = snap.Region

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("appconfig: restore snapshot tables: %w", err)
	}

	if snap.TagsByArn == nil {
		snap.TagsByArn = make(map[string]map[string]string)
	}

	b.tags = snap.TagsByArn

	if snap.VersionCounters == nil {
		snap.VersionCounters = make(map[string]map[string]int32)
	}

	b.versionCounters = snap.VersionCounters

	if snap.DeploymentCounters == nil {
		snap.DeploymentCounters = make(map[string]map[string]int32)
	}

	b.deploymentCounters = snap.DeploymentCounters

	b.accountSettings = snap.AccountSettings
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
// Handler previously had no Snapshot/Restore of its own -- and neither did
// InMemoryBackend -- so cli.go's generic setupPersistence (which
// type-asserts the registered service.Registerable, i.e. the Handler, for a
// Snapshot/Restore pair) never picked AppConfig up at all: dead wiring, with
// no persistence underneath it either. This delegation (matching the
// cleanrooms/codecommit/emr/workmail pattern) is what wires AppConfig into
// persistence for the first time.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
