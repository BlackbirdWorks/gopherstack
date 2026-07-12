package medialive

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// Snapshot implements persistence.Persistable by delegating to the backend.
//
// h.Backend is the StorageBackend interface, which already declares
// Snapshot(ctx context.Context) []byte (see interfaces.go) with a shape
// matching persistence.Persistable exactly, so this can call it directly --
// no local type assertion needed. But interface membership alone does not
// help: h.Backend is a named field, not an embedded one, so InMemoryBackend's
// methods are never promoted onto *Handler. Without this delegation, cli.go's
// setupPersistence type-asserts the registered service.Registerable (this
// *Handler) against persistence.Persistable, fails silently, and never
// registers medialive for snapshot/restore despite the backend being fully
// capable. Mirrors services/securityhub's Handler-level delegation.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}

// Compile-time proof Handler satisfies the persistence layer's contract.
var _ persistence.Persistable = (*Handler)(nil)

// medialiveSnapshotVersion identifies the shape of [backendSnapshot]. It must
// be bumped whenever a change to backendSnapshot (or a value type held by one
// of the registered tables) would make an older snapshot unsafe to decode as
// the current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll, not a partial decode) any mismatch -- see
// Restore. The pre-Phase-3.3 snapshot format had no version field at all, so
// an old snapshot decodes with Version == 0, which is guaranteed to mismatch
// medialiveSnapshotVersion and is discarded the same way any other
// incompatible snapshot is.
const medialiveSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the MediaLive backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// b.registry.SnapshotAll() -- every store.Table-backed resource field is a
// "clean" table (see store_setup.go's file doc comment), so no ephemeral
// DTO registry is needed here. Tags and ScheduleActions are left as plain
// maps (not store.Table-backed, see store_setup.go) but were persisted
// before this refactor and remain so. PendingTransferDeviceIDs is
// intentionally absent: it is an ephemeral reverse index rebuilt from
// InputDevices on Restore, and was never part of the persisted shape.
type backendSnapshot struct {
	Tables          map[string]json.RawMessage         `json:"tables"`
	Tags            map[string]map[string]string       `json:"tags"`
	ScheduleActions map[string][]*storedScheduleAction `json:"scheduleActions"`
	AccountKmsKeyID string                             `json:"accountKmsKeyId"`
	AccountID       string                             `json:"accountId"`
	Region          string                             `json:"region"`
	Version         int                                `json:"version"`
}

// Snapshot serializes current state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "medialive: snapshot table marshal failed", "error", err)

		return nil
	}

	s := backendSnapshot{
		Version:         medialiveSnapshotVersion,
		Tables:          tables,
		Tags:            b.tags,
		ScheduleActions: b.scheduleActions,
		AccountKmsKeyID: b.accountKmsKeyID,
		AccountID:       b.accountID,
		Region:          b.region,
	}

	return persistence.MarshalSnapshot(ctx, "medialive", s)
}

// Restore deserializes state from JSON.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var s backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "medialive", data, &s); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if s.Version != medialiveSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"medialive: discarding incompatible snapshot version, starting empty",
			"gotVersion", s.Version, "wantVersion", medialiveSnapshotVersion)

		b.registry.ResetAll()
		b.tags = make(map[string]map[string]string)
		b.scheduleActions = make(map[string][]*storedScheduleAction)
		b.pendingTransferDeviceIDs = make(map[string]struct{})

		return nil
	}

	if err := b.registry.RestoreAll(s.Tables); err != nil {
		return fmt.Errorf("medialive: restore snapshot tables: %w", err)
	}

	if s.Tags != nil {
		b.tags = s.Tags
	} else {
		b.tags = make(map[string]map[string]string)
	}

	if s.ScheduleActions != nil {
		b.scheduleActions = s.ScheduleActions
	} else {
		b.scheduleActions = make(map[string][]*storedScheduleAction)
	}

	b.rebuildPendingTransferIndex()
	b.accountKmsKeyID = s.AccountKmsKeyID
	b.accountID = s.AccountID
	b.region = s.Region

	return nil
}

// rebuildPendingTransferIndex rebuilds the pendingTransferDeviceIDs set from
// the current inputDevices table. Call after any bulk restore of
// inputDevices.
func (b *InMemoryBackend) rebuildPendingTransferIndex() {
	b.pendingTransferDeviceIDs = make(map[string]struct{})

	for _, d := range b.inputDevices.All() {
		if d.PendingTransfer != nil {
			b.pendingTransferDeviceIDs[d.ID] = struct{}{}
		}
	}
}
