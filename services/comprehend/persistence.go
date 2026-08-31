package comprehend

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// comprehendSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to a registered table's value type or
// backendSnapshot itself would make an older snapshot unsafe to decode as
// the current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll plus resetting the raw maps, not a partial
// decode) any mismatch -- see Restore below. This is the first version:
// Comprehend had no persistence at all before Phase 3.3 (Handler had no
// Snapshot/Restore, and neither did InMemoryBackend, so cli.go's generic
// setupPersistence never picked Comprehend up -- see the Handler.Snapshot/
// Restore delegation at the bottom of this file), so there is no legacy
// snapshot shape to be compatible with -- any snapshot without a matching
// Version (including one with no version field, which decodes as 0) is
// discarded the same way any other incompatible snapshot is.
// Version 2 added PolicyCreatedAt/PolicyModifiedAt (gopherstack-6flj/21my:
// DescribeResourcePolicy previously stamped both timestamps with time.Now()
// on every call instead of tracking real per-policy state).
const comprehendSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the Comprehend backend.
//
// Tables holds one JSON-encoded array per registered table name (jobs,
// resources, iterations -- see store_setup.go), produced directly by
// b.registry.SnapshotAll(): all three derive their store.Table key from a
// real, already-exported field on the value type, so none of them need a DTO
// wrapper.
//
// Tags, Policies, and PolicyRevisions are the remaining raw (non-store.Table)
// maps: their values are plain strings / string maps, not *T, so there is
// nothing for store.Table to key on (see store_setup.go's file doc comment).
// They are persisted directly here.
type backendSnapshot struct {
	Tables           map[string]json.RawMessage   `json:"tables"`
	Tags             map[string]map[string]string `json:"tags"`
	Policies         map[string]string            `json:"policies"`
	PolicyRevisions  map[string]string            `json:"policyRevisions"`
	PolicyCreatedAt  map[string]time.Time         `json:"policyCreatedAt"`
	PolicyModifiedAt map[string]time.Time         `json:"policyModifiedAt"`
	AccountID        string                       `json:"accountID"`
	Region           string                       `json:"region"`
	Version          int                          `json:"version"`
}

// Snapshot serializes the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "comprehend: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:          comprehendSnapshotVersion,
		Tables:           tables,
		Tags:             b.tags,
		Policies:         b.policies,
		PolicyRevisions:  b.policyRevisions,
		PolicyCreatedAt:  b.policyCreatedAt,
		PolicyModifiedAt: b.policyModifiedAt,
		AccountID:        b.accountID,
		Region:           b.region,
	}

	return persistence.MarshalSnapshot(ctx, "comprehend", snap)
}

// Restore deserializes backend state from a snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "comprehend", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != comprehendSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"comprehend: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", comprehendSnapshotVersion)

		b.registry.ResetAll()
		b.tags = make(map[string]map[string]string)
		b.policies = make(map[string]string)
		b.policyRevisions = make(map[string]string)
		b.policyCreatedAt = make(map[string]time.Time)
		b.policyModifiedAt = make(map[string]time.Time)
		b.accountID = snap.AccountID
		b.region = snap.Region

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("comprehend: restore snapshot tables: %w", err)
	}

	if snap.Tags == nil {
		snap.Tags = make(map[string]map[string]string)
	}
	if snap.Policies == nil {
		snap.Policies = make(map[string]string)
	}
	if snap.PolicyRevisions == nil {
		snap.PolicyRevisions = make(map[string]string)
	}
	if snap.PolicyCreatedAt == nil {
		snap.PolicyCreatedAt = make(map[string]time.Time)
	}
	if snap.PolicyModifiedAt == nil {
		snap.PolicyModifiedAt = make(map[string]time.Time)
	}

	b.tags = snap.Tags
	b.policies = snap.Policies
	b.policyRevisions = snap.PolicyRevisions
	b.policyCreatedAt = snap.PolicyCreatedAt
	b.policyModifiedAt = snap.PolicyModifiedAt
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
// Handler previously had no Snapshot/Restore of its own -- and neither did
// InMemoryBackend -- so cli.go's generic setupPersistence (which
// type-asserts the registered service.Registerable, i.e. the Handler, for a
// Snapshot/Restore pair) never picked Comprehend up at all: dead wiring, with
// no persistence underneath it either. This delegation (matching the
// codecommit/cleanrooms pattern) is what wires Comprehend into persistence
// for the first time.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
