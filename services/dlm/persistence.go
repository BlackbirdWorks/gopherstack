package dlm

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// dlmSnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to backendSnapshot (or a value type held by one of
// the registered tables) would make an older snapshot unsafe to decode as the
// current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll, not a partial decode) any mismatch -- see
// Restore. The pre-Phase-3.3 snapshot format had no version field at all, so
// an old snapshot decodes with Version == 0, which is guaranteed to mismatch
// dlmSnapshotVersion and is discarded the same way any other incompatible
// snapshot is.
const dlmSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the DLM backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// b.registry.SnapshotAll() -- policies is the only store.Table-backed
// resource field (see store_setup.go). Counter is a scalar, not a keyed
// collection, so it is carried alongside Tables rather than through the
// registry. Version guards against decoding a snapshot from an incompatible
// (older or newer) build of this backend as though it were the current
// shape; see Restore.
type backendSnapshot struct {
	Tables  map[string]json.RawMessage `json:"tables"`
	Counter int                        `json:"counter"`
	Version int                        `json:"version"`
}

// Snapshot serializes the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "dlm: snapshot table marshal failed", "error", err)

		return nil
	}

	return persistence.MarshalSnapshot(ctx, "dlm", backendSnapshot{
		Version: dlmSnapshotVersion,
		Tables:  tables,
		Counter: b.counter,
	})
}

// Restore deserializes backend state from a snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "dlm", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != dlmSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"dlm: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", dlmSnapshotVersion)

		b.registry.ResetAll()
		b.counter = 0

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("dlm: restore snapshot tables: %w", err)
	}

	// Counter backward compat: if snapshot's counter is unset, derive it from
	// the restored policy IDs so a restored backend never issues a colliding
	// PolicyID.
	if snap.Counter > 0 {
		b.counter = snap.Counter
	} else {
		b.counter = maxCounterFromPolicies(b.policies.All())
	}

	return nil
}

// maxCounterFromPolicies derives the highest counter value seen in the policy
// ID set. Used for backward compat when restoring snapshots that predate
// counter serialization.
func maxCounterFromPolicies(policies []*storedPolicy) int {
	var highest int

	for _, p := range policies {
		hex := strings.TrimPrefix(p.PolicyID, policyIDPrefix)
		if n, err := strconv.ParseInt(hex, 16, 64); err == nil && n >= 0 && n <= math.MaxInt && int(n) > highest {
			highest = int(n)
		}
	}

	return highest
}

// Snapshot implements persistence.Persistable by delegating to the backend.
//
// DEAD-WIRING FIX: prior to this change, Handler exposed no Snapshot/Restore
// methods even though InMemoryBackend fully implemented both. cli.go's
// setupPersistence type-asserts each registered service.Registerable
// (the *Handler returned by Provider.Init) against a Snapshot/Restore
// interface before registering it with the persistence.Manager -- with no
// such methods on Handler, dlm was silently never registered for
// snapshot-based state save/restore, matching the delegation pattern already
// used by services/dax and services/sesv2.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
