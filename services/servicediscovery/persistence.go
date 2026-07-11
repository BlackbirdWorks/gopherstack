package servicediscovery

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// servicediscoverySnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to backendSnapshot (or a value type held by
// one of the registered tables) would make an older snapshot unsafe to decode
// as the current shape. Restore compares this against the persisted value and
// discards (ResetAll, not a partial decode) any mismatch -- see Restore. The
// pre-Phase-3.3 snapshot format had no version field at all, so an old
// snapshot decodes with Version == 0, which is guaranteed to mismatch
// servicediscoverySnapshotVersion and is discarded the same way any other
// incompatible snapshot is.
const servicediscoverySnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the ServiceDiscovery
// backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// b.registry.SnapshotAll() -- namespaces/services/instances/operations are
// all "clean" store.Table-backed resources (see store_setup.go's file doc
// comment), so no ephemeral DTO registry is needed here. ServiceAttributes
// and InstanceHealthStatuses are the two maps left un-converted (their values
// are plain string maps/strings, not *T). Version guards against decoding a
// snapshot from an incompatible (older or newer) build of this backend as
// though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables                 map[string]json.RawMessage   `json:"tables"`
	ServiceAttributes      map[string]map[string]string `json:"serviceAttributes"`
	InstanceHealthStatuses map[string]string            `json:"instanceHealthStatuses"`
	AccountID              string                       `json:"accountID"`
	Region                 string                       `json:"region"`
	InstanceRevision       int64                        `json:"instanceRevision"`
	NsCounter              int                          `json:"nsCounter"`
	SvcCounter             int                          `json:"svcCounter"`
	OpCounter              int                          `json:"opCounter"`
	Version                int                          `json:"version"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		// The registered tables are all plain JSON-friendly structs, so a
		// marshal failure here would indicate a programming error rather
		// than bad input data. Log and skip the snapshot rather than panic,
		// matching the persistence.Persistable contract (nil is skipped by
		// the Manager).
		logger.Load(ctx).WarnContext(ctx, "servicediscovery: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:                servicediscoverySnapshotVersion,
		Tables:                 tables,
		ServiceAttributes:      b.serviceAttributes,
		InstanceHealthStatuses: b.instanceHealthStatuses,
		AccountID:              b.accountID,
		Region:                 b.region,
		InstanceRevision:       b.instanceRevision,
		NsCounter:              b.nsCounter,
		SvcCounter:             b.svcCounter,
		OpCounter:              b.opCounter,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "servicediscovery: snapshot marshal failed", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "servicediscovery", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != servicediscoverySnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"servicediscovery: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", servicediscoverySnapshotVersion)

		b.registry.ResetAll()
		b.serviceAttributes = make(map[string]map[string]string)
		b.instanceHealthStatuses = make(map[string]string)
		b.accountID = snap.AccountID
		b.region = snap.Region
		b.instanceRevision = 0
		b.nsCounter = 0
		b.svcCounter = 0
		b.opCounter = 0

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("servicediscovery: restore snapshot tables: %w", err)
	}

	if snap.ServiceAttributes == nil {
		snap.ServiceAttributes = make(map[string]map[string]string)
	}

	if snap.InstanceHealthStatuses == nil {
		snap.InstanceHealthStatuses = make(map[string]string)
	}

	b.serviceAttributes = snap.ServiceAttributes
	b.instanceHealthStatuses = snap.InstanceHealthStatuses
	b.instanceRevision = snap.InstanceRevision
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.nsCounter = snap.NsCounter
	b.svcCounter = snap.SvcCounter
	b.opCounter = snap.OpCounter

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
