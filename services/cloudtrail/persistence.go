package cloudtrail

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// cloudtrailSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to backendSnapshot (or a value type held
// by one of the registered tables) would make an older snapshot unsafe to
// decode as the current shape. Restore compares this against the persisted
// value and discards (ResetAll, not a partial decode) any mismatch -- see
// Restore. The pre-Phase-3.3 snapshot format had no version field at all, so
// an old snapshot decodes with Version == 0, which is guaranteed to mismatch
// cloudtrailSnapshotVersion and is discarded the same way any other
// incompatible snapshot is.
const cloudtrailSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the CloudTrail backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// b.registry.SnapshotAll() -- every store.Table-backed resource field is a
// "clean" table (see store_setup.go's file doc comment), so no ephemeral DTO
// registry is needed here. Events is the one field left un-converted (it is
// an append-only []Event log, not a map[string]*T). Version guards against
// decoding a snapshot from an incompatible (older or newer) build of this
// backend as though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables           map[string]json.RawMessage `json:"tables"`
	AccountID        string                     `json:"accountID"`
	Region           string                     `json:"region"`
	Events           []Event                    `json:"events,omitempty"`
	Version          int                        `json:"version"`
	ChannelCounter   int                        `json:"channelCounter"`
	DashboardCounter int                        `json:"dashboardCounter"`
	EDSCounter       int                        `json:"edsCounter"`
	QueryCounter     int                        `json:"queryCounter"`
	ImportCounter    int                        `json:"importCounter"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
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
		logger.Load(ctx).WarnContext(ctx, "cloudtrail: snapshot table marshal failed", "error", err)

		return nil
	}

	events := make([]Event, len(b.events))
	copy(events, b.events)

	snap := backendSnapshot{
		Version:          cloudtrailSnapshotVersion,
		Tables:           tables,
		Events:           events,
		AccountID:        b.accountID,
		Region:           b.region,
		ChannelCounter:   b.channelCounter,
		DashboardCounter: b.dashboardCounter,
		EDSCounter:       b.edsCounter,
		QueryCounter:     b.queryCounter,
		ImportCounter:    b.importCounter,
	}

	return persistence.MarshalSnapshot(ctx, "cloudtrail", &snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "cloudtrail", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != cloudtrailSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"cloudtrail: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", cloudtrailSnapshotVersion)

		b.registry.ResetAll()
		b.events = nil
		b.accountID = snap.AccountID
		b.region = snap.Region
		b.channelCounter = 0
		b.dashboardCounter = 0
		b.edsCounter = 0
		b.queryCounter = 0
		b.importCounter = 0

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("cloudtrail: restore snapshot tables: %w", err)
	}

	b.events = snap.Events
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.channelCounter = snap.ChannelCounter
	b.dashboardCounter = snap.DashboardCounter
	b.edsCounter = snap.EDSCounter
	b.queryCounter = snap.QueryCounter
	b.importCounter = snap.ImportCounter

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
