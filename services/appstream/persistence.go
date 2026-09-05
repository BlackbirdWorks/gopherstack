package appstream

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// appstreamSnapshotVersion identifies the shape of [backendSnapshot]. It must
// be bumped whenever a change to backendSnapshot (or a value type held by one
// of the registered tables) would make an older snapshot unsafe to decode as
// the current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll plus resetting every raw map, not a partial
// decode) any mismatch -- see Restore below. The pre-Phase-3.3 snapshot
// format had no version field at all, so an old snapshot decodes with
// Version == 0, which is guaranteed to mismatch appstreamSnapshotVersion and
// is discarded the same way any other incompatible snapshot is.
const appstreamSnapshotVersion = 2

// backendSnapshot is the top-level on-disk shape for the AppStream backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// b.registry.SnapshotAll() -- every store.Table-backed resource field is a
// "clean" table (see store_setup.go's file doc comment), so no ephemeral DTO
// registry is needed here.
//
// Associations, Tags, AppBlockBuilderAssoc, AppFleetAssoc, EntitlementApps,
// SoftwareAssoc, and UserStackAssoc are the seven plain maps left
// unconverted by Phase 3.3 (see the field comments on InMemoryBackend in
// store.go and store_setup.go's doc comment): each holds a value with no
// identity of its own (a many-to-many association set, or a tag map keyed by
// an external ARN), so there is nothing for store.Table to key on, and each
// is persisted here directly, matching the pre-conversion behavior.
// UsageReport is a single scalar record (at most one usage-report
// subscription exists at a time), so it is also persisted directly rather
// than through a table.
type backendSnapshot struct {
	Tables               map[string]json.RawMessage     `json:"tables"`
	Associations         map[string]map[string]bool     `json:"associations"`
	Tags                 map[string]map[string]string   `json:"tags"`
	AppBlockBuilderAssoc map[string]map[string]bool     `json:"appBlockBuilderAssoc"`
	AppFleetAssoc        map[string]map[string]bool     `json:"appFleetAssoc"`
	EntitlementApps      map[string]map[string]bool     `json:"entitlementApps"`
	SoftwareAssoc        map[string]map[string]bool     `json:"softwareAssoc"`
	UserStackAssoc       map[string]map[string]bool     `json:"userStackAssoc"`
	UsageReport          *storedUsageReportSubscription `json:"usageReport"`
	SessionSeq           int                            `json:"sessionSeq"`
	ExportTaskSeq        int                            `json:"exportTaskSeq"`
	Version              int                            `json:"version"`
}

// Snapshot serializes backend state to JSON. It implements
// persistence.Persistable.
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
		logger.Load(ctx).WarnContext(ctx, "appstream: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:              appstreamSnapshotVersion,
		Tables:               tables,
		Associations:         b.associations,
		Tags:                 b.tags,
		AppBlockBuilderAssoc: b.appBlockBuilderAssoc,
		AppFleetAssoc:        b.appFleetAssoc,
		EntitlementApps:      b.entitlementApps,
		SoftwareAssoc:        b.softwareAssoc,
		UserStackAssoc:       b.userStackAssoc,
		UsageReport:          b.usageReport,
		SessionSeq:           b.sessionSeq,
		ExportTaskSeq:        b.exportTaskSeq,
	}

	return persistence.MarshalSnapshot(ctx, "appstream", snap)
}

// Restore deserializes backend state from a snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "appstream", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != appstreamSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"appstream: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", appstreamSnapshotVersion)

		b.registry.ResetAll()
		b.resetRawMaps()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("appstream: restore snapshot tables: %w", err)
	}

	b.restoreRawMaps(&snap)

	return nil
}

// resetRawMaps clears every plain (non-store.Table) persisted field to its
// empty state. Shared by Reset (store.go) and Restore's version-mismatch
// path. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) resetRawMaps() {
	b.associations = make(map[string]map[string]bool)
	b.tags = make(map[string]map[string]string)
	b.appBlockBuilderAssoc = make(map[string]map[string]bool)
	b.appFleetAssoc = make(map[string]map[string]bool)
	b.entitlementApps = make(map[string]map[string]bool)
	b.softwareAssoc = make(map[string]map[string]bool)
	b.userStackAssoc = make(map[string]map[string]bool)
	b.usageReport = nil
	b.sessionSeq = 0
	b.exportTaskSeq = 0
}

// restoreRawMaps restores every plain (non-store.Table) persisted map from
// snap, defaulting each to an empty map when absent from the snapshot (e.g.
// an older snapshot predating a field, though appstreamSnapshotVersion's own
// guard makes that unreachable today). Factored out of Restore to keep
// Restore's own cognitive complexity low. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restoreRawMaps(snap *backendSnapshot) {
	b.associations = snap.Associations
	if b.associations == nil {
		b.associations = make(map[string]map[string]bool)
	}

	b.tags = snap.Tags
	if b.tags == nil {
		b.tags = make(map[string]map[string]string)
	}

	b.appBlockBuilderAssoc = snap.AppBlockBuilderAssoc
	if b.appBlockBuilderAssoc == nil {
		b.appBlockBuilderAssoc = make(map[string]map[string]bool)
	}

	b.appFleetAssoc = snap.AppFleetAssoc
	if b.appFleetAssoc == nil {
		b.appFleetAssoc = make(map[string]map[string]bool)
	}

	b.entitlementApps = snap.EntitlementApps
	if b.entitlementApps == nil {
		b.entitlementApps = make(map[string]map[string]bool)
	}

	b.softwareAssoc = snap.SoftwareAssoc
	if b.softwareAssoc == nil {
		b.softwareAssoc = make(map[string]map[string]bool)
	}

	b.userStackAssoc = snap.UserStackAssoc
	if b.userStackAssoc == nil {
		b.userStackAssoc = make(map[string]map[string]bool)
	}

	b.usageReport = snap.UsageReport
	b.sessionSeq = snap.SessionSeq
	b.exportTaskSeq = snap.ExportTaskSeq
}
