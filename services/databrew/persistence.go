package databrew

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// databrewSnapshotVersion identifies the shape of backendSnapshot's Tables
// blob (i.e. the set of resource tables registered on b.registry -- see
// store_setup.go) plus JobRuns. It must be bumped whenever a change there
// would make an older snapshot unsafe to decode as the current shape.
// DataBrew had no persistence at all before Phase 3.3 (Handler had no
// Snapshot/Restore, and neither did InMemoryBackend, so cli.go's generic
// setupPersistence never picked it up -- dead wiring with nothing underneath
// it either; see the Handler.Snapshot/Restore delegation at the bottom of
// this file and the StorageBackend doc comment in interfaces.go), so there is
// no legacy snapshot shape to be compatible with -- any snapshot without a
// matching Version (including one with no version field, which decodes as 0)
// is discarded the same way any other incompatible snapshot is. This mirrors
// the services/pipes, services/eventbridge, and services/cleanrooms/
// services/codecommit ("no persistence before") conversions.
const databrewSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the DataBrew backend.
//
// Tables holds one JSON-encoded, key-sorted array per registered
// *store.Table[V] on b.registry, produced by
// [github.com/blackbirdworks/gopherstack/pkgs/store.Registry.SnapshotAll].
// Each per-region table is registered under "<resourceName>/<region>"
// (datasets, recipes, projects, jobs, rulesets, schedules) -- see
// store_setup.go's *Table accessors. Restore must pre-register every
// (resource, region) tuple found in Tables before calling
// registry.RestoreAll, see preRegisterSnapshotTables below.
//
// JobRuns is the one remaining raw (non-store.Table) map: each job's run
// history is an order-sensitive slice that store.Table/store.Index cannot
// represent -- see store_setup.go's package doc -- so it is persisted
// directly here, nested the same way it is held on InMemoryBackend
// (region -> job name -> run history).
type backendSnapshot struct {
	Tables    map[string]json.RawMessage      `json:"tables"`
	JobRuns   map[string]map[string][]*JobRun `json:"jobRuns"`
	AccountID string                          `json:"accountID"`
	Region    string                          `json:"region"`
	Version   int                             `json:"version"`
}

// Snapshot serialises the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		// The registered tables are plain JSON-friendly structs, so a marshal
		// failure here would indicate a programming error rather than bad
		// input data. Log and skip the snapshot rather than panic, matching
		// the persistence.Persistable contract (nil is skipped by the Manager).
		logger.Load(ctx).WarnContext(ctx, "databrew: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:   databrewSnapshotVersion,
		Tables:    tables,
		JobRuns:   b.jobRuns,
		AccountID: b.accountID,
		Region:    b.defaultRegion,
	}

	return persistence.MarshalSnapshot(ctx, "databrew", snap)
}

// preRegisterSnapshotTables ensures every (resource, region) pair present in
// tables is registered on b.registry before registry.RestoreAll is called.
// RestoreAll only restores tables already registered (see
// [github.com/blackbirdworks/gopherstack/pkgs/store.Registry.RestoreAll]); a
// region a fresh backend has never touched would otherwise not yet have a
// table registered under that name, and its persisted data would be silently
// dropped instead of restored. Each table name has the shape
// "<resourceName>/<region>" (see store_setup.go's *Table accessors);
// splitting on the first "/" recovers both parts (region names never
// contain "/").
func (b *InMemoryBackend) preRegisterSnapshotTables(tables map[string]json.RawMessage) {
	for key := range tables {
		name, region, found := strings.Cut(key, "/")
		if !found {
			continue
		}

		switch name {
		case "datasets":
			b.datasetsTable(region)
		case "recipes":
			b.recipesTable(region)
		case "projects":
			b.projectsTable(region)
		case "jobs":
			b.jobsTable(region)
		case "rulesets":
			b.rulesetsTable(region)
		case "schedules":
			b.schedulesTable(region)
		}
	}
}

// Restore loads backend state from a JSON snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "databrew", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != databrewSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"databrew: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", databrewSnapshotVersion)

		b.registry.ResetAll()
		b.jobRuns = make(map[string]map[string][]*JobRun)

		return nil
	}

	if snap.Tables == nil {
		snap.Tables = make(map[string]json.RawMessage)
	}

	b.preRegisterSnapshotTables(snap.Tables)

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("databrew: restore snapshot tables: %w", err)
	}

	b.jobRuns = snap.JobRuns
	if b.jobRuns == nil {
		b.jobRuns = make(map[string]map[string][]*JobRun)
	}

	b.accountID = snap.AccountID
	b.defaultRegion = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
// Handler previously had no Snapshot/Restore of its own -- and neither did
// InMemoryBackend -- so cli.go's generic setupPersistence (which
// type-asserts the registered service.Registerable, i.e. the Handler, for a
// Snapshot/Restore pair) never picked DataBrew up at all: dead wiring, with
// no persistence underneath it either. This delegation (matching the
// codecommit/codepipeline/cleanrooms pattern) is what wires DataBrew into
// persistence for the first time.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
