package swf

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// swfSnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to a DTO type, a registered table's value type, or
// backendSnapshot itself would make an older snapshot unsafe to decode as the
// current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll plus the dirty tables'/raw maps' own reset, not
// a partial decode) any mismatch -- see Restore below. This is the first
// version: the pre-Phase-3.3 snapshot carried no version field at all, so it
// also fails this check and is discarded rather than misread, which is the
// safe behaviour across any snapshot-format change.
const swfSnapshotVersion = 1

// activeActivityTaskDTO and activeDecisionTaskDTO wrap the "dirty"
// activeActivityTasks/activeDecisionTasks tables' records alongside the
// taskToken identity those records intentionally exclude from JSON
// (`json:"-"` on TaskToken -- see backend.go) for round-tripping through the
// ephemeral persistence registry below, the same technique services/ses's
// identitySnapshot and services/codeartifact's regionalDTO use.
type activeActivityTaskDTO struct {
	Record    *activeActivityTaskRecord `json:"record"`
	TaskToken string                    `json:"taskToken"`
}

func activeActivityTaskDTOKeyFn(d *activeActivityTaskDTO) string { return d.TaskToken }

type activeDecisionTaskDTO struct {
	Record    *activeDecisionTaskRecord `json:"record"`
	TaskToken string                    `json:"taskToken"`
}

func activeDecisionTaskDTOKeyFn(d *activeDecisionTaskDTO) string { return d.TaskToken }

// persistenceDTOTables groups the two ephemeral DTO tables
// buildPersistenceDTORegistry constructs, so Snapshot/Restore can pass them
// around as one value instead of separate return values.
type persistenceDTOTables struct {
	registry            *store.Registry
	activeActivityTasks *store.Table[activeActivityTaskDTO]
	activeDecisionTasks *store.Table[activeDecisionTaskDTO]
}

// buildPersistenceDTORegistry constructs the ephemeral DTO registry used by
// both Snapshot and Restore for the two "dirty" tables (activeActivityTasks,
// activeDecisionTasks -- see store_setup.go's registerAllTables doc). It is
// built fresh on every call (rather than reusing b.registry) because the DTO
// value types differ from the live table value types.
func buildPersistenceDTORegistry() persistenceDTOTables {
	dtoReg := store.NewRegistry()

	return persistenceDTOTables{
		registry: dtoReg,
		activeActivityTasks: store.Register(
			dtoReg, "activeActivityTasks", store.New(activeActivityTaskDTOKeyFn),
		),
		activeDecisionTasks: store.Register(
			dtoReg, "activeDecisionTasks", store.New(activeDecisionTaskDTOKeyFn),
		),
	}
}

// backendSnapshot is the top-level on-disk shape for the SWF backend.
//
// Tables holds one JSON-encoded array per registered table name: the four
// "clean" tables on b.registry (domains, workflows, activities, executions)
// plus the two DTO tables built above for the "dirty" activeActivityTasks/
// activeDecisionTasks. History, ActivityQueues, and DecisionQueues stay plain
// maps by design (ORDER-SENSITIVE event histories and FIFO task queues --
// see the InMemoryBackend doc comment in backend.go); only History and Tags
// were ever part of the pre-Phase-3.3 snapshot, and that persisted/ephemeral
// split is preserved unchanged here: ActivityQueues/DecisionQueues remain
// ephemeral (never written to or read from a snapshot).
type backendSnapshot struct {
	Tables         map[string]json.RawMessage   `json:"tables"`
	History        map[string][]HistoryEvent    `json:"history,omitempty"`
	Tags           map[string]map[string]string `json:"tags,omitempty"`
	ExecutionOrder []string                     `json:"executionOrder"`
	Version        int                          `json:"version"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "swf: snapshot table marshal failed", "error", err)

		return nil
	}

	dtos := buildPersistenceDTORegistry()

	for _, v := range b.activeActivityTasks.Snapshot() {
		dtos.activeActivityTasks.Put(&activeActivityTaskDTO{Record: v, TaskToken: v.TaskToken})
	}

	for _, v := range b.activeDecisionTasks.Snapshot() {
		dtos.activeDecisionTasks.Put(&activeDecisionTaskDTO{Record: v, TaskToken: v.TaskToken})
	}

	dtoTables, err := dtos.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "swf: snapshot DTO table marshal failed", "error", err)

		return nil
	}
	maps.Copy(tables, dtoTables)

	history := make(map[string][]HistoryEvent, len(b.history))
	for k, v := range b.history {
		cp := make([]HistoryEvent, len(v))
		copy(cp, v)
		history[k] = cp
	}

	tags := make(map[string]map[string]string, len(b.tags))
	for k, v := range b.tags {
		cp := make(map[string]string, len(v))
		maps.Copy(cp, v)
		tags[k] = cp
	}

	order := make([]string, len(b.executionOrder))
	copy(order, b.executionOrder)

	snap := backendSnapshot{
		Version:        swfSnapshotVersion,
		Tables:         tables,
		History:        history,
		Tags:           tags,
		ExecutionOrder: order,
	}

	return persistence.MarshalSnapshot(ctx, "swf", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "swf", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != swfSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"swf: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", swfSnapshotVersion)

		b.registry.ResetAll()
		b.activeActivityTasks.Reset()
		b.activeDecisionTasks.Reset()
		b.history = make(map[string][]HistoryEvent)
		b.activityQueues = make(map[string][]*ActivityTask)
		b.decisionQueues = make(map[string][]*DecisionTask)
		b.tags = make(map[string]map[string]string)
		b.executionOrder = nil

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("swf: restore snapshot tables: %w", err)
	}

	if err := b.restoreDirtyTablesLocked(snap.Tables); err != nil {
		return err
	}

	if snap.History == nil {
		snap.History = make(map[string][]HistoryEvent)
	}
	b.history = snap.History

	if snap.Tags == nil {
		snap.Tags = make(map[string]map[string]string)
	}
	b.tags = snap.Tags

	b.executionOrder = snap.ExecutionOrder

	return nil
}

// restoreDirtyTablesLocked rebuilds the two "dirty" tables (activeActivityTasks,
// activeDecisionTasks; see store_setup.go's registerAllTables doc) from
// tables via the ephemeral DTO registry, factored out of Restore to keep
// Restore's own cognitive complexity low. Callers must hold b.mu.Lock.
//
// activityQueues and decisionQueues are deliberately left untouched here,
// exactly as the pre-Phase-3.3 Restore did: neither was ever part of
// backendSnapshot, so a normal (version-matched) Restore call has never reset
// or otherwise touched them -- only the version-mismatch branch above resets
// every raw map, including these two, to a clean slate.
func (b *InMemoryBackend) restoreDirtyTablesLocked(tables map[string]json.RawMessage) error {
	dtos := buildPersistenceDTORegistry()
	if err := dtos.registry.RestoreAll(tables); err != nil {
		return fmt.Errorf("swf: restore snapshot DTO tables: %w", err)
	}

	liveActivityTasks := make([]*activeActivityTaskRecord, 0, dtos.activeActivityTasks.Len())

	for _, dto := range dtos.activeActivityTasks.All() {
		if dto.Record == nil {
			continue
		}
		dto.Record.TaskToken = dto.TaskToken
		liveActivityTasks = append(liveActivityTasks, dto.Record)
	}

	b.activeActivityTasks.Restore(liveActivityTasks)

	liveDecisionTasks := make([]*activeDecisionTaskRecord, 0, dtos.activeDecisionTasks.Len())

	for _, dto := range dtos.activeDecisionTasks.All() {
		if dto.Record == nil {
			continue
		}
		dto.Record.TaskToken = dto.TaskToken
		liveDecisionTasks = append(liveDecisionTasks, dto.Record)
	}

	b.activeDecisionTasks.Restore(liveDecisionTasks)

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
