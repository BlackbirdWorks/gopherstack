package stepfunctions

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// sfnSnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to executionSnapshot or backendSnapshot itself
// would make an older snapshot unsafe to decode as the current shape (e.g. a
// field's meaning or type changes). Restore compares this against the
// persisted value and discards (rather than attempts to partially decode)
// any mismatch -- see Restore below.
//
// Phase 3.3 introduced this version: the pre-Phase-3.3 shape had no version
// field at all (backendSnapshot was a plain map[string]*T per resource type),
// so any snapshot taken before this change is unconditionally incompatible
// and falls into the "discard, start empty" path.
const sfnSnapshotVersion = 1

// executionSnapshot is the on-disk DTO for Execution. It exists as a separate
// type (rather than JSON tags directly on Execution) solely to expose the
// unexported history field: Execution.history is deliberately unexported
// because *Execution is returned directly as the wire body for
// DescribeExecution/StartExecution/etc., and AWS's real DescribeExecution
// response has no "history" field -- history is retrieved only via
// GetExecutionHistory. Being unexported also means encoding/json (including
// the marshal [store.Table.Snapshot] would perform) skips it entirely; this
// DTO adds it back as an ordinary exported field purely for the on-disk
// snapshot round trip.
type executionSnapshot struct {
	RedriveDate            *float64        `json:"redriveDate,omitempty"`
	StopDate               *float64        `json:"stopDate,omitempty"`
	Status                 string          `json:"status"`
	ExecutionArn           string          `json:"executionArn"`
	StateMachineArn        string          `json:"stateMachineArn"`
	StateMachineVersionArn string          `json:"stateMachineVersionArn,omitempty"`
	StateMachineAliasArn   string          `json:"stateMachineAliasArn,omitempty"`
	Name                   string          `json:"name"`
	Input                  string          `json:"input,omitempty"`
	Output                 string          `json:"output,omitempty"`
	Error                  string          `json:"error,omitempty"`
	Cause                  string          `json:"cause,omitempty"`
	History                []*HistoryEvent `json:"history,omitempty"`
	StartDate              float64         `json:"startDate"`
	RedriveCount           int             `json:"redriveCount,omitempty"`
}

func executionSnapshotKey(v *executionSnapshot) string { return v.ExecutionArn }

// newPersistedDTORegistry builds the ephemeral registry covering exactly the
// three tables backendSnapshot has ever persisted: stateMachines, activities,
// and executions. This matches pre-Phase-3.3 behavior, where
// versions/aliases/mapRuns (and their smVersions/smAliases/execMapRuns index
// maps) were never part of backendSnapshot either -- see the Phase 3.3
// tracking issue for the per-map persistence audit.
//
// stateMachines and activities are registered by reusing the live
// *store.Table pointers directly: their on-disk shape is identical to their
// live shape (same struct, same JSON tags), so RestoreAll on this registry
// restores straight into live state and SnapshotAll reads straight from it.
// Only executions needs a distinct DTO type, for the inline-history reason
// documented on [executionSnapshot]. This is the same DTO-registry pattern
// services/sqs's persistence.go (commit 0f09d77c) and
// services/cloudwatchlogs's persistence.go use.
func (b *InMemoryBackend) newPersistedDTORegistry() (*store.Registry, *store.Table[executionSnapshot]) {
	dtoReg := store.NewRegistry()
	store.Register(dtoReg, "stateMachines", b.stateMachines)
	store.Register(dtoReg, "activities", b.activities)
	execDTOs := store.Register(dtoReg, "executions", store.New(executionSnapshotKey))

	return dtoReg, execDTOs
}

// backendSnapshot is the top-level on-disk shape for the stepfunctions backend.
//
// Tables holds one JSON-encoded array per registered DTO table, produced by
// [store.Registry.SnapshotAll] -- "stateMachines", "activities", and
// "executions" (see [InMemoryBackend.newPersistedDTORegistry]). Version
// guards against decoding a snapshot from an incompatible (older or newer)
// build of this backend as though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage `json:"tables"`
	AccountID string                     `json:"accountID"`
	Region    string                     `json:"region"`
	Version   int                        `json:"version"`
}

// handlerSnapshot wraps the backend snapshot together with handler-level state
// (tags) so both are persisted and restored together.
type handlerSnapshot struct {
	Tags    map[string]map[string]string `json:"tags,omitempty"`
	Backend json.RawMessage              `json:"backend"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
// Any execution still RUNNING at snapshot time is promoted to TIMED_OUT so that
// Restore() never encounters non-terminal executions without a running goroutine.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	dtoReg, execDTOs := b.newPersistedDTORegistry()

	// exec.history is written by appendHistory under only b.mu.RLock +
	// b.historyMu.Lock (a deliberate hot-path optimization -- see
	// appendHistory in backend.go), so the whole-struct copy below, which
	// touches every field including history, must also take historyMu:
	// b.mu's RLock held above does not by itself exclude that write.
	b.historyMu.RLock()
	defer b.historyMu.RUnlock()

	for _, exec := range b.executions.Snapshot() {
		cp := *exec
		if cp.Status == statusRunning {
			cp.Status = "TIMED_OUT"
		}

		execDTOs.Put(&executionSnapshot{
			StopDate:               cp.StopDate,
			RedriveDate:            cp.RedriveDate,
			History:                cp.history,
			ExecutionArn:           cp.ExecutionArn,
			StateMachineArn:        cp.StateMachineArn,
			StateMachineVersionArn: cp.StateMachineVersionArn,
			StateMachineAliasArn:   cp.StateMachineAliasArn,
			Name:                   cp.Name,
			Status:                 cp.Status,
			Input:                  cp.Input,
			Output:                 cp.Output,
			Error:                  cp.Error,
			Cause:                  cp.Cause,
			StartDate:              cp.StartDate,
			RedriveCount:           cp.RedriveCount,
		})
	}

	tables, err := dtoReg.SnapshotAll()
	if err != nil {
		// The DTOs above are plain JSON-friendly structs, so a marshal failure
		// here would indicate a programming error rather than bad input data.
		// Log and skip the snapshot rather than panic, matching the
		// persistence.Persistable contract (nil is skipped by the Manager).
		logger.Load(ctx).WarnContext(ctx,
			"stepfunctions: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:   sfnSnapshotVersion,
		Tables:    tables,
		AccountID: b.accountID,
		Region:    b.region,
	}

	return persistence.MarshalSnapshot(ctx, "stepfunctions", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
// Service integrations (Lambda, SQS, SNS, DynamoDB) are not restored — they are re-wired by the CLI.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "stepfunctions", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != sfnSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"stepfunctions: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", sfnSnapshotVersion)

		b.resetLocked()

		return nil
	}

	dtoReg, execDTOs := b.newPersistedDTORegistry()

	if err := dtoReg.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("stepfunctions: restore snapshot tables: %w", err)
	}

	// RestoreAll above already restored stateMachines and activities straight
	// into b.stateMachines/b.activities (they were registered by reusing the
	// live table pointers). Only executions needs rebuilding from its DTO
	// shape back into live *Execution values with the inline history restored.
	liveExecs := make([]*Execution, 0, execDTOs.Len())

	for _, dto := range execDTOs.All() {
		liveExecs = append(liveExecs, &Execution{
			StopDate:               dto.StopDate,
			RedriveDate:            dto.RedriveDate,
			history:                dto.History,
			ExecutionArn:           dto.ExecutionArn,
			StateMachineArn:        dto.StateMachineArn,
			StateMachineVersionArn: dto.StateMachineVersionArn,
			StateMachineAliasArn:   dto.StateMachineAliasArn,
			Name:                   dto.Name,
			Status:                 dto.Status,
			Input:                  dto.Input,
			Output:                 dto.Output,
			Error:                  dto.Error,
			Cause:                  dto.Cause,
			StartDate:              dto.StartDate,
			RedriveCount:           dto.RedriveCount,
		})
	}

	// Restore rebuilds executionsByStateMachine too (store.Table.Restore
	// maintains every registered store.Index from scratch).
	b.executions.Restore(liveExecs)

	b.accountID = snap.AccountID
	b.region = snap.Region

	// Rebuild secondary indexes derived from restored state. These are plain
	// maps (see backend.go), not store.Index, so Table.Restore does not
	// maintain them automatically.
	b.nameIndex = make(map[string]map[string]string)

	for _, sm := range b.stateMachines.All() {
		region := regionFromARN(sm.StateMachineArn, b.region)
		if b.nameIndex[region] == nil {
			b.nameIndex[region] = make(map[string]string)
		}

		b.nameIndex[region][sm.Name] = sm.StateMachineArn
	}

	b.smExecsByStatus = make(map[string]map[string][]string)

	for _, exec := range b.executions.All() {
		smARN := exec.StateMachineArn
		if b.smExecsByStatus[smARN] == nil {
			b.smExecsByStatus[smARN] = make(map[string][]string)
		}

		b.smExecsByStatus[smARN][exec.Status] = append(b.smExecsByStatus[smARN][exec.Status], exec.ExecutionArn)
	}

	// Rebuild activity name index and create empty queues (pending tasks are not persisted).
	b.activityNameIndex = make(map[string]map[string]string)
	b.pendingTaskQueues = make(map[string]chan *activityTaskEntry, b.activities.Len())

	for _, a := range b.activities.All() {
		actRegion := regionFromARN(a.ActivityArn, b.region)
		if b.activityNameIndex[actRegion] == nil {
			b.activityNameIndex[actRegion] = make(map[string]string)
		}

		b.activityNameIndex[actRegion][a.Name] = a.ActivityArn
		b.pendingTaskQueues[a.ActivityArn] = make(chan *activityTaskEntry, maxPendingActivityTasks)
	}

	b.tasksByToken = make(map[string]*activityTaskEntry)

	// cancelFns is intentionally empty after restore. Snapshot() converts any
	// RUNNING execution to TIMED_OUT before serialization, so all restored
	// executions are in terminal states and no goroutines need to be tracked.
	b.cancelFns = make(map[string]context.CancelFunc)
	b.deletedExecs = make(map[string]bool)
	b.historyTruncated = make(map[string]bool)

	// versions/aliases/mapRuns (and smAliases/executionDefinitions) are left
	// untouched here, matching pre-Phase-3.3 Restore -- backendSnapshot has
	// never included those fields, so a fresh backend simply keeps them empty
	// as constructed. See the Phase 3.3 tracking issue's per-map persistence
	// audit.

	return nil
}

// Snapshot implements persistence.Persistable.
// It serialises both the backend state and the handler's tag map.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	type snapshotter interface {
		Snapshot(ctx context.Context) []byte
	}

	var backendBytes json.RawMessage
	if s, ok := h.Backend.(snapshotter); ok {
		backendBytes = s.Snapshot(ctx)
	}

	h.tagsMu.RLock("Handler.Snapshot")
	tagsData := make(map[string]map[string]string, len(h.tags))
	for resourceID, t := range h.tags {
		tagsData[resourceID] = t.Clone()
	}
	h.tagsMu.RUnlock()

	snap := handlerSnapshot{
		Backend: backendBytes,
		Tags:    tagsData,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return backendBytes
	}

	return data
}

// Restore implements persistence.Persistable.
// It restores both the backend state and the handler's tag map.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	var snap handlerSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "stepfunctions", data, &snap); err != nil {
		return err
	}

	// If the new wrapped format has a backend field use it;
	// otherwise treat the raw data as a legacy backend-only snapshot.
	backendData := snap.Backend
	if len(backendData) == 0 {
		backendData = data
	}

	type restorer interface {
		Restore(context.Context, []byte) error
	}
	if r, ok := h.Backend.(restorer); ok {
		if err := r.Restore(ctx, backendData); err != nil {
			return err
		}
	}

	if len(snap.Tags) > 0 {
		h.tagsMu.Lock("Handler.Restore")
		for _, t := range h.tags {
			t.Close()
		}

		h.tags = make(map[string]*tags.Tags, len(snap.Tags))
		for resourceID, tagMap := range snap.Tags {
			h.tags[resourceID] = tags.FromMap("sfn."+resourceID+".tags", tagMap)
		}
		h.tagsMu.Unlock()
	}

	return nil
}
