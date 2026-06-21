package stepfunctions

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type backendSnapshot struct {
	StateMachines map[string]*StateMachine   `json:"stateMachines"`
	Executions    map[string]*Execution      `json:"executions"`
	History       map[string][]*HistoryEvent `json:"history"`
	Activities    map[string]*Activity       `json:"activities,omitempty"`
	AccountID     string                     `json:"accountID"`
	Region        string                     `json:"region"`
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

	// Build a sanitised copy of executions: RUNNING → TIMED_OUT.
	execsCopy := make(map[string]*Execution, len(b.executions))
	for k, exec := range b.executions {
		cp := *exec
		if cp.Status == statusRunning {
			cp.Status = "TIMED_OUT"
		}

		execsCopy[k] = &cp
	}

	snap := backendSnapshot{
		StateMachines: b.stateMachines,
		Executions:    execsCopy,
		History:       b.history,
		Activities:    b.activities,
		AccountID:     b.accountID,
		Region:        b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(b.svcCtx).Warn("persistence: snapshot marshal failed", "service", "stepfunctions", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
// Service integrations (Lambda, SQS, SNS, DynamoDB) are not restored — they are re-wired by the CLI.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.StateMachines == nil {
		snap.StateMachines = make(map[string]*StateMachine)
	}

	if snap.Executions == nil {
		snap.Executions = make(map[string]*Execution)
	}

	if snap.History == nil {
		snap.History = make(map[string][]*HistoryEvent)
	}

	if snap.Activities == nil {
		snap.Activities = make(map[string]*Activity)
	}

	b.stateMachines = snap.StateMachines
	b.executions = snap.Executions
	b.history = snap.History
	b.activities = snap.Activities
	b.accountID = snap.AccountID
	b.region = snap.Region

	// Rebuild secondary indexes from the restored state.
	b.nameIndex = make(map[string]map[string]string)
	for smARN, sm := range b.stateMachines {
		region := regionFromARN(smARN, b.region)
		if b.nameIndex[region] == nil {
			b.nameIndex[region] = make(map[string]string)
		}

		b.nameIndex[region][sm.Name] = smARN
	}

	b.smExecutions = make(map[string][]string)
	for execARN, exec := range b.executions {
		b.smExecutions[exec.StateMachineArn] = append(b.smExecutions[exec.StateMachineArn], execARN)
	}

	// Rebuild activity name index and create empty queues (pending tasks are not persisted).
	b.activityNameIndex = make(map[string]map[string]string)
	b.pendingTaskQueues = make(map[string]chan *activityTaskEntry, len(b.activities))

	for actARN, a := range b.activities {
		actRegion := regionFromARN(actARN, b.region)
		if b.activityNameIndex[actRegion] == nil {
			b.activityNameIndex[actRegion] = make(map[string]string)
		}

		b.activityNameIndex[actRegion][a.Name] = actARN
		b.pendingTaskQueues[actARN] = make(chan *activityTaskEntry, maxPendingActivityTasks)
	}

	b.tasksByToken = make(map[string]*activityTaskEntry)

	// cancelFns is intentionally empty after restore. Snapshot() converts any
	// RUNNING execution to TIMED_OUT before serialization, so all restored
	// executions are in terminal states and no goroutines need to be tracked.
	b.cancelFns = make(map[string]context.CancelFunc)
	b.deletedExecs = make(map[string]bool)
	b.historyTruncated = make(map[string]bool)

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
	if err := json.Unmarshal(data, &snap); err != nil {
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
