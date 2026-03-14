package stepfunctions

import (
	"context"
	"encoding/json"
)

type backendSnapshot struct {
	StateMachines map[string]*StateMachine   `json:"stateMachines"`
	Executions    map[string]*Execution      `json:"executions"`
	History       map[string][]*HistoryEvent `json:"history"`
	AccountID     string                     `json:"accountID"`
	Region        string                     `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
// Any execution still RUNNING at snapshot time is promoted to TIMED_OUT so that
// Restore() never encounters non-terminal executions without a running goroutine.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	// Build a sanitised copy of executions: RUNNING → TIMED_OUT.
	execsCopy := make(map[string]*Execution, len(b.executions))
	for k, exec := range b.executions {
		cp := *exec
		if cp.Status == "RUNNING" {
			cp.Status = "TIMED_OUT"
		}

		execsCopy[k] = &cp
	}

	snap := backendSnapshot{
		StateMachines: b.stateMachines,
		Executions:    execsCopy,
		History:       b.history,
		AccountID:     b.accountID,
		Region:        b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		b.logger.Warn("persistence: snapshot marshal failed", "service", "stepfunctions", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
// Service integrations (Lambda, SQS, SNS, DynamoDB) are not restored — they are re-wired by the CLI.
func (b *InMemoryBackend) Restore(data []byte) error {
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

	b.stateMachines = snap.StateMachines
	b.executions = snap.Executions
	b.history = snap.History
	b.accountID = snap.AccountID
	b.region = snap.Region

	// Rebuild secondary indexes from the restored state.
	b.nameIndex = make(map[string]string, len(b.stateMachines))
	for smARN, sm := range b.stateMachines {
		b.nameIndex[sm.Name] = smARN
	}

	b.smExecutions = make(map[string][]string)
	for execARN, exec := range b.executions {
		b.smExecutions[exec.StateMachineArn] = append(b.smExecutions[exec.StateMachineArn], execARN)
	}

	// cancelFns is intentionally empty after restore. Snapshot() converts any
	// RUNNING execution to TIMED_OUT before serialization, so all restored
	// executions are in terminal states and no goroutines need to be tracked.
	b.cancelFns = make(map[string]context.CancelFunc)
	b.deletedExecs = make(map[string]bool)

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	type snapshotter interface{ Snapshot() []byte }
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot()
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	type restorer interface{ Restore([]byte) error }
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(data)
	}

	return nil
}
