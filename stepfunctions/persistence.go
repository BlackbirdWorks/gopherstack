package stepfunctions

import (
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
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		StateMachines: b.stateMachines,
		Executions:    b.executions,
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

	return nil
}
