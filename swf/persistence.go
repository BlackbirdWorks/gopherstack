package swf

import (
	"encoding/json"
)

type backendSnapshot struct {
	Domains    map[string]*Domain            `json:"domains"`
	Workflows  map[string]*WorkflowType      `json:"workflows"`
	Executions map[string]*WorkflowExecution `json:"executions"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Domains:    b.domains,
		Workflows:  b.workflows,
		Executions: b.executions,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Domains == nil {
		snap.Domains = make(map[string]*Domain)
	}

	if snap.Workflows == nil {
		snap.Workflows = make(map[string]*WorkflowType)
	}

	if snap.Executions == nil {
		snap.Executions = make(map[string]*WorkflowExecution)
	}

	b.domains = snap.Domains
	b.workflows = snap.Workflows
	b.executions = snap.Executions

	return nil
}
