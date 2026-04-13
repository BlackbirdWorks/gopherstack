package swf

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	Domains        map[string]*Domain            `json:"domains"`
	Workflows      map[string]*WorkflowType      `json:"workflows"`
	Activities     map[string]*ActivityType      `json:"activities"`
	Executions     map[string]*WorkflowExecution `json:"executions"`
	ExecutionOrder []string                      `json:"executionOrder"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	// Deep-copy maps to ensure snapshot isolation.
	domains := make(map[string]*Domain, len(b.domains))
	for k, v := range b.domains {
		cp := *v
		domains[k] = &cp
	}

	workflows := make(map[string]*WorkflowType, len(b.workflows))
	for k, v := range b.workflows {
		cp := *v
		workflows[k] = &cp
	}

	activities := make(map[string]*ActivityType, len(b.activities))
	for k, v := range b.activities {
		cp := *v
		activities[k] = &cp
	}

	executions := make(map[string]*WorkflowExecution, len(b.executions))
	for k, v := range b.executions {
		cp := *v
		executions[k] = &cp
	}

	order := make([]string, len(b.executionOrder))
	copy(order, b.executionOrder)

	snap := backendSnapshot{
		Domains:        domains,
		Workflows:      workflows,
		Activities:     activities,
		Executions:     executions,
		ExecutionOrder: order,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("swf: snapshot marshal failed", "error", err)

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

	if snap.Activities == nil {
		snap.Activities = make(map[string]*ActivityType)
	}

	if snap.Executions == nil {
		snap.Executions = make(map[string]*WorkflowExecution)
	}

	b.domains = snap.Domains
	b.workflows = snap.Workflows
	b.activities = snap.Activities
	b.executions = snap.Executions
	b.executionOrder = snap.ExecutionOrder

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	return h.Backend.Snapshot()
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	return h.Backend.Restore(data)
}
