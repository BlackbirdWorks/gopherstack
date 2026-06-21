package swf

import (
	"context"
	"encoding/json"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type backendSnapshot struct {
	Domains        map[string]*Domain            `json:"domains"`
	Workflows      map[string]*WorkflowType      `json:"workflows"`
	Activities     map[string]*ActivityType      `json:"activities"`
	Executions     map[string]*WorkflowExecution `json:"executions"`
	History        map[string][]HistoryEvent     `json:"history,omitempty"`
	Tags           map[string]map[string]string  `json:"tags,omitempty"`
	ExecutionOrder []string                      `json:"executionOrder"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	domains := make(map[string]*Domain, len(b.domains))
	for k, v := range b.domains {
		domains[k] = cloneDomain(v)
	}

	workflows := make(map[string]*WorkflowType, len(b.workflows))
	for k, v := range b.workflows {
		workflows[k] = cloneWorkflowType(v)
	}

	activities := make(map[string]*ActivityType, len(b.activities))
	for k, v := range b.activities {
		activities[k] = cloneActivityType(v)
	}

	executions := make(map[string]*WorkflowExecution, len(b.executions))
	for k, v := range b.executions {
		executions[k] = cloneExecution(v)
	}

	order := make([]string, len(b.executionOrder))
	copy(order, b.executionOrder)

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

	snap := backendSnapshot{
		Domains:        domains,
		Workflows:      workflows,
		Activities:     activities,
		Executions:     executions,
		ExecutionOrder: order,
		History:        history,
		Tags:           tags,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "swf: snapshot marshal failed", "error", err)

		return nil
	}

	return data
}

// cloneDomain returns a shallow copy of d.
func cloneDomain(d *Domain) *Domain {
	cp := *d

	return &cp
}

// cloneWorkflowType returns a shallow copy of wt.
func cloneWorkflowType(wt *WorkflowType) *WorkflowType {
	cp := *wt

	return &cp
}

// cloneActivityType returns a shallow copy of at.
func cloneActivityType(at *ActivityType) *ActivityType {
	cp := *at

	return &cp
}

// cloneExecution returns a deep copy of e (TagList slice is cloned).
func cloneExecution(e *WorkflowExecution) *WorkflowExecution {
	cp := *e
	if e.TagList != nil {
		cp.TagList = make([]string, len(e.TagList))
		copy(cp.TagList, e.TagList)
	}

	return &cp
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "swf", data, &snap); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.domains = snap.Domains
	b.workflows = snap.Workflows
	b.activities = snap.Activities
	b.executions = snap.Executions
	b.executionOrder = snap.ExecutionOrder
	b.history = snap.History
	b.tags = snap.Tags

	return nil
}

// ensureNonNilMaps replaces any nil maps in the snapshot with empty maps.
func ensureNonNilMaps(s *backendSnapshot) {
	if s.Domains == nil {
		s.Domains = make(map[string]*Domain)
	}

	if s.Workflows == nil {
		s.Workflows = make(map[string]*WorkflowType)
	}

	if s.Activities == nil {
		s.Activities = make(map[string]*ActivityType)
	}

	if s.Executions == nil {
		s.Executions = make(map[string]*WorkflowExecution)
	}

	if s.History == nil {
		s.History = make(map[string][]HistoryEvent)
	}

	if s.Tags == nil {
		s.Tags = make(map[string]map[string]string)
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
