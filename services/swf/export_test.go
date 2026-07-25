package swf

import "slices"

// DecisionHandlerTypes returns the sorted set of DecisionType strings the
// decision dispatch table (decision_tasks.go's decisionHandlers) routes to a
// handler, for a table-driven test asserting every SWF decision type is
// covered.
func DecisionHandlerTypes() []string {
	types := make([]string, 0, len(decisionHandlers()))
	for t := range decisionHandlers() {
		types = append(types, t)
	}
	slices.Sort(types)

	return types
}

// DomainCount returns the number of domains in the backend.
func DomainCount(b *InMemoryBackend) int {
	b.mu.RLock("DomainCount")
	defer b.mu.RUnlock()

	return b.domains.Len()
}

// WorkflowTypeCount returns the number of workflow types in the backend.
func WorkflowTypeCount(b *InMemoryBackend) int {
	b.mu.RLock("WorkflowTypeCount")
	defer b.mu.RUnlock()

	return b.workflows.Len()
}

// ActivityTypeCount returns the number of activity types in the backend.
func ActivityTypeCount(b *InMemoryBackend) int {
	b.mu.RLock("ActivityTypeCount")
	defer b.mu.RUnlock()

	return b.activities.Len()
}

// ExecutionCount returns the number of workflow executions in the backend.
func ExecutionCount(b *InMemoryBackend) int {
	b.mu.RLock("ExecutionCount")
	defer b.mu.RUnlock()

	return b.executions.Len()
}

// HandlerOpsLen returns the number of operations in the handler's dispatch table.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}
