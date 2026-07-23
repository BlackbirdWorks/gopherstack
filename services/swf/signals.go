package swf

import "fmt"

// SignalWorkflowExecution sends a signal to a workflow execution, recording it in history.
func (b *InMemoryBackend) SignalWorkflowExecution(
	domain, workflowID, runID, signalName, input string,
) error {
	b.mu.Lock("SignalWorkflowExecution")
	defer b.mu.Unlock()

	key := domain + ":" + workflowID
	exec, ok := b.executions.Get(key)
	if !ok {
		return fmt.Errorf("%w: execution %s/%s not found", ErrNotFound, domain, workflowID)
	}
	if runID != "" && exec.RunID != runID {
		return fmt.Errorf(
			"%w: runId %s does not match current run %s",
			ErrNotFound,
			runID,
			exec.RunID,
		)
	}
	// Real AWS: "If the specified workflow execution isn't open, this method
	// fails with UnknownResource." (see SignalWorkflowExecution doc) -- not
	// ValidationException, which isn't even in this op's fault model.
	if exec.Status != statusRunning {
		return fmt.Errorf("%w: execution %s/%s is not open", ErrNotFound, domain, workflowID)
	}

	attrKey := eventAttrKey("WorkflowExecutionSignaled")
	attrs := map[string]any{
		attrKey: map[string]any{
			attrSignalName: signalName,
			attrInput:      input,
		},
	}
	b.appendHistoryEventLocked(domain, workflowID, "WorkflowExecutionSignaled", attrs)

	// Enqueue a decision task so the workflow decider can react.
	b.enqueueDecisionTaskLocked(domain, workflowID)

	return nil
}
