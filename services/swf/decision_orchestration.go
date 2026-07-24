package swf

import (
	"errors"
	"maps"
	"time"

	"github.com/google/uuid"
)

// This file implements the four SWF decision types that reach across
// execution boundaries: ContinueAsNewWorkflowExecution (closes the current
// run and immediately starts a new one under the same workflowId),
// StartChildWorkflowExecution, SignalExternalWorkflowExecution, and
// RequestCancelExternalWorkflowExecution (the latter three act on a
// *different* execution than the one the decision came from). All four used
// to only record an *Initiated history event with empty attributes and never
// perform the underlying action -- see the historical PARITY.md gaps entries
// this file closes.
//
// Architectural caveat (documented in PARITY.md): this backend's executions/
// history stores are keyed by domain+workflowId only (see store.go's
// InMemoryBackend doc comment), not domain+workflowId+runId. Real AWS keeps
// every run of a workflowId as an independently queryable record; here,
// ContinueAsNewWorkflowExecution necessarily overwrites the same
// domain+workflowId row (and appends to the same shared history) when the new
// run starts, so the completed old run is not separately queryable via
// DescribeWorkflowExecution/GetWorkflowExecutionHistory after continuation --
// only the latest run ever is. This is a pre-existing store-shape limitation,
// not something this pass could fix without a broader multi-run redesign.

// handleContinueAsNewWorkflowExecutionDecision closes the current run as
// CONTINUED_AS_NEW and immediately starts a new run under the same
// domain+workflowId, honoring any decision-supplied overrides and otherwise
// falling back to the (possibly re-versioned) WorkflowType's registered
// defaults -- the same resolution StartWorkflowExecution uses. If the
// workflow type can't be resolved, the decision fails with
// ContinueAsNewWorkflowExecutionFailed and the execution is left open (real
// AWS never closes the run on a rejected decision) with a fresh decision task
// so the decider can retry.
func (b *InMemoryBackend) handleContinueAsNewWorkflowExecutionDecision(dc decisionCtx) {
	attrs := dc.decision.ContinueAsNewWorkflowExecutionAttrs
	if attrs == nil {
		attrs = &ContinueAsNewWorkflowExecutionDecisionAttrs{}
	}

	typeVersion := attrs.WorkflowTypeVersion
	if typeVersion == "" {
		typeVersion = dc.exec.WorkflowTypeVersion
	}

	newInput := StartWorkflowExecutionInput{
		Domain:                       dc.domain,
		WorkflowID:                   dc.workflowID,
		WorkflowTypeName:             dc.exec.WorkflowTypeName,
		WorkflowTypeVersion:          typeVersion,
		Input:                        attrs.Input,
		TaskList:                     attrs.TaskList,
		ChildPolicy:                  attrs.ChildPolicy,
		LambdaRole:                   attrs.LambdaRole,
		ExecutionStartToCloseTimeout: attrs.ExecutionStartToCloseTimeout,
		TaskStartToCloseTimeout:      attrs.TaskStartToCloseTimeout,
		TaskPriority:                 attrs.TaskPriority,
		TagList:                      attrs.TagList,
	}

	defaults, err := b.resolveExecutionDefaultsLocked(newInput)
	if err != nil {
		b.appendHistoryEventLocked(dc.domain, dc.workflowID, "ContinueAsNewWorkflowExecutionFailed", map[string]any{
			eventAttrKey("ContinueAsNewWorkflowExecutionFailed"): map[string]any{
				attrDTCEventID: dc.decisionTaskCompletedEventID,
				attrCause:      continueAsNewFailureCause(err),
			},
		})
		// The old run never closed; re-enqueue a decision task so the
		// decider gets another chance instead of the execution going
		// silently stuck (its previous decision task token was already
		// consumed by RespondDecisionTaskCompleted).
		b.enqueueDecisionTaskLocked(dc.domain, dc.workflowID)

		return
	}

	oldRunID := dc.exec.RunID
	newInput.RunID = uuid.New().String()

	b.appendHistoryEventLocked(dc.domain, dc.workflowID, "WorkflowExecutionContinuedAsNew", map[string]any{
		eventAttrKey("WorkflowExecutionContinuedAsNew"): map[string]any{
			attrDTCEventID:      dc.decisionTaskCompletedEventID,
			"newExecutionRunId": newInput.RunID,
			attrChildPolicy:     defaults.childPolicy,
			attrTaskList:        map[string]any{attrName: defaults.taskList},
			attrWorkflowType: map[string]any{
				attrName:    newInput.WorkflowTypeName,
				attrVersion: newInput.WorkflowTypeVersion,
			},
			attrExecToCloseTO: defaults.execTimeout,
			attrTaskToCloseTO: defaults.taskTimeout,
			"taskPriority":    newInput.TaskPriority,
			attrLambdaRole:    defaults.lambdaRole,
			attrTagList:       newInput.TagList,
			attrInput:         newInput.Input,
		},
	})

	// Close the old run first: createExecutionLocked's "already open" guard
	// checks the live executions row's Status, and dc.exec IS that live row
	// (a pointer into the executions table, not a copy -- see
	// RespondDecisionTaskCompleted). From any external caller's point of view
	// there is still no window where this workflowId has no open execution,
	// since we hold b.mu for the entire close-old/open-new sequence and
	// Put() below immediately replaces this row with the new run --
	// matching real AWS's atomic continue-as-new semantics.
	dc.exec.Status = statusContinuedAsNew
	dc.exec.CloseStatus = statusContinuedAsNew
	dc.exec.CloseTimestamp = float64(time.Now().UnixMilli()) / milliDivisor

	if _, createErr := b.createExecutionLocked(newInput, defaults, oldRunID, nil); createErr != nil {
		// Only reachable if createExecutionLocked's precondition changes out
		// from under this handler in the future -- kept so that happens
		// loudly (a recorded failure event) instead of silently dropping the
		// decision. Revert the close so the execution isn't left stranded.
		dc.exec.Status = statusRunning
		dc.exec.CloseStatus = ""
		dc.exec.CloseTimestamp = 0
		b.appendHistoryEventLocked(dc.domain, dc.workflowID, "ContinueAsNewWorkflowExecutionFailed", map[string]any{
			eventAttrKey("ContinueAsNewWorkflowExecutionFailed"): map[string]any{
				attrDTCEventID: dc.decisionTaskCompletedEventID,
				attrCause:      causeOpNotPermitted,
			},
		})
		b.enqueueDecisionTaskLocked(dc.domain, dc.workflowID)
	}
}

// continueAsNewFailureCause maps a resolveExecutionDefaultsLocked error to the
// real ContinueAsNewWorkflowExecutionFailedCause wire value.
func continueAsNewFailureCause(err error) string {
	switch {
	case errors.Is(err, ErrTypeDeprecated):
		return "WORKFLOW_TYPE_DEPRECATED"
	case errors.Is(err, ErrNotFound):
		return "WORKFLOW_TYPE_DOES_NOT_EXIST"
	default:
		return causeOpNotPermitted
	}
}

// handleStartChildWorkflowExecutionDecision actually starts the child
// execution (reusing the same createExecutionLocked core as
// StartWorkflowExecution/ContinueAsNewWorkflowExecution) instead of only
// recording an Initiated event. On success it records
// ChildWorkflowExecutionStarted on the parent's history and links the child
// back to its parent (see WorkflowExecution.ParentWorkflowID et al. in
// models.go) so the child's eventual close is echoed back onto the parent
// (propagateChildClosureLocked below) and DescribeWorkflowExecution's
// openCounts.openChildWorkflowExecutions reflects it. On failure it records
// StartChildWorkflowExecutionFailed with the matching real-AWS cause instead.
func (b *InMemoryBackend) handleStartChildWorkflowExecutionDecision(dc decisionCtx) {
	attrs := dc.decision.StartChildWorkflowExecutionAttrs
	if attrs == nil {
		return
	}

	initiatedEventID := b.appendHistoryEventLocked(
		dc.domain, dc.workflowID, "StartChildWorkflowExecutionInitiated", map[string]any{
			eventAttrKey("StartChildWorkflowExecutionInitiated"): map[string]any{
				attrDTCEventID: dc.decisionTaskCompletedEventID,
				attrWorkflowID: attrs.WorkflowID,
				attrWorkflowType: map[string]any{
					attrName:    attrs.WorkflowType.Name,
					attrVersion: attrs.WorkflowType.Version,
				},
				attrControl:       attrs.Control,
				attrInput:         attrs.Input,
				attrExecToCloseTO: attrs.ExecutionStartToCloseTimeout,
				attrTaskList:      map[string]any{attrName: attrs.TaskList},
				"taskPriority":    attrs.TaskPriority,
				attrTaskToCloseTO: attrs.TaskStartToCloseTimeout,
				attrChildPolicy:   attrs.ChildPolicy,
				attrLambdaRole:    attrs.LambdaRole,
				attrTagList:       attrs.TagList,
			},
		})

	childInput := StartWorkflowExecutionInput{
		Domain:                       dc.domain,
		WorkflowID:                   attrs.WorkflowID,
		WorkflowTypeName:             attrs.WorkflowType.Name,
		WorkflowTypeVersion:          attrs.WorkflowType.Version,
		Input:                        attrs.Input,
		TaskList:                     attrs.TaskList,
		ChildPolicy:                  attrs.ChildPolicy,
		LambdaRole:                   attrs.LambdaRole,
		ExecutionStartToCloseTimeout: attrs.ExecutionStartToCloseTimeout,
		TaskStartToCloseTimeout:      attrs.TaskStartToCloseTimeout,
		TaskPriority:                 attrs.TaskPriority,
		TagList:                      attrs.TagList,
	}

	defaults, err := b.resolveExecutionDefaultsLocked(childInput)

	var child *WorkflowExecution
	if err == nil {
		child, err = b.createExecutionLocked(childInput, defaults, "", &childLink{
			parentWorkflowID:       dc.workflowID,
			parentRunID:            dc.exec.RunID,
			parentInitiatedEventID: initiatedEventID,
		})
	}

	if err != nil {
		b.appendHistoryEventLocked(dc.domain, dc.workflowID, "StartChildWorkflowExecutionFailed", map[string]any{
			eventAttrKey("StartChildWorkflowExecutionFailed"): map[string]any{
				attrDTCEventID:    dc.decisionTaskCompletedEventID,
				attrInitiatedEvID: initiatedEventID,
				attrWorkflowID:    attrs.WorkflowID,
				attrWorkflowType: map[string]any{
					attrName:    attrs.WorkflowType.Name,
					attrVersion: attrs.WorkflowType.Version,
				},
				attrControl: attrs.Control,
				attrCause:   startChildFailureCause(err),
			},
		})

		return
	}

	startedEventID := b.appendHistoryEventLocked(
		dc.domain,
		dc.workflowID,
		"ChildWorkflowExecutionStarted",
		map[string]any{
			eventAttrKey("ChildWorkflowExecutionStarted"): map[string]any{
				attrInitiatedEvID: initiatedEventID,
				attrWorkflowExec: map[string]any{
					attrWorkflowID: child.WorkflowID, attrRunID: child.RunID,
				},
				attrWorkflowType: map[string]any{
					attrName:    child.WorkflowTypeName,
					attrVersion: child.WorkflowTypeVersion,
				},
			},
		},
	)

	// createExecutionLocked returns a detached copy (see its doc comment);
	// stamp ParentStartedEventID onto the live stored row so
	// propagateChildClosureLocked can echo it back when this child closes.
	if live, ok := b.executions.Get(dc.domain + ":" + child.WorkflowID); ok {
		live.ParentStartedEventID = startedEventID
	}
}

// startChildFailureCause maps a createExecutionLocked/resolveExecutionDefaultsLocked
// error to the real StartChildWorkflowExecutionFailedCause wire value.
func startChildFailureCause(err error) string {
	switch {
	case errors.Is(err, ErrWorkflowAlreadyStarted):
		return "WORKFLOW_ALREADY_RUNNING"
	case errors.Is(err, ErrTypeDeprecated):
		return "WORKFLOW_TYPE_DEPRECATED"
	case errors.Is(err, ErrNotFound):
		return "WORKFLOW_TYPE_DOES_NOT_EXIST"
	default:
		return causeOpNotPermitted
	}
}

// unknownExternalExecutionCause is the real
// SignalExternalWorkflowExecutionFailedCause /
// RequestCancelExternalWorkflowExecutionFailedCause value AWS uses when the
// targeted execution can't be found, isn't open, or its runId doesn't match.
const unknownExternalExecutionCause = "UNKNOWN_EXTERNAL_WORKFLOW_EXECUTION"

// handleSignalExternalWorkflowExecutionDecision actually delivers the signal
// to the target execution (appending WorkflowExecutionSignaled to its history
// and enqueuing it a decision task) instead of only recording an Initiated
// event with no effect. If the target isn't found/open/run-matching, it
// records SignalExternalWorkflowExecutionFailed instead, matching real AWS's
// UNKNOWN_EXTERNAL_WORKFLOW_EXECUTION cause.
func (b *InMemoryBackend) handleSignalExternalWorkflowExecutionDecision(dc decisionCtx) {
	attrs := dc.decision.SignalExternalWorkflowExecutionAttrs
	if attrs == nil {
		return
	}

	initiatedEventID := b.appendHistoryEventLocked(
		dc.domain, dc.workflowID, "SignalExternalWorkflowExecutionInitiated", map[string]any{
			eventAttrKey("SignalExternalWorkflowExecutionInitiated"): map[string]any{
				attrDTCEventID: dc.decisionTaskCompletedEventID,
				attrWorkflowID: attrs.WorkflowID,
				attrRunID:      attrs.RunID,
				attrSignalName: attrs.SignalName,
				attrInput:      attrs.Input,
				attrControl:    attrs.Control,
			},
		})

	target, ok := b.executions.Get(dc.domain + ":" + attrs.WorkflowID)
	if !ok || target.Status != statusRunning || (attrs.RunID != "" && target.RunID != attrs.RunID) {
		b.appendHistoryEventLocked(dc.domain, dc.workflowID, "SignalExternalWorkflowExecutionFailed", map[string]any{
			eventAttrKey("SignalExternalWorkflowExecutionFailed"): map[string]any{
				attrDTCEventID:    dc.decisionTaskCompletedEventID,
				attrInitiatedEvID: initiatedEventID,
				attrWorkflowID:    attrs.WorkflowID,
				attrRunID:         attrs.RunID,
				attrControl:       attrs.Control,
				attrCause:         unknownExternalExecutionCause,
			},
		})

		return
	}

	b.appendHistoryEventLocked(dc.domain, attrs.WorkflowID, "WorkflowExecutionSignaled", map[string]any{
		eventAttrKey("WorkflowExecutionSignaled"): map[string]any{
			attrSignalName:             attrs.SignalName,
			attrInput:                  attrs.Input,
			"externalInitiatedEventId": initiatedEventID,
			"externalWorkflowExecution": map[string]any{
				attrWorkflowID: dc.workflowID, attrRunID: dc.exec.RunID,
			},
		},
	})
	b.enqueueDecisionTaskLocked(dc.domain, attrs.WorkflowID)

	b.appendHistoryEventLocked(dc.domain, dc.workflowID, "ExternalWorkflowExecutionSignaled", map[string]any{
		eventAttrKey("ExternalWorkflowExecutionSignaled"): map[string]any{
			attrInitiatedEvID: initiatedEventID,
			attrWorkflowExec: map[string]any{
				attrWorkflowID: target.WorkflowID, attrRunID: target.RunID,
			},
		},
	})
}

// handleRequestCancelExternalWorkflowExecutionDecision actually requests
// cancellation of the target execution (setting CancelRequested, appending
// WorkflowExecutionCancelRequested to its history, and enqueuing it a
// decision task) instead of only recording an Initiated event with no
// effect. If the target isn't found/open/run-matching, it records
// RequestCancelExternalWorkflowExecutionFailed instead, matching real AWS's
// UNKNOWN_EXTERNAL_WORKFLOW_EXECUTION cause.
func (b *InMemoryBackend) handleRequestCancelExternalWorkflowExecutionDecision(dc decisionCtx) {
	attrs := dc.decision.RequestCancelExternalWorkflowExecutionAttrs
	if attrs == nil {
		return
	}

	initiatedEventID := b.appendHistoryEventLocked(
		dc.domain, dc.workflowID, "RequestCancelExternalWorkflowExecutionInitiated", map[string]any{
			eventAttrKey("RequestCancelExternalWorkflowExecutionInitiated"): map[string]any{
				attrDTCEventID: dc.decisionTaskCompletedEventID,
				attrWorkflowID: attrs.WorkflowID,
				attrRunID:      attrs.RunID,
				attrControl:    attrs.Control,
			},
		})

	target, ok := b.executions.Get(dc.domain + ":" + attrs.WorkflowID)
	if !ok || target.Status != statusRunning || (attrs.RunID != "" && target.RunID != attrs.RunID) {
		b.appendHistoryEventLocked(
			dc.domain,
			dc.workflowID,
			"RequestCancelExternalWorkflowExecutionFailed",
			map[string]any{
				eventAttrKey("RequestCancelExternalWorkflowExecutionFailed"): map[string]any{
					attrDTCEventID:    dc.decisionTaskCompletedEventID,
					attrInitiatedEvID: initiatedEventID,
					attrWorkflowID:    attrs.WorkflowID,
					attrRunID:         attrs.RunID,
					attrControl:       attrs.Control,
					attrCause:         unknownExternalExecutionCause,
				},
			},
		)

		return
	}

	target.CancelRequested = true
	b.appendHistoryEventLocked(dc.domain, attrs.WorkflowID, "WorkflowExecutionCancelRequested", map[string]any{
		eventAttrKey("WorkflowExecutionCancelRequested"): map[string]any{
			"externalInitiatedEventId": initiatedEventID,
			"externalWorkflowExecution": map[string]any{
				attrWorkflowID: dc.workflowID, attrRunID: dc.exec.RunID,
			},
		},
	})
	b.enqueueDecisionTaskLocked(dc.domain, attrs.WorkflowID)

	b.appendHistoryEventLocked(dc.domain, dc.workflowID, "ExternalWorkflowExecutionCancelRequested", map[string]any{
		eventAttrKey("ExternalWorkflowExecutionCancelRequested"): map[string]any{
			attrInitiatedEvID: initiatedEventID,
			attrWorkflowExec: map[string]any{
				attrWorkflowID: target.WorkflowID, attrRunID: target.RunID,
			},
		},
	})
}

// propagateChildClosureLocked appends the appropriate Child* closure event to
// the parent's history when a child execution (one with ParentWorkflowID set
// via a prior StartChildWorkflowExecution decision) closes, and enqueues the
// parent a decision task -- so the parent decider learns the outcome without
// polling the child directly, matching real AWS's automatic child-closure
// notification. extra supplies the event-specific payload (e.g. "result" for
// Completed, "reason"/"details" for Failed/Canceled); pass nil for
// Terminated, which carries none. No-op if child has no parent link (a
// top-level execution) or the parent is no longer a live, open execution.
// Caller must hold the write lock.
func (b *InMemoryBackend) propagateChildClosureLocked(
	domain string,
	child *WorkflowExecution,
	eventType string,
	extra map[string]any,
) {
	if child.ParentWorkflowID == "" {
		return
	}
	parent, ok := b.executions.Get(domain + ":" + child.ParentWorkflowID)
	if !ok || parent.RunID != child.ParentRunID || parent.Status != statusRunning {
		return
	}

	attrs := map[string]any{
		attrInitiatedEvID: child.ParentInitiatedEventID,
		"startedEventId":  child.ParentStartedEventID,
		attrWorkflowExec: map[string]any{
			attrWorkflowID: child.WorkflowID, attrRunID: child.RunID,
		},
		attrWorkflowType: map[string]any{
			attrName:    child.WorkflowTypeName,
			attrVersion: child.WorkflowTypeVersion,
		},
	}
	maps.Copy(attrs, extra)

	b.appendHistoryEventLocked(domain, child.ParentWorkflowID, eventType, map[string]any{
		eventAttrKey(eventType): attrs,
	})
	b.enqueueDecisionTaskLocked(domain, child.ParentWorkflowID)
}
