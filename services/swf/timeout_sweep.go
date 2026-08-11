package swf

import (
	"strconv"
	"time"
)

// executionDeadline returns exec's ExecutionStartToCloseTimeout deadline as
// epoch seconds, and whether one is configured at all -- an empty or "NONE"
// timeout (validateDuration's accepted sentinel for "no timeout") never
// expires.
func executionDeadline(exec *WorkflowExecution) (float64, bool) {
	if exec.ExecutionStartToCloseTimeout == "" || exec.ExecutionStartToCloseTimeout == retentionNone {
		return 0, false
	}

	secs, err := strconv.Atoi(exec.ExecutionStartToCloseTimeout)
	if err != nil {
		return 0, false
	}

	return exec.StartTimestamp + float64(secs), true
}

// sweepTimedOutExecutionsLocked closes every RUNNING execution whose
// ExecutionStartToCloseTimeout has elapsed as of now. This backend has no
// background timer (see PARITY.md's leaks note) -- timeout enforcement is
// instead lazily evaluated by this sweep at the top of every backend op that
// reads or mutates execution state (Describe/GetHistory/List/Count/Poll/
// Respond/Terminate/RequestCancel/Signal/Start), so a timed-out execution
// becomes visible on the next such call rather than at the real wall-clock
// instant it expired. now is a parameter rather than an internal time.Now()
// call so the sweep's evaluation instant is directly controllable in tests,
// without sleeping or a background goroutine. Caller must hold the write
// lock. Returns the number of executions closed.
func (b *InMemoryBackend) sweepTimedOutExecutionsLocked(now time.Time) int {
	nowEpoch := float64(now.UnixMilli()) / milliDivisor

	swept := 0
	for _, exec := range b.executions.All() {
		if exec.Status != statusRunning {
			continue
		}

		deadline, hasTimeout := executionDeadline(exec)
		if !hasTimeout || nowEpoch < deadline {
			continue
		}

		b.timeoutExecutionLocked(exec.Domain, exec, nowEpoch)
		swept++
	}

	return swept
}

// timeoutExecutionLocked closes exec as TIMED_OUT: real SWF's
// WorkflowExecutionTimedOutEventAttributes carries only childPolicy and
// timeoutType (confirmed against aws-sdk-go-v2/service/swf@v1.37.4
// types.go's WorkflowExecutionTimedOutEventAttributes struct), and
// CloseStatus "TIMED_OUT" is a real types.CloseStatus enum value
// (CloseStatusTimedOut). Real SWF invokes the child policy on exactly two
// events -- TerminateWorkflowExecution and an execution timing out -- so
// this mirrors terminateExecutionLocked: it notifies exec's own parent (if
// still open) and cascades exec.ChildPolicy onto exec's own open children.
// Caller must hold the write lock.
func (b *InMemoryBackend) timeoutExecutionLocked(domain string, exec *WorkflowExecution, closeEpoch float64) {
	exec.Status = statusTimedOut
	exec.CloseStatus = statusTimedOut
	exec.CloseTimestamp = closeEpoch

	attrKey := eventAttrKey("WorkflowExecutionTimedOut")
	attrs := map[string]any{
		attrKey: map[string]any{
			attrChildPolicy: exec.ChildPolicy,
			attrTimeoutType: timeoutTypeStartToClose,
		},
	}
	b.appendHistoryEventLocked(domain, exec.WorkflowID, exec.RunID, "WorkflowExecutionTimedOut", attrs)
	b.propagateChildClosureLocked(domain, exec, "ChildWorkflowExecutionTimedOut", nil)
	b.applyChildPolicyLocked(domain, exec, exec.ChildPolicy)
}
