package swf

import (
	"slices"
	"strconv"
	"time"
)

// closedExecutionRetentionCutoffLocked returns the epoch cutoff for closed executions
// and false when retention is NONE/unset. Caller holds the read lock.
// https://docs.aws.amazon.com/amazonswf/latest/apireference/API_RegisterDomain.html
func (b *InMemoryBackend) closedExecutionRetentionCutoffLocked(domain string, now time.Time) (float64, bool) {
	d, ok := b.domains.Get(domain)
	if !ok || d.WorkflowExecutionRetentionPeriodInDays == "" ||
		d.WorkflowExecutionRetentionPeriodInDays == retentionNone {
		return 0, false
	}

	days, err := strconv.Atoi(d.WorkflowExecutionRetentionPeriodInDays)
	if err != nil || days < 0 {
		return 0, false
	}

	return float64(now.AddDate(0, 0, -days).Unix()), true
}

// sweepExpiredClosedExecutionsLocked evicts closed executions past their domain's
// retention. Caller holds the write lock.
func (b *InMemoryBackend) sweepExpiredClosedExecutionsLocked(now time.Time) {
	var toEvict []string

	for _, exec := range b.executions.All() {
		if exec.Status == statusRunning || exec.CloseTimestamp == 0 {
			continue
		}

		cutoff, finite := b.closedExecutionRetentionCutoffLocked(exec.Domain, now)
		if !finite || exec.CloseTimestamp >= cutoff {
			continue
		}

		toEvict = append(toEvict, executionKey(exec.Domain, exec.WorkflowID, exec.RunID))
	}

	for _, key := range toEvict {
		b.evictExecutionLocked(key)
	}
}

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
// ExecutionStartToCloseTimeout has elapsed as of now, and fires any of its
// open StartTimer decisions whose StartToFireTimeout has likewise elapsed.
// This backend has no background timer (see PARITY.md's leaks note) --
// timeout/timer enforcement is instead lazily evaluated by this sweep at the
// top of every backend op that reads or mutates execution state
// (Describe/GetHistory/List/Count/Poll/Respond/Terminate/RequestCancel/
// Signal/Start), so a timed-out execution or fired timer becomes visible on
// the next such call rather than at the real wall-clock instant it expired.
// now is a parameter rather than an internal time.Now() call so the sweep's
// evaluation instant is directly controllable in tests, without sleeping or
// a background goroutine, and evicts closed executions past retention. Caller must
// hold the write lock. Returns executions closed (evictions not counted).
func (b *InMemoryBackend) sweepTimedOutExecutionsLocked(now time.Time) int {
	nowEpoch := float64(now.UnixMilli()) / milliDivisor

	swept := 0
	for _, exec := range b.executions.All() {
		if exec.Status != statusRunning {
			continue
		}

		b.fireExpiredTimersLocked(exec, nowEpoch)

		deadline, hasTimeout := executionDeadline(exec)
		if !hasTimeout || nowEpoch < deadline {
			continue
		}

		b.timeoutExecutionLocked(exec.Domain, exec, nowEpoch)
		swept++
	}

	b.sweepExpiredClosedExecutionsLocked(now)

	return swept
}

// fireExpiredTimersLocked fires every one of exec's open StartTimer
// decisions whose StartToFireTimeout has elapsed as of nowEpoch: real SWF
// appends a TimerFired history event (types.TimerFiredEventAttributes
// requires startedEventId/timerId, both derived from the timer's own
// StartTimer decision -- confirmed against
// aws-sdk-go-v2/service/swf@v1.37.4/types/types.go) and gives the execution
// a fresh decision task. Caller must hold the write lock; exec must be
// RUNNING.
func (b *InMemoryBackend) fireExpiredTimersLocked(exec *WorkflowExecution, nowEpoch float64) {
	for _, timerID := range slices.Clone(exec.OpenTimerIDs) {
		deadline, ok := exec.OpenTimerDeadlines[timerID]
		if !ok || nowEpoch < deadline {
			continue
		}

		startedEventID := exec.TimerStartedEventIDs[timerID]

		idx := slices.Index(exec.OpenTimerIDs, timerID)
		exec.OpenTimerIDs = slices.Delete(exec.OpenTimerIDs, idx, idx+1)
		delete(exec.TimerStartedEventIDs, timerID)
		delete(exec.OpenTimerDeadlines, timerID)

		b.appendHistoryEventLocked(exec.Domain, exec.WorkflowID, exec.RunID, "TimerFired", map[string]any{
			eventAttrKey("TimerFired"): map[string]any{
				attrTimerID:     timerID,
				attrStartedEvID: startedEventID,
			},
		})
		b.enqueueDecisionTaskLocked(exec.Domain, exec.WorkflowID, exec.RunID)
	}
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
	// ChildWorkflowExecutionTimedOutEventAttributes.TimeoutType is required
	// (types/types.go, same real enum/value as WorkflowExecutionTimedOut above) --
	// propagateChildClosureLocked's own base attrs carry no timeoutType, so it must
	// come from extra here, same as Completed/Failed/Canceled pass their own payload.
	b.propagateChildClosureLocked(domain, exec, "ChildWorkflowExecutionTimedOut", map[string]any{
		attrTimeoutType: timeoutTypeStartToClose,
	})
	b.applyChildPolicyLocked(domain, exec, exec.ChildPolicy)
}
