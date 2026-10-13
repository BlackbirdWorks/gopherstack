package swf

import (
	"slices"
	"strconv"
	"time"
)

const (
	timeoutTypeScheduleToStart = "SCHEDULE_TO_START"
	timeoutTypeScheduleToClose = "SCHEDULE_TO_CLOSE"
	timeoutTypeHeartbeat       = "HEARTBEAT"
)

func nowEpoch(t time.Time) float64 { return float64(t.UnixMilli()) / milliDivisor }

// timeoutSeconds parses a SWF duration; "" and "NONE" are unbounded.
func timeoutSeconds(d string) (float64, bool) {
	if d == "" || d == retentionNone {
		return 0, false
	}

	n, err := strconv.Atoi(d)
	if err != nil || n < 0 {
		return 0, false
	}

	return float64(n), true
}

type deadlineCandidate struct {
	kind     string
	deadline float64
	bounded  bool
}

// expiredKind returns the kind of the earliest bounded deadline that has passed.
func expiredKind(now float64, candidates ...deadlineCandidate) (string, bool) {
	kind, best, found := "", 0.0, false

	for _, c := range candidates {
		if !c.bounded || c.deadline > now {
			continue
		}

		if !found || c.deadline < best {
			kind, best, found = c.kind, c.deadline, true
		}
	}

	return kind, found
}

func candidate(kind string, base float64, timeout string) deadlineCandidate {
	secs, bounded := timeoutSeconds(timeout)

	return deadlineCandidate{kind: kind, deadline: base + secs, bounded: bounded}
}

// resolveTimeout prefers the decision's timeout over the activity type's default.
func resolveTimeout(decision, typeDefault string) string {
	if decision != "" {
		return decision
	}

	return typeDefault
}

// sweepTaskTimeoutsLocked times out queued/started activity tasks and decision tasks
// whose timeouts elapsed, appending the ActivityTaskTimedOut/DecisionTaskTimedOut event
// and scheduling a fresh decision task. Caller must hold the write lock.
func (b *InMemoryBackend) sweepTaskTimeoutsLocked(now float64) {
	b.sweepQueuedActivityTasksLocked(now)
	b.sweepActiveActivityTasksLocked(now)
	b.sweepQueuedDecisionTasksLocked(now)
	b.sweepActiveDecisionTasksLocked(now)
}

func (b *InMemoryBackend) runningExecution(domain, workflowID, runID string) (*WorkflowExecution, bool) {
	exec, ok := b.executions.Get(executionKey(domain, workflowID, runID))
	if !ok || exec.Status != statusRunning {
		return nil, false
	}

	return exec, true
}

func (b *InMemoryBackend) sweepQueuedActivityTasksLocked(now float64) {
	for qkey, queue := range b.activityQueues {
		domain := queueDomain(qkey)

		for _, task := range slices.Clone(queue) {
			if _, running := b.runningExecution(domain, task.WorkflowID, task.RunID); !running {
				continue
			}

			kind, expired := expiredKind(now,
				candidate(timeoutTypeScheduleToStart, task.ScheduledAt, task.ScheduleToStartTimeout),
				candidate(timeoutTypeScheduleToClose, task.ScheduledAt, task.ScheduleToCloseTimeout),
			)
			if !expired {
				continue
			}

			b.activityQueues[qkey] = slices.DeleteFunc(
				b.activityQueues[qkey],
				func(t *ActivityTask) bool { return t == task },
			)
			b.timeoutActivityLocked(domain, task.WorkflowID, task.RunID, task.ScheduledEventID, 0, kind, "")
		}
	}
}

func (b *InMemoryBackend) sweepActiveActivityTasksLocked(now float64) {
	for _, rec := range b.activeActivityTasks.All() {
		if _, running := b.runningExecution(rec.Domain, rec.WorkflowID, rec.RunID); !running {
			continue
		}

		kind, expired := expiredKind(now,
			candidate(timeoutTypeStartToClose, rec.StartedAt, rec.StartToCloseTimeout),
			candidate(timeoutTypeHeartbeat, rec.LastHeartbeatAt, rec.HeartbeatTimeout),
			candidate(timeoutTypeScheduleToClose, rec.ScheduledAt, rec.ScheduleToCloseTimeout),
		)
		if !expired {
			continue
		}

		b.activeActivityTasks.Delete(rec.TaskToken)
		b.timeoutActivityLocked(
			rec.Domain, rec.WorkflowID, rec.RunID, rec.ScheduledEventID, rec.StartedEventID, kind, rec.HeartbeatDetails,
		)
	}
}

func (b *InMemoryBackend) timeoutActivityLocked(
	domain, workflowID, runID string, scheduledEventID, startedEventID int64, timeoutType, details string,
) {
	attrs := map[string]any{
		attrScheduledEvID: scheduledEventID,
		attrStartedEvID:   startedEventID,
		attrTimeoutType:   timeoutType,
	}
	if details != "" {
		attrs[attrDetails] = details
	}

	b.appendHistoryEventLocked(domain, workflowID, runID, "ActivityTaskTimedOut", map[string]any{
		eventAttrKey("ActivityTaskTimedOut"): attrs,
	})
	b.enqueueDecisionTaskLocked(domain, workflowID, runID)
}

func (b *InMemoryBackend) sweepQueuedDecisionTasksLocked(now float64) {
	for qkey, queue := range b.decisionQueues {
		domain := queueDomain(qkey)

		for _, task := range slices.Clone(queue) {
			if task.StickyDeadline == 0 || task.StickyDeadline > now {
				continue
			}

			exec, running := b.runningExecution(domain, task.WorkflowID, task.RunID)
			if !running {
				continue
			}

			b.decisionQueues[qkey] = slices.DeleteFunc(
				b.decisionQueues[qkey],
				func(t *DecisionTask) bool { return t == task },
			)
			b.timeoutDecisionLocked(domain, exec, task.ScheduledEventID, 0, timeoutTypeScheduleToStart, true)
		}
	}
}

func (b *InMemoryBackend) sweepActiveDecisionTasksLocked(now float64) {
	for _, rec := range b.activeDecisionTasks.All() {
		exec, running := b.runningExecution(rec.Domain, rec.WorkflowID, rec.RunID)
		if !running {
			continue
		}

		secs, bounded := timeoutSeconds(exec.TaskStartToCloseTimeout)
		if !bounded || rec.StartedAt+secs > now {
			continue
		}

		b.activeDecisionTasks.Delete(rec.TaskToken)
		b.timeoutDecisionLocked(
			rec.Domain,
			exec,
			rec.ScheduledEventID,
			rec.StartedEventID,
			timeoutTypeStartToClose,
			rec.Sticky,
		)
	}
}

// timeoutDecisionLocked records DecisionTaskTimedOut and reschedules a decision task; a
// timed-out sticky task also reverts the task list override
// (api_op_RespondDecisionTaskCompleted.go: TaskListScheduleToStartTimeout).
func (b *InMemoryBackend) timeoutDecisionLocked(
	domain string,
	exec *WorkflowExecution,
	scheduledEventID, startedEventID int64,
	timeoutType string,
	revertSticky bool,
) {
	b.appendHistoryEventLocked(domain, exec.WorkflowID, exec.RunID, "DecisionTaskTimedOut", map[string]any{
		eventAttrKey("DecisionTaskTimedOut"): map[string]any{
			attrScheduledEvID: scheduledEventID,
			attrStartedEvID:   startedEventID,
			attrTimeoutType:   timeoutType,
		},
	})

	if revertSticky {
		exec.StickyTaskList = ""
		exec.StickyScheduleToStartTimeout = ""
	}

	b.enqueueDecisionTaskLocked(domain, exec.WorkflowID, exec.RunID)
}

func queueDomain(queueKey string) string {
	for i := range queueKey {
		if queueKey[i] == ':' {
			return queueKey[:i]
		}
	}

	return queueKey
}

// DecisionResponseOptions carries RespondDecisionTaskCompleted's optional sticky task
// list override.
type DecisionResponseOptions struct {
	StickyTaskList               string
	StickyScheduleToStartTimeout string
}

// RespondDecisionOption customises RespondDecisionTaskCompleted.
type RespondDecisionOption func(*DecisionResponseOptions)

// WithStickyTaskList routes the execution's future decision tasks to taskList until
// scheduleToStart seconds pass unstarted ("" or NONE: permanent).
func WithStickyTaskList(taskList, scheduleToStart string) RespondDecisionOption {
	return func(o *DecisionResponseOptions) {
		o.StickyTaskList = taskList
		o.StickyScheduleToStartTimeout = scheduleToStart
	}
}

type activityTimeouts struct {
	scheduleToStart string
	scheduleToClose string
	startToClose    string
	heartbeat       string
}

// resolveActivityTimeoutsLocked applies the activity type's registered defaults to
// timeouts the decision left unset. Caller holds the lock.
func (b *InMemoryBackend) resolveActivityTimeoutsLocked(
	domain string,
	attrs *ScheduleActivityTaskDecisionAttrs,
) activityTimeouts {
	var def ActivityTypeDefaults
	if at, ok := b.activities.Get(domain + ":" + attrs.ActivityType.Name + ":" + attrs.ActivityType.Version); ok {
		def = at.Defaults
	}

	return activityTimeouts{
		scheduleToStart: resolveTimeout(attrs.ScheduleToStartTimeout, def.DefaultTaskScheduleToStartTimeout),
		scheduleToClose: resolveTimeout(attrs.ScheduleToCloseTimeout, def.DefaultTaskScheduleToCloseTimeout),
		startToClose:    resolveTimeout(attrs.StartToCloseTimeout, def.DefaultTaskStartToCloseTimeout),
		heartbeat:       resolveTimeout(attrs.HeartbeatTimeout, def.DefaultTaskHeartbeatTimeout),
	}
}

// addTo records the resolved timeouts on an ActivityTaskScheduled attribute map.
func (t activityTimeouts) addTo(attrs map[string]any) {
	for key, v := range map[string]string{
		"scheduleToStartTimeout": t.scheduleToStart,
		"scheduleToCloseTimeout": t.scheduleToClose,
		"startToCloseTimeout":    t.startToClose,
		"heartbeatTimeout":       t.heartbeat,
	} {
		if v != "" {
			attrs[key] = v
		}
	}
}
