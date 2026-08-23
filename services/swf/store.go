package swf

import (
	"fmt"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// InMemoryBackend is the in-memory store for SWF resources.
//
// domains, workflows, activities, and executions are "clean" store.Table-backed
// collections (Phase 3.3 of the datalayer refactor): each value type already carries its
// own identity as real, wire-visible JSON fields (Domain/Name/Version or
// Domain/WorkflowID), so each is registered directly on registry -- see store_setup.go.
// workflows/activities/executions additionally carry a companion byDomain store.Index
// (workflowsByDomain/activitiesByDomain/executionsByDomain) replacing the linear
// full-table scan+filter the old flat maps needed for every domain-scoped List/Count op.
//
// activeActivityTasks/activeDecisionTasks are "dirty" store.Table-backed collections:
// their key (taskToken) has no home on the value type, so each gained a TaskToken field
// tagged json:"-" purely for store.Table's keyFn (see the type docs in models.go) and is
// NOT registered on registry -- persistence.go instead round-trips them through an
// ephemeral DTO registry, exactly as the "dirty" tables in services/ses and
// services/codeartifact do.
//
// history, activityQueues, decisionQueues, and tags are deliberately left as plain maps:
// history/activityQueues/decisionQueues are ORDER-SENSITIVE (event histories and FIFO
// task queues, where store.Index's swap-with-last removal would silently reorder pending
// entries), and tags's values (map[string]string) are not *T, which store.Table requires.
//
// executions/history are keyed by domain+":"+workflowID+":"+runID (see workflowExecutionKeyFn/
// appendHistoryEventLocked), NOT domain+":"+workflowID alone: real AWS keeps every run of a
// workflowId as an independently queryable record (DescribeWorkflowExecution/
// GetWorkflowExecutionHistory's Execution parameter requires BOTH WorkflowId and RunId --
// confirmed against aws-sdk-go-v2/service/swf's types.WorkflowExecution, where RunId is a
// required member, not optional), so a second/later run under the same workflowId must not
// collide with or overwrite an earlier one. executionsByWorkflow groups every run (open or
// closed) under domain+":"+workflowID so resolveExecutionLocked/openExecutionLocked can find
// "the currently open run" without a full-domain scan -- real AWS guarantees at most one OPEN
// run per workflowId at a time (createExecutionLocked's "already open" guard), so that lookup
// is always unambiguous.
type InMemoryBackend struct {
	registry             *store.Registry
	domains              *store.Table[Domain]
	workflows            *store.Table[WorkflowType] // key: domain+":"+name+":"+version
	workflowsByDomain    *store.Index[WorkflowType]
	activities           *store.Table[ActivityType] // key: domain+":"+name+":"+version
	activitiesByDomain   *store.Index[ActivityType]
	executions           *store.Table[WorkflowExecution] // key: domain+":"+workflowID+":"+runID
	executionsByDomain   *store.Index[WorkflowExecution]
	executionsByWorkflow *store.Index[WorkflowExecution]        // key: domain+":"+workflowID
	activeActivityTasks  *store.Table[activeActivityTaskRecord] // key: taskToken
	activeDecisionTasks  *store.Table[activeDecisionTaskRecord] // key: taskToken
	history              map[string][]HistoryEvent              // key: domain+":"+workflowID+":"+runID
	activityQueues       map[string][]*ActivityTask             // key: domain+":"+taskList
	decisionQueues       map[string][]*DecisionTask             // key: domain+":"+taskList
	tags                 map[string]map[string]string           // key: resourceARN
	mu                   *lockmetrics.RWMutex
	executionOrder       []string // FIFO order of execution keys for eviction
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		registry:       store.NewRegistry(),
		history:        make(map[string][]HistoryEvent),
		activityQueues: make(map[string][]*ActivityTask),
		decisionQueues: make(map[string][]*DecisionTask),
		tags:           make(map[string]map[string]string),
		mu:             lockmetrics.New("swf"),
	}
	registerAllTables(b)

	return b
}

// Reset clears all backend state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.activeActivityTasks.Reset()
	b.activeDecisionTasks.Reset()
	b.history = make(map[string][]HistoryEvent)
	b.activityQueues = make(map[string][]*ActivityTask)
	b.decisionQueues = make(map[string][]*DecisionTask)
	b.tags = make(map[string]map[string]string)
	b.executionOrder = nil
}

// appendHistoryEventLocked appends a history event for one specific run of a
// workflow execution. attrs is the event-type-specific attributes map; pass
// nil if none. Caller must hold the write lock.
func (b *InMemoryBackend) appendHistoryEventLocked(
	domain, workflowID, runID, eventType string,
	attrs map[string]any,
) int64 {
	key := executionKey(domain, workflowID, runID)
	events := b.history[key]
	eventID := int64(len(events)) + 1
	ev := HistoryEvent{
		EventID:   eventID,
		EventType: eventType,
		Timestamp: float64(time.Now().UnixMilli()) / milliDivisor,
	}
	if len(attrs) > 0 {
		ev.Attributes = attrs
	}
	b.history[key] = append(events, ev)

	return eventID
}

// executionKey builds the primary key for one specific run of a workflow
// execution: domain+":"+workflowID+":"+runID. This is the key shape
// b.executions/b.history are stored under -- see the InMemoryBackend doc
// comment for why it includes runID.
func executionKey(domain, workflowID, runID string) string {
	return domain + ":" + workflowID + ":" + runID
}

// workflowGroupKey builds the key executionsByWorkflow groups every run
// (open or closed) of a workflow under: domain+":"+workflowID, with no runID.
func workflowGroupKey(domain, workflowID string) string {
	return domain + ":" + workflowID
}

// openExecutionLocked returns the single currently-OPEN run for
// domain+workflowID, if any. Real AWS enforces at most one OPEN run per
// workflowId at a time (see createExecutionLocked's "already open" guard),
// so this is always unambiguous. Caller must hold at least the read lock.
func (b *InMemoryBackend) openExecutionLocked(domain, workflowID string) (*WorkflowExecution, bool) {
	for _, e := range b.executionsByWorkflow.Get(workflowGroupKey(domain, workflowID)) {
		if e.Status == statusRunning {
			return e, true
		}
	}

	return nil, false
}

// resolveExecutionLocked finds a run by domain+workflowID, optionally pinned
// to a specific runID.
//
// A non-empty runID is looked up directly and can address ANY run, open or
// already closed -- this is what makes a completed run's history
// independently queryable after ContinueAsNewWorkflowExecution, which real
// AWS's WorkflowExecution.RunId (a required field on
// DescribeWorkflowExecution/GetWorkflowExecutionHistory) requires.
//
// An empty runID first tries the currently open run -- real AWS's own
// convention for the ops whose RunId parameter is genuinely optional
// (TerminateWorkflowExecution/RequestCancelWorkflowExecution/
// SignalWorkflowExecution: "if not specified, defaults to the currently
// running execution"); those three ops separately re-check exec.Status ==
// running regardless of how exec was resolved, so this fallback cannot let
// them act on the wrong run. If no run is currently open, this falls back
// further to the most recently started run for that workflowID (deliberately
// lenient: real AWS would error UnknownResource here, but this backend keeps
// its pre-multi-run behavior for the still-common case of a caller
// Describe/GetHistory-ing a workflowId without tracking its RunId, now that
// a closed run is no longer silently overwritten by whichever run happens to
// share its workflowId). Caller must hold at least the read lock.
func (b *InMemoryBackend) resolveExecutionLocked(domain, workflowID, runID string) (*WorkflowExecution, bool) {
	if runID != "" {
		return b.executions.Get(executionKey(domain, workflowID, runID))
	}

	if exec, ok := b.openExecutionLocked(domain, workflowID); ok {
		return exec, true
	}

	runs := b.executionsByWorkflow.Get(workflowGroupKey(domain, workflowID))
	if len(runs) == 0 {
		return nil, false
	}

	return runs[len(runs)-1], true
}

// AccountID returns the account ID for this backend.
func (b *InMemoryBackend) AccountID() string { return defaultAccountID }

// domainARN constructs the SWF ARN for a domain.
func domainARN(region, account, name string) string {
	return arn.Build("swf", region, account, fmt.Sprintf("/domain/%s", name))
}

// validateChildPolicy returns an error if policy is not a valid SWF child policy.
// An empty string is allowed (means "use default").
func validateChildPolicy(policy string) error {
	switch policy {
	case "", childPolicyTerminate, childPolicyRequestCancel, childPolicyAbandon:
		return nil
	}

	return fmt.Errorf(
		"%w: invalid childPolicy %q, must be TERMINATE, REQUEST_CANCEL, or ABANDON",
		ErrValidation,
		policy,
	)
}

// validateDuration returns an error if d is not a valid SWF duration string.
// Valid values: positive integer (seconds) or "NONE". Empty string means unset.
func validateDuration(d string) error {
	if d == "" || d == retentionNone {
		return nil
	}
	n, err := strconv.Atoi(d)
	if err != nil || n < 0 {
		return fmt.Errorf(
			"%w: invalid duration %q, must be a non-negative integer or NONE",
			ErrValidation,
			d,
		)
	}

	return nil
}

// validateRetention returns an error if retention is not a valid SWF retention period.
// Valid values: "0"-"90" or "NONE".
func validateRetention(retention string) error {
	if retention == "" || retention == retentionNone {
		return nil
	}
	n, err := strconv.Atoi(retention)
	if err != nil || n < 0 || n > 90 {
		return fmt.Errorf(
			"%w: invalid workflowExecutionRetentionPeriodInDays %q, must be 0-90 or NONE",
			ErrValidation, retention,
		)
	}

	return nil
}

// validateRegistrationStatus returns an error if status is not REGISTERED or DEPRECATED.
func validateRegistrationStatus(status string) error {
	switch status {
	case statusRegistered, statusDeprecated:
		return nil
	case "":
		return nil
	}

	return fmt.Errorf(
		"%w: invalid registrationStatus %q, must be REGISTERED or DEPRECATED",
		ErrValidation, status,
	)
}

// EnqueueActivityTaskInternal seeds an activity task in a task list for testing.
func (b *InMemoryBackend) EnqueueActivityTaskInternal(
	domain, taskList, activityID, activityName, activityVersion, input, workflowID, runID string,
) {
	b.mu.Lock("EnqueueActivityTaskInternal")
	defer b.mu.Unlock()

	key := domain + ":" + taskList
	b.activityQueues[key] = append(b.activityQueues[key], &ActivityTask{
		ActivityID:   activityID,
		ActivityType: ActivityTaskActivityType{Name: activityName, Version: activityVersion},
		Input:        input,
		WorkflowID:   workflowID,
		RunID:        runID,
	})
}

// EnqueueDecisionTaskInternal seeds a decision task in a task list for testing.
func (b *InMemoryBackend) EnqueueDecisionTaskInternal(domain, taskList, workflowID, runID string) {
	b.mu.Lock("EnqueueDecisionTaskInternal")
	defer b.mu.Unlock()

	key := domain + ":" + taskList
	b.decisionQueues[key] = append(b.decisionQueues[key], &DecisionTask{
		WorkflowID: workflowID,
		RunID:      runID,
	})
}

// AddWorkflowTypeInternal seeds a workflow type directly for testing.
func (b *InMemoryBackend) AddWorkflowTypeInternal(domain, name, version, status string) {
	b.mu.Lock("AddWorkflowTypeInternal")
	defer b.mu.Unlock()

	b.workflows.Put(&WorkflowType{Domain: domain, Name: name, Version: version, Status: status})
}

// AddActivityTypeInternal seeds an activity type directly for testing.
func (b *InMemoryBackend) AddActivityTypeInternal(domain, name, version, status string) {
	b.mu.Lock("AddActivityTypeInternal")
	defer b.mu.Unlock()

	b.activities.Put(&ActivityType{Domain: domain, Name: name, Version: version, Status: status})
}

// enqueueDecisionTaskLocked adds a decision task for one specific run's task
// list. Caller must hold the write lock. Records DecisionTaskScheduled
// (DecisionTaskScheduledEventAttributes requires taskList) so PollForDecisionTask's
// DecisionTaskStarted and RespondDecisionTaskCompleted's DecisionTaskCompleted --
// both of which require a scheduledEventId back-reference -- have a real event to
// point to, mirroring the ActivityTaskScheduled/Started/Completed chain in
// activity_tasks.go.
func (b *InMemoryBackend) enqueueDecisionTaskLocked(domain, workflowID, runID string) {
	exec, ok := b.executions.Get(executionKey(domain, workflowID, runID))
	if !ok || exec.TaskList == "" {
		return
	}
	key := domain + ":" + exec.TaskList
	scheduledEventID := b.appendHistoryEventLocked(
		domain, workflowID, exec.RunID, "DecisionTaskScheduled", map[string]any{
			eventAttrKey("DecisionTaskScheduled"): map[string]any{
				attrTaskList: map[string]any{attrName: exec.TaskList},
			},
		})
	b.decisionQueues[key] = append(b.decisionQueues[key], &DecisionTask{
		WorkflowID:       workflowID,
		RunID:            exec.RunID,
		ScheduledEventID: scheduledEventID,
	})
}
