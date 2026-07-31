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
type InMemoryBackend struct {
	registry            *store.Registry
	domains             *store.Table[Domain]
	workflows           *store.Table[WorkflowType] // key: domain+":"+name+":"+version
	workflowsByDomain   *store.Index[WorkflowType]
	activities          *store.Table[ActivityType] // key: domain+":"+name+":"+version
	activitiesByDomain  *store.Index[ActivityType]
	executions          *store.Table[WorkflowExecution] // key: domain+":"+workflowID
	executionsByDomain  *store.Index[WorkflowExecution]
	activeActivityTasks *store.Table[activeActivityTaskRecord] // key: taskToken
	activeDecisionTasks *store.Table[activeDecisionTaskRecord] // key: taskToken
	history             map[string][]HistoryEvent              // key: domain+":"+workflowID
	activityQueues      map[string][]*ActivityTask             // key: domain+":"+taskList
	decisionQueues      map[string][]*DecisionTask             // key: domain+":"+taskList
	tags                map[string]map[string]string           // key: resourceARN
	mu                  *lockmetrics.RWMutex
	executionOrder      []string // FIFO order of execution keys for eviction
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

// appendHistoryEventLocked appends a history event for a workflow execution.
// attrs is the event-type-specific attributes map; pass nil if none.
// Caller must hold the write lock.
func (b *InMemoryBackend) appendHistoryEventLocked(
	domain, workflowID, eventType string,
	attrs map[string]any,
) int64 {
	key := domain + ":" + workflowID
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

// enqueueDecisionTaskLocked adds a decision task for the execution's task list.
// Caller must hold the write lock.
func (b *InMemoryBackend) enqueueDecisionTaskLocked(domain, workflowID string) {
	exec, ok := b.executions.Get(domain + ":" + workflowID)
	if !ok || exec.TaskList == "" {
		return
	}
	key := domain + ":" + exec.TaskList
	b.decisionQueues[key] = append(b.decisionQueues[key], &DecisionTask{
		WorkflowID: workflowID,
		RunID:      exec.RunID,
	})
}
