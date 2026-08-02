package swf

import (
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
)

// ExecutionFilter holds optional filters for counting/listing executions.
type ExecutionFilter struct {
	OldestDate          *time.Time
	LatestDate          *time.Time
	CloseOldestDate     *time.Time
	CloseLatestDate     *time.Time
	WorkflowID          string
	WorkflowTypeName    string
	WorkflowTypeVersion string
	Tag                 string
	CloseStatus         string
}

func (f ExecutionFilter) matchOpen(e *WorkflowExecution) bool {
	if e.Status != statusRunning {
		return false
	}
	if f.WorkflowID != "" && e.WorkflowID != f.WorkflowID {
		return false
	}
	if f.WorkflowTypeName != "" && e.WorkflowTypeName != f.WorkflowTypeName {
		return false
	}
	if f.WorkflowTypeVersion != "" && e.WorkflowTypeVersion != f.WorkflowTypeVersion {
		return false
	}
	if f.Tag != "" && !slices.Contains(e.TagList, f.Tag) {
		return false
	}
	st := time.Unix(int64(e.StartTimestamp), 0)
	if f.OldestDate != nil && st.Before(*f.OldestDate) {
		return false
	}
	if f.LatestDate != nil && st.After(*f.LatestDate) {
		return false
	}

	return true
}

// matchClosed reports whether a non-RUNNING execution satisfies every filter
// dimension. Each dimension is evaluated by its own predicate; matchClosed
// short-circuits on the first predicate that fails, in the same order the
// checks used to appear inline.
func (f ExecutionFilter) matchClosed(e *WorkflowExecution) bool {
	if e.Status == statusRunning {
		return false
	}

	return f.matchClosedIdentity(e) &&
		f.matchClosedStatus(e) &&
		f.matchStartRange(e) &&
		f.matchCloseRange(e)
}

// matchClosedIdentity checks the workflow ID/type/tag filter dimensions.
func (f ExecutionFilter) matchClosedIdentity(e *WorkflowExecution) bool {
	if f.WorkflowID != "" && e.WorkflowID != f.WorkflowID {
		return false
	}
	if f.WorkflowTypeName != "" && e.WorkflowTypeName != f.WorkflowTypeName {
		return false
	}
	if f.WorkflowTypeVersion != "" && e.WorkflowTypeVersion != f.WorkflowTypeVersion {
		return false
	}
	if f.Tag != "" && !slices.Contains(e.TagList, f.Tag) {
		return false
	}

	return true
}

// matchClosedStatus checks the CloseStatus filter dimension.
func (f ExecutionFilter) matchClosedStatus(e *WorkflowExecution) bool {
	return f.CloseStatus == "" || e.CloseStatus == f.CloseStatus
}

// matchStartRange checks the OldestDate/LatestDate (start time) filter dimension.
func (f ExecutionFilter) matchStartRange(e *WorkflowExecution) bool {
	if f.OldestDate == nil && f.LatestDate == nil {
		return true
	}
	st := time.Unix(int64(e.StartTimestamp), 0)
	if f.OldestDate != nil && st.Before(*f.OldestDate) {
		return false
	}
	if f.LatestDate != nil && st.After(*f.LatestDate) {
		return false
	}

	return true
}

// matchCloseRange checks the CloseOldestDate/CloseLatestDate filter dimension.
func (f ExecutionFilter) matchCloseRange(e *WorkflowExecution) bool {
	if f.CloseOldestDate == nil && f.CloseLatestDate == nil {
		return true
	}
	if e.CloseTimestamp == 0 {
		return false
	}
	ct := time.Unix(int64(e.CloseTimestamp), 0)
	if f.CloseOldestDate != nil && ct.Before(*f.CloseOldestDate) {
		return false
	}
	if f.CloseLatestDate != nil && ct.After(*f.CloseLatestDate) {
		return false
	}

	return true
}

// CountOpenWorkflowExecutions counts RUNNING workflow executions in a domain, applying filters.
func (b *InMemoryBackend) CountOpenWorkflowExecutions(domain string, filter ExecutionFilter) int {
	b.mu.RLock("CountOpenWorkflowExecutions")
	defer b.mu.RUnlock()

	count := 0
	for _, e := range b.executionsByDomain.Get(domain) {
		if filter.matchOpen(e) {
			count++
		}
	}

	return count
}

// CountClosedWorkflowExecutions counts non-RUNNING workflow executions in a domain, applying filters.
func (b *InMemoryBackend) CountClosedWorkflowExecutions(domain string, filter ExecutionFilter) int {
	b.mu.RLock("CountClosedWorkflowExecutions")
	defer b.mu.RUnlock()

	count := 0
	for _, e := range b.executionsByDomain.Get(domain) {
		if filter.matchClosed(e) {
			count++
		}
	}

	return count
}

// validateStartWorkflowExecutionInput validates the parameters of a
// StartWorkflowExecution request that don't require backend state (no lock
// needed). Checks run in the exact order callers depend on for which error
// surfaces first.
func validateStartWorkflowExecutionInput(input StartWorkflowExecutionInput) error {
	if input.Domain == "" {
		return fmt.Errorf("%w: domain is required", ErrValidation)
	}
	if input.WorkflowID == "" {
		return fmt.Errorf("%w: workflowId is required", ErrValidation)
	}
	if err := validateChildPolicy(input.ChildPolicy); err != nil {
		return err
	}
	if err := validateDuration(input.ExecutionStartToCloseTimeout); err != nil {
		return err
	}
	if err := validateDuration(input.TaskStartToCloseTimeout); err != nil {
		return err
	}

	return nil
}

// startExecutionDefaults holds the effective (post-WorkflowType-defaulting)
// parameters used to construct a new WorkflowExecution.
type startExecutionDefaults struct {
	taskList    string
	childPolicy string
	execTimeout string
	taskTimeout string
	lambdaRole  string
}

// resolveExecutionDefaultsLocked applies WorkflowType defaults (when a
// workflow type is referenced and registered) on top of the caller-supplied
// input, then falls back to "TERMINATE" for an unset childPolicy. Caller must
// hold the write lock.
func (b *InMemoryBackend) resolveExecutionDefaultsLocked(
	input StartWorkflowExecutionInput,
) (startExecutionDefaults, error) {
	d := startExecutionDefaults{
		taskList:    input.TaskList,
		childPolicy: input.ChildPolicy,
		execTimeout: input.ExecutionStartToCloseTimeout,
		taskTimeout: input.TaskStartToCloseTimeout,
		lambdaRole:  input.LambdaRole,
	}

	if input.WorkflowTypeName != "" {
		wt, err := b.lookupRegisteredWorkflowType(input)
		if err != nil {
			return startExecutionDefaults{}, err
		}
		d = d.withWorkflowTypeDefaults(wt.Defaults)
	}

	if d.childPolicy == "" {
		d.childPolicy = childPolicyTerminate
	}

	return d, nil
}

// lookupRegisteredWorkflowType looks up the WorkflowType referenced by input
// and rejects unregistered or deprecated types. Caller must hold at least the
// read lock.
func (b *InMemoryBackend) lookupRegisteredWorkflowType(input StartWorkflowExecutionInput) (*WorkflowType, error) {
	key := input.Domain + ":" + input.WorkflowTypeName + ":" + input.WorkflowTypeVersion
	wt, ok := b.workflows.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: workflow type %s/%s not found",
			ErrNotFound, input.WorkflowTypeName, input.WorkflowTypeVersion)
	}
	if wt.Status == statusDeprecated {
		return nil, fmt.Errorf("%w: workflow type %s/%s is deprecated",
			ErrTypeDeprecated, input.WorkflowTypeName, input.WorkflowTypeVersion)
	}

	return wt, nil
}

// withWorkflowTypeDefaults returns a copy of d with any unset (empty-string)
// fields filled in from the WorkflowType's registered defaults.
func (d startExecutionDefaults) withWorkflowTypeDefaults(wtd WorkflowTypeDefaults) startExecutionDefaults {
	if d.taskList == "" {
		d.taskList = wtd.DefaultTaskList
	}
	if d.childPolicy == "" {
		d.childPolicy = wtd.DefaultChildPolicy
	}
	if d.execTimeout == "" {
		d.execTimeout = wtd.DefaultExecutionStartToCloseTimeout
	}
	if d.taskTimeout == "" {
		d.taskTimeout = wtd.DefaultTaskStartToCloseTimeout
	}
	if d.lambdaRole == "" {
		d.lambdaRole = wtd.DefaultLambdaRole
	}

	return d
}

// registerExecutionOrderLocked records key in the LRU execution order (for
// new keys only) and evicts the oldest execution once the cache reaches
// maxWorkflowExecutions. Caller must hold the write lock.
func (b *InMemoryBackend) registerExecutionOrderLocked(key string) {
	if b.executions.Has(key) {
		return
	}
	b.executionOrder = append(b.executionOrder, key)
	if len(b.executionOrder) >= maxWorkflowExecutions {
		oldest := b.executionOrder[0]
		b.executionOrder = b.executionOrder[1:]
		b.executions.Delete(oldest)
		delete(b.history, oldest)
	}
}

// childLink identifies the parent execution/decision when a new execution is
// being created as the child of a StartChildWorkflowExecution decision (see
// decision_orchestration.go). nil for a top-level (non-child) execution.
type childLink struct {
	parentWorkflowID       string
	parentRunID            string
	parentInitiatedEventID int64
}

// createExecutionLocked is the shared core behind StartWorkflowExecution,
// ContinueAsNewWorkflowExecution, and StartChildWorkflowExecution: it rejects
// a workflowId that already has an open run, stores the new WorkflowExecution,
// appends its WorkflowExecutionStarted history event, and enqueues its
// initial decision task. continuedFromRunID and parent are both optional and
// mutually exclusive in practice (continuation vs. child-start); pass "" /
// nil when neither applies (the plain StartWorkflowExecution case).
// Caller must hold the write lock.
func (b *InMemoryBackend) createExecutionLocked(
	input StartWorkflowExecutionInput,
	defaults startExecutionDefaults,
	continuedFromRunID string,
	parent *childLink,
) (*WorkflowExecution, error) {
	key := input.Domain + ":" + input.WorkflowID

	if existing, exists := b.executions.Get(key); exists && existing.Status == statusRunning {
		return nil, fmt.Errorf("%w: %s", ErrWorkflowAlreadyStarted, input.WorkflowID)
	}

	b.registerExecutionOrderLocked(key)

	runID := input.RunID
	if runID == "" {
		runID = uuid.New().String()
	}

	now := float64(time.Now().UnixMilli()) / milliDivisor
	exec := &WorkflowExecution{
		Domain:                       input.Domain,
		WorkflowID:                   input.WorkflowID,
		RunID:                        runID,
		Status:                       statusRunning,
		StartTimestamp:               now,
		WorkflowTypeName:             input.WorkflowTypeName,
		WorkflowTypeVersion:          input.WorkflowTypeVersion,
		TaskList:                     defaults.taskList,
		Input:                        input.Input,
		TagList:                      input.TagList,
		ChildPolicy:                  defaults.childPolicy,
		LambdaRole:                   defaults.lambdaRole,
		ExecutionStartToCloseTimeout: defaults.execTimeout,
		TaskStartToCloseTimeout:      defaults.taskTimeout,
		TaskPriority:                 input.TaskPriority,
	}
	if parent != nil {
		exec.ParentWorkflowID = parent.parentWorkflowID
		exec.ParentRunID = parent.parentRunID
		exec.ParentInitiatedEventID = parent.parentInitiatedEventID
	}
	b.executions.Put(exec)

	startedAttrs := map[string]any{
		attrInput:       input.Input,
		attrChildPolicy: defaults.childPolicy,
		attrTaskList:    map[string]any{attrName: defaults.taskList},
		attrWorkflowType: map[string]any{
			attrName:    input.WorkflowTypeName,
			attrVersion: input.WorkflowTypeVersion,
		},
		attrExecToCloseTO: defaults.execTimeout,
		attrTaskToCloseTO: defaults.taskTimeout,
		attrLambdaRole:    defaults.lambdaRole,
		attrTagList:       input.TagList,
	}
	if continuedFromRunID != "" {
		startedAttrs["continuedExecutionRunId"] = continuedFromRunID
	}
	if parent != nil {
		startedAttrs["parentInitiatedEventId"] = parent.parentInitiatedEventID
		startedAttrs["parentWorkflowExecution"] = map[string]any{
			attrWorkflowID: parent.parentWorkflowID,
			attrRunID:      parent.parentRunID,
		}
	}
	b.appendHistoryEventLocked(input.Domain, input.WorkflowID, "WorkflowExecutionStarted", map[string]any{
		eventAttrKey("WorkflowExecutionStarted"): startedAttrs,
	})

	// Real AWS schedules the first decision task immediately after starting an
	// execution, so a decider can PollForDecisionTask and see the
	// WorkflowExecutionStarted event without any other event (signal, cancel
	// request, activity completion) first triggering one. Without this, a
	// freshly started workflow with no other stimulus never gets its first
	// decision task and stays OPEN forever.
	b.enqueueDecisionTaskLocked(input.Domain, input.WorkflowID)

	cp := *exec

	return &cp, nil
}

// StartWorkflowExecution starts a new workflow execution.
// It validates that the referenced WorkflowType exists and is REGISTERED.
func (b *InMemoryBackend) StartWorkflowExecution(
	input StartWorkflowExecutionInput,
) (*WorkflowExecution, error) {
	if err := validateStartWorkflowExecutionInput(input); err != nil {
		return nil, err
	}

	b.mu.Lock("StartWorkflowExecution")
	defer b.mu.Unlock()

	if !b.domains.Has(input.Domain) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, input.Domain)
	}

	defaults, err := b.resolveExecutionDefaultsLocked(input)
	if err != nil {
		return nil, err
	}

	return b.createExecutionLocked(input, defaults, "", nil)
}

// TerminateWorkflowExecution terminates a running workflow execution.
// runID is optional; if provided, it must match. reason and details are
// stored in history. childPolicyOverride, if non-empty, is real SWF's
// per-call override of the child policy applied to this execution's open
// child executions -- it takes precedence over the policy stored on exec
// (set at StartWorkflowExecution time) for this call only, and never
// mutates that stored value. An empty override falls back to the stored
// policy, exactly as if the field had been omitted on the wire.
func (b *InMemoryBackend) TerminateWorkflowExecution(
	domain, workflowID, runID, reason, details, childPolicyOverride string,
) error {
	b.mu.Lock("TerminateWorkflowExecution")
	defer b.mu.Unlock()

	if err := validateChildPolicy(childPolicyOverride); err != nil {
		return err
	}

	key := domain + ":" + workflowID
	exec, ok := b.executions.Get(key)
	if !ok {
		return fmt.Errorf("%w: execution %s/%s not found", ErrNotFound, domain, workflowID)
	}
	if exec.Status != statusRunning {
		return fmt.Errorf("%w: execution %s/%s is not open", ErrNotFound, domain, workflowID)
	}
	if runID != "" && exec.RunID != runID {
		return fmt.Errorf(
			"%w: runId %s does not match current run %s",
			ErrNotFound,
			runID,
			exec.RunID,
		)
	}

	effectivePolicy := exec.ChildPolicy
	if childPolicyOverride != "" {
		effectivePolicy = childPolicyOverride
	}

	b.terminateExecutionLocked(domain, exec, reason, details, causeOperatorInitiated, effectivePolicy)

	return nil
}

// terminateExecutionLocked marks exec terminated, records its
// WorkflowExecutionTerminated history event, notifies exec's own parent (if
// still open) that this child has closed, and cascades policy onto exec's
// own open children per applyChildPolicyLocked. cause is "OPERATOR_INITIATED"
// for a direct TerminateWorkflowExecution call, or "CHILD_POLICY_APPLIED"
// when this termination is itself the result of an ancestor's TERMINATE
// child-policy cascade -- in which case reason/details are empty, matching
// real AWS (a cascade-triggered termination carries no operator-supplied
// reason). Caller must hold the write lock.
func (b *InMemoryBackend) terminateExecutionLocked(
	domain string, exec *WorkflowExecution, reason, details, cause, policy string,
) {
	exec.Status = statusTerminated
	exec.CloseStatus = statusTerminated
	exec.CloseTimestamp = float64(time.Now().UnixMilli()) / milliDivisor

	attrKey := eventAttrKey("WorkflowExecutionTerminated")
	attrs := map[string]any{
		attrKey: map[string]any{
			attrReason:      reason,
			attrDetails:     details,
			attrCause:       cause,
			attrChildPolicy: policy,
		},
	}
	b.appendHistoryEventLocked(domain, exec.WorkflowID, "WorkflowExecutionTerminated", attrs)
	b.propagateChildClosureLocked(domain, exec, "ChildWorkflowExecutionTerminated", nil)
	b.applyChildPolicyLocked(domain, exec, policy)
}

// applyChildPolicyLocked cascades policy from a just-closed parent execution
// onto its still-open child executions (those with ParentWorkflowID/
// ParentRunID pointing at parent), matching real SWF's automatic
// child-closure handling:
//
//   - TERMINATE: each open child is itself terminated (cause
//     CHILD_POLICY_APPLIED), which in turn cascades that child's own stored
//     ChildPolicy onto its children, and so on down the tree.
//   - REQUEST_CANCEL: a WorkflowExecutionCancelRequested event (cause
//     CHILD_POLICY_APPLIED) is recorded on each open child and it is
//     enqueued a fresh decision task, exactly as RequestCancelWorkflowExecution
//     does for a direct call -- it is up to that child's own decider to act.
//   - ABANDON, or any other value: no action; children continue running
//     untouched.
//
// The child snapshot is taken before mutating anything so that terminating
// (and thereby further cascading into) one child cannot perturb iteration
// over its siblings. Caller must hold the write lock.
func (b *InMemoryBackend) applyChildPolicyLocked(domain string, parent *WorkflowExecution, policy string) {
	var children []*WorkflowExecution
	for _, e := range b.executionsByDomain.Get(domain) {
		if e.Status == statusRunning &&
			e.ParentWorkflowID == parent.WorkflowID &&
			e.ParentRunID == parent.RunID {
			children = append(children, e)
		}
	}

	for _, child := range children {
		switch policy {
		case childPolicyTerminate:
			b.terminateExecutionLocked(domain, child, "", "", causeChildPolicyApplied, child.ChildPolicy)
		case childPolicyRequestCancel:
			b.cascadeCancelRequestLocked(domain, child)
		}
	}
}

// cascadeCancelRequestLocked applies a REQUEST_CANCEL child-policy cascade
// to a single open child: sets CancelRequested, records a
// WorkflowExecutionCancelRequested event with cause CHILD_POLICY_APPLIED --
// the only cause value real SWF's WorkflowExecutionCancelRequestedCause enum
// defines, reserved for exactly this automatic-cascade case -- and enqueues
// the child a fresh decision task so its decider learns of the request.
// Caller must hold the write lock.
//
// NOTE: RequestCancelWorkflowExecution (a direct, operator-initiated call)
// separately stamps this same event with cause "OPERATOR_INITIATED", which
// is not a value the real enum defines; that mismatch predates this change,
// is unrelated to child-policy threading, and is left alone here.
func (b *InMemoryBackend) cascadeCancelRequestLocked(domain string, exec *WorkflowExecution) {
	exec.CancelRequested = true

	attrKey := eventAttrKey("WorkflowExecutionCancelRequested")
	attrs := map[string]any{
		attrKey: map[string]any{
			attrCause: causeChildPolicyApplied,
		},
	}
	b.appendHistoryEventLocked(domain, exec.WorkflowID, "WorkflowExecutionCancelRequested", attrs)

	if exec.TaskList != "" {
		qkey := domain + ":" + exec.TaskList
		b.decisionQueues[qkey] = append(b.decisionQueues[qkey], &DecisionTask{
			WorkflowID: exec.WorkflowID,
			RunID:      exec.RunID,
		})
	}
}

// DescribeWorkflowExecution returns a workflow execution.
func (b *InMemoryBackend) DescribeWorkflowExecution(
	domain, workflowID string,
) (*WorkflowExecution, error) {
	b.mu.RLock("DescribeWorkflowExecution")
	defer b.mu.RUnlock()

	key := domain + ":" + workflowID
	exec, ok := b.executions.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: execution %s/%s not found", ErrNotFound, domain, workflowID)
	}
	cp := *exec

	return &cp, nil
}

// openCountsLocked returns open activity/decision/timer/child-workflow counts
// for an execution. Caller must hold at least RLock.
func (b *InMemoryBackend) openCountsLocked(domain, workflowID string) map[string]int {
	activityCount := 0
	for _, rec := range b.activeActivityTasks.All() {
		if rec.Domain == domain && rec.WorkflowID == workflowID {
			activityCount++
		}
	}
	decisionCount := 0
	for _, q := range b.decisionQueues {
		for _, t := range q {
			if t.WorkflowID == workflowID {
				decisionCount++
			}
		}
	}

	timerCount := 0
	if exec, ok := b.executions.Get(domain + ":" + workflowID); ok {
		timerCount = len(exec.OpenTimerIDs)
	}

	childCount := 0
	if exec, ok := b.executions.Get(domain + ":" + workflowID); ok {
		for _, e := range b.executionsByDomain.Get(domain) {
			if e.Status == statusRunning && e.ParentWorkflowID == workflowID && e.ParentRunID == exec.RunID {
				childCount++
			}
		}
	}

	return map[string]int{
		"openActivityTasks":           activityCount,
		"openDecisionTasks":           decisionCount,
		"openTimers":                  timerCount,
		"openChildWorkflowExecutions": childCount,
	}
}

// ListOpenWorkflowExecutions returns all running executions in a domain matching the filter.
func (b *InMemoryBackend) ListOpenWorkflowExecutions(
	domain string,
	filter ExecutionFilter,
) []WorkflowExecution {
	b.mu.RLock("ListOpenWorkflowExecutions")
	defer b.mu.RUnlock()

	byDomain := b.executionsByDomain.Get(domain)
	out := make([]WorkflowExecution, 0, len(byDomain))

	for _, e := range byDomain {
		if filter.matchOpen(e) {
			out = append(out, *e)
		}
	}

	return out
}

// ListClosedWorkflowExecutions returns all closed executions in a domain matching the filter.
func (b *InMemoryBackend) ListClosedWorkflowExecutions(
	domain string,
	filter ExecutionFilter,
) []WorkflowExecution {
	b.mu.RLock("ListClosedWorkflowExecutions")
	defer b.mu.RUnlock()

	byDomain := b.executionsByDomain.Get(domain)
	out := make([]WorkflowExecution, 0, len(byDomain))

	for _, e := range byDomain {
		if filter.matchClosed(e) {
			out = append(out, *e)
		}
	}

	return out
}

// RequestCancelWorkflowExecution requests cancellation of a running execution.
// runID is optional; if provided, it must match.
func (b *InMemoryBackend) RequestCancelWorkflowExecution(domain, workflowID, runID string) error {
	b.mu.Lock("RequestCancelWorkflowExecution")
	defer b.mu.Unlock()

	key := domain + ":" + workflowID
	exec, ok := b.executions.Get(key)
	if !ok {
		return fmt.Errorf("%w: execution %s/%s not found", ErrNotFound, domain, workflowID)
	}
	// Real AWS: "If the specified workflow execution isn't open, this method
	// fails with UnknownResource." (see RequestCancelWorkflowExecution doc) --
	// not ValidationException, which isn't even in this op's fault model.
	if exec.Status != statusRunning {
		return fmt.Errorf("%w: execution %s/%s is not open", ErrNotFound, domain, workflowID)
	}
	if runID != "" && exec.RunID != runID {
		return fmt.Errorf(
			"%w: runId %s does not match current run %s",
			ErrNotFound,
			runID,
			exec.RunID,
		)
	}

	exec.CancelRequested = true

	attrKey := eventAttrKey("WorkflowExecutionCancelRequested")
	attrs := map[string]any{
		attrKey: map[string]any{
			attrCause: causeOperatorInitiated,
		},
	}
	b.appendHistoryEventLocked(domain, workflowID, "WorkflowExecutionCancelRequested", attrs)

	// Enqueue a decision task so the workflow decider can react.
	if exec.TaskList != "" {
		qkey := domain + ":" + exec.TaskList
		b.decisionQueues[qkey] = append(b.decisionQueues[qkey], &DecisionTask{
			WorkflowID: workflowID,
			RunID:      exec.RunID,
		})
	}

	return nil
}
