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

//nolint:gocognit,cyclop // sequential date/filter checks are inherently complex
func (f ExecutionFilter) matchClosed(
	e *WorkflowExecution,
) bool {
	if e.Status == statusRunning {
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
	if f.CloseStatus != "" && e.CloseStatus != f.CloseStatus {
		return false
	}
	if f.OldestDate != nil || f.LatestDate != nil {
		st := time.Unix(int64(e.StartTimestamp), 0)
		if f.OldestDate != nil && st.Before(*f.OldestDate) {
			return false
		}
		if f.LatestDate != nil && st.After(*f.LatestDate) {
			return false
		}
	}
	if f.CloseOldestDate != nil || f.CloseLatestDate != nil {
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

// StartWorkflowExecution starts a new workflow execution.
// It validates that the referenced WorkflowType exists and is REGISTERED.
//
//nolint:gocognit,cyclop,nestif,funlen // sequential parameter validation is inherently complex
func (b *InMemoryBackend) StartWorkflowExecution(
	input StartWorkflowExecutionInput,
) (*WorkflowExecution, error) {
	if input.Domain == "" {
		return nil, fmt.Errorf("%w: domain is required", ErrValidation)
	}
	if input.WorkflowID == "" {
		return nil, fmt.Errorf("%w: workflowId is required", ErrValidation)
	}
	if err := validateChildPolicy(input.ChildPolicy); err != nil {
		return nil, err
	}
	if err := validateDuration(input.ExecutionStartToCloseTimeout); err != nil {
		return nil, err
	}
	if err := validateDuration(input.TaskStartToCloseTimeout); err != nil {
		return nil, err
	}

	b.mu.Lock("StartWorkflowExecution")
	defer b.mu.Unlock()

	if !b.domains.Has(input.Domain) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, input.Domain)
	}

	// Validate workflow type if provided.
	taskList := input.TaskList
	childPolicy := input.ChildPolicy
	execTimeout := input.ExecutionStartToCloseTimeout
	taskTimeout := input.TaskStartToCloseTimeout
	lambdaRole := input.LambdaRole

	if input.WorkflowTypeName != "" {
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
		// Apply type defaults when not overridden.
		if taskList == "" {
			taskList = wt.Defaults.DefaultTaskList
		}
		if childPolicy == "" {
			childPolicy = wt.Defaults.DefaultChildPolicy
		}
		if execTimeout == "" {
			execTimeout = wt.Defaults.DefaultExecutionStartToCloseTimeout
		}
		if taskTimeout == "" {
			taskTimeout = wt.Defaults.DefaultTaskStartToCloseTimeout
		}
		if lambdaRole == "" {
			lambdaRole = wt.Defaults.DefaultLambdaRole
		}
	}

	if childPolicy == "" {
		childPolicy = "TERMINATE"
	}

	runID := input.RunID
	if runID == "" {
		runID = uuid.New().String()
	}

	key := input.Domain + ":" + input.WorkflowID

	// Reject if there is already an open (RUNNING) execution for this workflowId.
	if existing, exists := b.executions.Get(key); exists && existing.Status == statusRunning {
		return nil, fmt.Errorf("%w: %s", ErrWorkflowAlreadyStarted, input.WorkflowID)
	}

	if !b.executions.Has(key) {
		b.executionOrder = append(b.executionOrder, key)
		if len(b.executionOrder) >= maxWorkflowExecutions {
			oldest := b.executionOrder[0]
			b.executionOrder = b.executionOrder[1:]
			b.executions.Delete(oldest)
			delete(b.history, oldest)
		}
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
		TaskList:                     taskList,
		Input:                        input.Input,
		TagList:                      input.TagList,
		ChildPolicy:                  childPolicy,
		LambdaRole:                   lambdaRole,
		ExecutionStartToCloseTimeout: execTimeout,
		TaskStartToCloseTimeout:      taskTimeout,
		TaskPriority:                 input.TaskPriority,
	}
	b.executions.Put(exec)

	attrKey := eventAttrKey("WorkflowExecutionStarted")
	attrs := map[string]any{
		attrKey: map[string]any{
			attrInput:     input.Input,
			"childPolicy": childPolicy,
			"taskList":    map[string]any{attrName: taskList},
			"workflowType": map[string]any{
				attrName:  input.WorkflowTypeName,
				"version": input.WorkflowTypeVersion,
			},
			"executionStartToCloseTimeout": execTimeout,
			"taskStartToCloseTimeout":      taskTimeout,
			"lambdaRole":                   lambdaRole,
			"tagList":                      input.TagList,
		},
	}
	b.appendHistoryEventLocked(input.Domain, input.WorkflowID, "WorkflowExecutionStarted", attrs)

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

// TerminateWorkflowExecution terminates a running workflow execution.
// runID is optional; if provided, it must match. reason and details are stored in history.
func (b *InMemoryBackend) TerminateWorkflowExecution(
	domain, workflowID, runID, reason, details string,
) error {
	b.mu.Lock("TerminateWorkflowExecution")
	defer b.mu.Unlock()

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

	exec.Status = statusTerminated
	exec.CloseStatus = statusTerminated
	exec.CloseTimestamp = float64(time.Now().UnixMilli()) / milliDivisor

	attrKey := eventAttrKey("WorkflowExecutionTerminated")
	attrs := map[string]any{
		attrKey: map[string]any{
			attrReason:    reason,
			attrDetails:   details,
			"cause":       "OPERATOR_INITIATED",
			"childPolicy": exec.ChildPolicy,
		},
	}
	b.appendHistoryEventLocked(domain, workflowID, "WorkflowExecutionTerminated", attrs)

	return nil
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

// openCountsLocked returns open activity/decision/timer counts for an execution.
// Caller must hold at least RLock.
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

	return map[string]int{
		"openActivityTasks":           activityCount,
		"openDecisionTasks":           decisionCount,
		"openTimers":                  0,
		"openChildWorkflowExecutions": 0,
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
			"cause": "OPERATOR_INITIATED",
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
