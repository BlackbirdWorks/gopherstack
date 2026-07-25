package iot

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"
)

// ---------------------------------------------------------------------------
// Audit mitigation-action tasks (StartAuditMitigationActionsTask and friends)
// ---------------------------------------------------------------------------

// AuditMitigationActionsTaskTarget identifies the audit findings that a
// StartAuditMitigationActionsTask task's mitigation actions are applied to.
type AuditMitigationActionsTaskTarget struct {
	AuditCheckToReasonCodeFilter map[string][]string `json:"auditCheckToReasonCodeFilter,omitempty"`
	AuditTaskID                  string              `json:"auditTaskId,omitempty"`
	FindingIDs                   []string            `json:"findingIds,omitempty"`
}

func cloneAuditMitigationTarget(t *AuditMitigationActionsTaskTarget) *AuditMitigationActionsTaskTarget {
	if t == nil {
		return nil
	}
	cp := *t
	cp.FindingIDs = append([]string(nil), t.FindingIDs...)
	cp.AuditCheckToReasonCodeFilter = make(map[string][]string, len(t.AuditCheckToReasonCodeFilter))
	for k, v := range t.AuditCheckToReasonCodeFilter {
		cp.AuditCheckToReasonCodeFilter[k] = append([]string(nil), v...)
	}

	return &cp
}

// TaskStatisticsForAuditCheck aggregates the mitigation-action execution counts
// for a single audit check within an AuditMitigationTask.
type TaskStatisticsForAuditCheck struct {
	TotalFindingsCount     int64 `json:"totalFindingsCount"`
	FailedFindingsCount    int64 `json:"failedFindingsCount"`
	SucceededFindingsCount int64 `json:"succeededFindingsCount"`
	SkippedFindingsCount   int64 `json:"skippedFindingsCount"`
	CanceledFindingsCount  int64 `json:"canceledFindingsCount"`
}

// AuditMitigationTask represents a task started by StartAuditMitigationActionsTask.
type AuditMitigationTask struct {
	Target                     *AuditMitigationActionsTaskTarget       `json:"target,omitempty"`
	AuditCheckToActionsMapping map[string][]string                     `json:"auditCheckToActionsMapping,omitempty"`
	TaskStatistics             map[string]*TaskStatisticsForAuditCheck `json:"taskStatistics,omitempty"`
	TaskID                     string                                  `json:"taskId"`
	TaskStatus                 string                                  `json:"taskStatus"`
	StartTime                  float64                                 `json:"startTime,omitempty"`
	EndTime                    float64                                 `json:"endTime,omitempty"`
}

func cloneAuditMitigationTask(t *AuditMitigationTask) *AuditMitigationTask {
	cp := *t
	cp.Target = cloneAuditMitigationTarget(t.Target)
	cp.AuditCheckToActionsMapping = make(map[string][]string, len(t.AuditCheckToActionsMapping))
	for k, v := range t.AuditCheckToActionsMapping {
		cp.AuditCheckToActionsMapping[k] = append([]string(nil), v...)
	}
	cp.TaskStatistics = make(map[string]*TaskStatisticsForAuditCheck, len(t.TaskStatistics))
	for k, v := range t.TaskStatistics {
		s := *v
		cp.TaskStatistics[k] = &s
	}

	return &cp
}

// AuditMitigationActionExecution represents one mitigation action applied (or to
// be applied) to one audit finding as part of an AuditMitigationTask.
type AuditMitigationActionExecution struct {
	TaskID     string  `json:"taskId"`
	FindingID  string  `json:"findingId"`
	ActionName string  `json:"actionName"`
	ActionID   string  `json:"actionId,omitempty"`
	Status     string  `json:"status"`
	ErrorCode  string  `json:"errorCode,omitempty"`
	Message    string  `json:"message,omitempty"`
	StartTime  float64 `json:"startTime,omitempty"`
	EndTime    float64 `json:"endTime,omitempty"`
}

func cloneAuditMitigationExecution(e *AuditMitigationActionExecution) *AuditMitigationActionExecution {
	cp := *e

	return &cp
}

// StartAuditMitigationActionsTaskInput is the input for StartAuditMitigationActionsTask.
type StartAuditMitigationActionsTaskInput struct {
	AuditCheckToActionsMapping map[string][]string
	Target                     *AuditMitigationActionsTaskTarget
	TaskID                     string
}

// auditMitigationFindingIDs resolves the set of stored audit finding IDs that
// a task target applies to. Must be called with b.mu held.
//
// Real AWS IoT's AuditMitigationActionsTaskTarget lets auditTaskId and
// auditCheckToReasonCodeFilter be combined (e.g. "this audit's findings for
// check X with reason code Y"), not just used interchangeably -- confirmed
// against the target field docs in types.AuditMitigationActionsTaskTarget.
// This previously matched on AuditTaskID alone whenever it was set (a
// switch's first matching case wins), silently ignoring
// AuditCheckToReasonCodeFilter even when both were populated, and matched
// AuditCheckToReasonCodeFilter by check name only -- ignoring the actual
// reason-code list, which real AWS filters on when non-empty (an empty list
// for a check means "any reason code for that check"). Both are real,
// previously-undiscovered target-resolution bugs; fixed here.
func (b *InMemoryBackend) auditMitigationFindingIDs(target *AuditMitigationActionsTaskTarget) []string {
	if target == nil {
		return nil
	}
	if len(target.FindingIDs) > 0 {
		return append([]string(nil), target.FindingIDs...)
	}

	var ids []string
	for _, f := range b.auditFindings.All() {
		if target.AuditTaskID != "" && f.TaskID != target.AuditTaskID {
			continue
		}

		if len(target.AuditCheckToReasonCodeFilter) > 0 {
			codes, ok := target.AuditCheckToReasonCodeFilter[f.CheckName]
			if !ok {
				continue
			}
			if len(codes) > 0 && !slices.Contains(codes, f.ReasonForNonComplianceCode) {
				continue
			}
		} else if target.AuditTaskID == "" {
			// Neither a task nor a check/reason-code filter was given (and
			// FindingIDs was empty) -- there is nothing to resolve against.
			continue
		}

		ids = append(ids, f.FindingID)
	}
	sort.Strings(ids)

	return ids
}

// StartAuditMitigationActionsTask creates a task that applies mitigation actions
// to the audit findings identified by input.Target, seeding one execution record
// per (finding, action) pair resolved from AuditCheckToActionsMapping.
func (b *InMemoryBackend) StartAuditMitigationActionsTask(
	input *StartAuditMitigationActionsTaskInput,
) (*AuditMitigationTask, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if input.TaskID == "" {
		return nil, fmt.Errorf("%w: taskId is required", ErrValidation)
	}
	if b.auditMitigationTaskObjects.Has(input.TaskID) {
		return nil, fmt.Errorf(
			"audit mitigation actions task %q already exists: %w",
			input.TaskID,
			ErrAlreadyExists,
		)
	}
	if input.Target == nil || len(input.AuditCheckToActionsMapping) == 0 {
		return nil, fmt.Errorf("%w: target and auditCheckToActionsMapping are required", ErrValidation)
	}

	now := float64(time.Now().Unix())
	task := &AuditMitigationTask{
		TaskID:                     input.TaskID,
		TaskStatus:                 string(JobStatusInProgress),
		Target:                     cloneAuditMitigationTarget(input.Target),
		AuditCheckToActionsMapping: input.AuditCheckToActionsMapping,
		StartTime:                  now,
		TaskStatistics:             map[string]*TaskStatisticsForAuditCheck{},
	}

	findingIDs := b.auditMitigationFindingIDs(input.Target)
	executions := make([]*AuditMitigationActionExecution, 0, len(findingIDs))

	for _, findingID := range findingIDs {
		checkName := ""
		if f, ok := b.auditFindings.Get(findingID); ok {
			checkName = f.CheckName
		}

		stats := task.TaskStatistics[checkName]
		if stats == nil {
			stats = &TaskStatisticsForAuditCheck{}
			task.TaskStatistics[checkName] = stats
		}
		stats.TotalFindingsCount++

		for _, actionName := range input.AuditCheckToActionsMapping[checkName] {
			actionID := ""
			if ma, ok := b.mitigationActions.Get(actionName); ok {
				actionID = ma.ActionID
			}
			executions = append(executions, &AuditMitigationActionExecution{
				TaskID:     input.TaskID,
				FindingID:  findingID,
				ActionName: actionName,
				ActionID:   actionID,
				Status:     string(JobStatusInProgress),
				StartTime:  now,
			})
		}
	}

	b.auditMitigationTaskObjects.Put(task)
	b.auditMitigationExecutions[input.TaskID] = executions
	// Keep the legacy status map (read by CancelAuditMitigationActionsTask's
	// original implementation) in sync with the rich task object.
	b.auditMitigationTasks[input.TaskID] = task.TaskStatus

	return cloneAuditMitigationTask(task), nil
}

// DescribeAuditMitigationActionsTask returns a previously started audit
// mitigation actions task, or ErrResourceNotFound if taskID is unknown.
func (b *InMemoryBackend) DescribeAuditMitigationActionsTask(taskID string) (*AuditMitigationTask, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	t, ok := b.auditMitigationTaskObjects.Get(taskID)
	if !ok {
		return nil, fmt.Errorf("audit mitigation actions task %q not found: %w", taskID, ErrResourceNotFound)
	}

	return cloneAuditMitigationTask(t), nil
}

// ListAuditMitigationActionsTasks returns stored audit mitigation actions tasks,
// optionally filtered by the audit task they target and/or their current status.
func (b *InMemoryBackend) ListAuditMitigationActionsTasks(auditTaskID, taskStatus string) []*AuditMitigationTask {
	b.mu.RLock()
	defer b.mu.RUnlock()

	items := b.auditMitigationTaskObjects.Snapshot()
	out := make([]*AuditMitigationTask, 0, len(items))

	for _, v := range items {
		t := v
		if auditTaskID != "" && (t.Target == nil || t.Target.AuditTaskID != auditTaskID) {
			continue
		}
		if taskStatus != "" && t.TaskStatus != taskStatus {
			continue
		}
		out = append(out, cloneAuditMitigationTask(t))
	}

	return out
}

// ListAuditMitigationActionsExecutions returns the mitigation-action execution
// records for a task, optionally filtered by finding ID and/or execution status.
func (b *InMemoryBackend) ListAuditMitigationActionsExecutions(
	taskID, findingID, actionStatus string,
) []*AuditMitigationActionExecution {
	b.mu.RLock()
	defer b.mu.RUnlock()

	taskIDs := []string{taskID}
	if taskID == "" {
		taskIDs = sortedKeys(b.auditMitigationExecutions)
	}

	out := []*AuditMitigationActionExecution{}

	for _, tid := range taskIDs {
		for _, e := range b.auditMitigationExecutions[tid] {
			if findingID != "" && e.FindingID != findingID {
				continue
			}
			if actionStatus != "" && e.Status != actionStatus {
				continue
			}
			out = append(out, cloneAuditMitigationExecution(e))
		}
	}

	return out
}

// ---------------------------------------------------------------------------
// Detect mitigation-action tasks (StartDetectMitigationActionsTask and friends)
// ---------------------------------------------------------------------------

// DetectMitigationActionsTaskTarget identifies the ML Detect violations that a
// StartDetectMitigationActionsTask task's actions are applied to.
type DetectMitigationActionsTaskTarget struct {
	SecurityProfileName string   `json:"securityProfileName,omitempty"`
	BehaviorName        string   `json:"behaviorName,omitempty"`
	ViolationIDs        []string `json:"violationIds,omitempty"`
}

func cloneDetectMitigationTarget(t *DetectMitigationActionsTaskTarget) *DetectMitigationActionsTaskTarget {
	if t == nil {
		return nil
	}
	cp := *t
	cp.ViolationIDs = append([]string(nil), t.ViolationIDs...)

	return &cp
}

// DetectMitigationTaskStatistics aggregates the mitigation-action execution
// counts for a DetectMitigationTask.
type DetectMitigationTaskStatistics struct {
	ActionsExecuted int64 `json:"actionsExecuted"`
	ActionsSkipped  int64 `json:"actionsSkipped"`
	ActionsFailed   int64 `json:"actionsFailed"`
}

// ViolationEventOccurrenceRange specifies the time period during which
// violation events occurred (aws-sdk-go-v2/service/iot/types.
// ViolationEventOccurrenceRange), an optional input to
// StartDetectMitigationActionsTask and echoed back on
// DetectMitigationActionsTaskSummary.
type ViolationEventOccurrenceRange struct {
	StartTime float64 `json:"startTime,omitempty"`
	EndTime   float64 `json:"endTime,omitempty"`
}

// MitigationActionRef is the shape of a mitigation action as embedded in
// DetectMitigationActionsTaskSummary.actionsDefinition (types.MitigationAction,
// confirmed against v1.76.0) -- a narrower shape than
// DescribeMitigationActionOutput's own top-level fields: just
// id/name/roleArn/actionParams, with no arn, actionType, or timestamps.
type MitigationActionRef struct {
	ActionParams map[string]any `json:"actionParams,omitempty"`
	ID           string         `json:"id,omitempty"`
	Name         string         `json:"name,omitempty"`
	RoleARN      string         `json:"roleArn,omitempty"`
}

// DetectMitigationTask represents a task started by StartDetectMitigationActionsTask.
//
// Actions is internal-only storage (json:"-"): real AWS's
// DetectMitigationActionsTaskSummary (confirmed against v1.76.0) has no
// "actions" field at all -- the wire field is "actionsDefinition", a list of
// full MitigationAction objects, not action names. Wire-response builders
// (handler_devicedefender.go's detectMitigationTaskSummaryWire) resolve
// Actions to their MitigationActionRef shape via
// [InMemoryBackend.MitigationActionRefs] rather than ever serializing this
// struct directly. ViolationEventOccurrenceRange keeps a normal json tag
// (unlike Actions) since it needs no such backend-lookup transformation --
// it round-trips as-is, both to the HTTP response and through this table's
// Snapshot/Restore (which marshals the struct directly; a json:"-" field
// would silently be dropped across a snapshot/restore cycle).
type DetectMitigationTask struct {
	Target                        *DetectMitigationActionsTaskTarget `json:"target,omitempty"`
	TaskStatistics                *DetectMitigationTaskStatistics    `json:"taskStatistics,omitempty"`
	ViolationEventOccurrenceRange *ViolationEventOccurrenceRange     `json:"violationEventOccurrenceRange,omitempty"`
	TaskID                        string                             `json:"taskId"`
	TaskStatus                    string                             `json:"taskStatus"`
	Actions                       []string                           `json:"-"`
	StartTime                     float64                            `json:"taskStartTime,omitempty"`
	EndTime                       float64                            `json:"taskEndTime,omitempty"`
	OnlyActiveViolationsIncluded  bool                               `json:"onlyActiveViolationsIncluded"`
	SuppressedAlertsIncluded      bool                               `json:"suppressedAlertsIncluded"`
}

func cloneDetectMitigationTask(t *DetectMitigationTask) *DetectMitigationTask {
	cp := *t
	cp.Target = cloneDetectMitigationTarget(t.Target)
	cp.Actions = append([]string(nil), t.Actions...)

	if t.TaskStatistics != nil {
		s := *t.TaskStatistics
		cp.TaskStatistics = &s
	}
	if t.ViolationEventOccurrenceRange != nil {
		r := *t.ViolationEventOccurrenceRange
		cp.ViolationEventOccurrenceRange = &r
	}

	return &cp
}

// MitigationActionRefs resolves action names to their MitigationActionRef
// wire shape (id/name/roleArn/actionParams), skipping any name that no
// longer has a corresponding MitigationAction (e.g. deleted after the task
// that referenced it was started).
func (b *InMemoryBackend) MitigationActionRefs(names []string) []MitigationActionRef {
	b.mu.RLock()
	defer b.mu.RUnlock()

	refs := make([]MitigationActionRef, 0, len(names))
	for _, name := range names {
		ma, ok := b.mitigationActions.Get(name)
		if !ok {
			continue
		}
		refs = append(refs, MitigationActionRef{
			ID:           ma.ActionID,
			Name:         ma.ActionName,
			RoleARN:      ma.RoleARN,
			ActionParams: ma.ActionParams,
		})
	}

	return refs
}

// DetectMitigationActionExecution represents one mitigation action applied
// (or to be applied) to one violation as part of a DetectMitigationTask.
//
// ExecutionStartDate/ExecutionEndDate were field-diffed against
// types.DetectMitigationActionExecution (awsRestjson1_deserializeDocumentDetectMitigationActionExecution
// in v1.76.0) and found to be wire-keyed "executionStartTime"/
// "executionEndTime" -- the real fields are "executionStartDate"/
// "executionEndDate". A real client's deserializer would never have found
// either key and left both fields permanently unset. Fixed.
type DetectMitigationActionExecution struct {
	TaskID             string  `json:"taskId"`
	ViolationID        string  `json:"violationId"`
	ActionName         string  `json:"actionName"`
	ThingName          string  `json:"thingName,omitempty"`
	Status             string  `json:"status"`
	ErrorCode          string  `json:"errorCode,omitempty"`
	Message            string  `json:"message,omitempty"`
	ExecutionStartDate float64 `json:"executionStartDate,omitempty"`
	ExecutionEndDate   float64 `json:"executionEndDate,omitempty"`
}

func cloneDetectMitigationExecution(e *DetectMitigationActionExecution) *DetectMitigationActionExecution {
	cp := *e

	return &cp
}

// StartDetectMitigationActionsTaskInput is the input for StartDetectMitigationActionsTask.
type StartDetectMitigationActionsTaskInput struct {
	Target                        *DetectMitigationActionsTaskTarget
	ViolationEventOccurrenceRange *ViolationEventOccurrenceRange
	TaskID                        string
	Actions                       []string
	IncludeOnlyActiveViolations   bool
	IncludeSuppressedAlerts       bool
}

// detectMitigationViolationIDs resolves the set of active-violation IDs that a
// task target applies to. Must be called with b.mu held.
func (b *InMemoryBackend) detectMitigationViolationIDs(target *DetectMitigationActionsTaskTarget) []string {
	if target == nil {
		return nil
	}
	if len(target.ViolationIDs) > 0 {
		return append([]string(nil), target.ViolationIDs...)
	}
	if target.SecurityProfileName == "" && target.BehaviorName == "" {
		return nil
	}

	var ids []string

	for _, v := range b.activeViolations.All() {
		if target.SecurityProfileName != "" && v.SecurityProfileName != target.SecurityProfileName {
			continue
		}
		if target.BehaviorName != "" && (v.Behavior == nil || v.Behavior.Name != target.BehaviorName) {
			continue
		}
		ids = append(ids, v.ViolationID)
	}
	sort.Strings(ids)

	return ids
}

// StartDetectMitigationActionsTask creates a Device Defender ML Detect
// mitigation-action task, seeding one execution record per (violation, action)
// pair resolved from the task target.
func (b *InMemoryBackend) StartDetectMitigationActionsTask(
	input *StartDetectMitigationActionsTaskInput,
) (*DetectMitigationTask, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if input.TaskID == "" {
		return nil, fmt.Errorf("%w: taskId is required", ErrValidation)
	}
	if b.detectMitigationTasks.Has(input.TaskID) {
		return nil, fmt.Errorf(
			"detect mitigation actions task %q already exists: %w",
			input.TaskID,
			ErrAlreadyExists,
		)
	}
	if input.Target == nil || len(input.Actions) == 0 {
		return nil, fmt.Errorf("%w: target and actions are required", ErrValidation)
	}

	now := float64(time.Now().Unix())
	task := &DetectMitigationTask{
		TaskID:                        input.TaskID,
		TaskStatus:                    string(JobStatusInProgress),
		Target:                        cloneDetectMitigationTarget(input.Target),
		Actions:                       append([]string(nil), input.Actions...),
		OnlyActiveViolationsIncluded:  input.IncludeOnlyActiveViolations,
		SuppressedAlertsIncluded:      input.IncludeSuppressedAlerts,
		StartTime:                     now,
		TaskStatistics:                &DetectMitigationTaskStatistics{},
		ViolationEventOccurrenceRange: input.ViolationEventOccurrenceRange,
	}

	violationIDs := b.detectMitigationViolationIDs(input.Target)
	executions := make([]*DetectMitigationActionExecution, 0, len(violationIDs)*len(input.Actions))

	for _, violationID := range violationIDs {
		thingName := ""
		if v, ok := b.activeViolations.Get(violationID); ok {
			thingName = v.ThingName
		}

		for _, actionName := range input.Actions {
			executions = append(executions, &DetectMitigationActionExecution{
				TaskID:             input.TaskID,
				ViolationID:        violationID,
				ActionName:         actionName,
				ThingName:          thingName,
				Status:             string(JobStatusInProgress),
				ExecutionStartDate: now,
			})
			task.TaskStatistics.ActionsExecuted++
		}
	}

	b.detectMitigationTasks.Put(task)
	b.detectMitigationExecutions[input.TaskID] = executions

	return cloneDetectMitigationTask(task), nil
}

// DescribeDetectMitigationActionsTask returns a previously started detect
// mitigation actions task, or ErrResourceNotFound if taskID is unknown.
func (b *InMemoryBackend) DescribeDetectMitigationActionsTask(taskID string) (*DetectMitigationTask, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	t, ok := b.detectMitigationTasks.Get(taskID)
	if !ok {
		return nil, fmt.Errorf("detect mitigation actions task %q not found: %w", taskID, ErrResourceNotFound)
	}

	return cloneDetectMitigationTask(t), nil
}

// ListDetectMitigationActionsTasks returns stored detect mitigation actions
// tasks, optionally filtered to those started within [startTime, endTime]
// (epoch seconds; zero means unbounded).
func (b *InMemoryBackend) ListDetectMitigationActionsTasks(startTime, endTime float64) []*DetectMitigationTask {
	b.mu.RLock()
	defer b.mu.RUnlock()

	items := b.detectMitigationTasks.Snapshot()
	out := make([]*DetectMitigationTask, 0, len(items))

	for _, v := range items {
		t := v
		if startTime > 0 && t.StartTime < startTime {
			continue
		}
		if endTime > 0 && t.StartTime > endTime {
			continue
		}
		out = append(out, cloneDetectMitigationTask(t))
	}

	return out
}

// CancelDetectMitigationActionsTask transitions a detect mitigation actions task
// to CANCELED, or returns ErrResourceNotFound if taskID is unknown.
func (b *InMemoryBackend) CancelDetectMitigationActionsTask(taskID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	t, ok := b.detectMitigationTasks.Get(taskID)
	if !ok {
		return fmt.Errorf("detect mitigation actions task %q not found: %w", taskID, ErrResourceNotFound)
	}
	t.TaskStatus = string(JobStatusCanceled)
	t.EndTime = float64(time.Now().Unix())

	return nil
}

// ListDetectMitigationActionsExecutions returns the mitigation-action execution
// records for a task, optionally filtered by violation ID and/or thing name.
func (b *InMemoryBackend) ListDetectMitigationActionsExecutions(
	taskID, violationID, thingName string,
) []*DetectMitigationActionExecution {
	b.mu.RLock()
	defer b.mu.RUnlock()

	taskIDs := []string{taskID}
	if taskID == "" {
		taskIDs = sortedKeys(b.detectMitigationExecutions)
	}

	out := []*DetectMitigationActionExecution{}

	for _, tid := range taskIDs {
		for _, e := range b.detectMitigationExecutions[tid] {
			if violationID != "" && e.ViolationID != violationID {
				continue
			}
			if thingName != "" && e.ThingName != thingName {
				continue
			}
			out = append(out, cloneDetectMitigationExecution(e))
		}
	}

	return out
}

// ---------------------------------------------------------------------------
// Device Defender Detect violations
// ---------------------------------------------------------------------------

// ViolationBehavior identifies the security-profile behavior that a violation
// or violation event relates to.
type ViolationBehavior struct {
	Name   string `json:"name,omitempty"`
	Metric string `json:"metric,omitempty"`
}

// ViolationEventAdditionalInfo carries extra detail about a violation or
// violation event (aws-sdk-go-v2/service/iot/types.ViolationEventAdditionalInfo).
type ViolationEventAdditionalInfo struct {
	ConfidenceLevel string `json:"confidenceLevel,omitempty"`
}

// ActiveViolation represents a currently active Device Defender Detect
// violation of a security-profile behavior by a thing.
//
// Field-diffed against types.ActiveViolation in v1.76.0: LastViolationTime
// and ViolationEventAdditionalInfo were both entirely missing (only
// ViolationStartTime was modeled, but real AWS distinguishes "when the
// violation started" from "when the most recent violation occurred" -- the
// latter updates on every subsequent detection of the same ongoing
// violation); both now modeled.
// Suppressed is internal-only (json:"-"): real AWS has no "suppressed" field
// on ActiveViolation itself -- ListActiveViolationsInput's listSuppressedAlerts
// filters on whether the *behavior* that raised the violation has
// SuppressAlerts configured (a security-profile-level setting), which this
// backend does not persist at all (CreateSecurityProfile doesn't store
// Behaviors currently -- a separate, larger gap in the security_profiles
// family, out of scope here). Modeling suppression as a directly-seedable
// flag (mirroring AuditFinding.IsSuppressed's identical simplification
// elsewhere in this same service) is what makes listSuppressedAlerts
// filtering honestly implementable without first building out that
// unrelated subsystem.
type ActiveViolation struct {
	Behavior                     *ViolationBehavior            `json:"behavior,omitempty"`
	LastViolationValue           map[string]any                `json:"lastViolationValue,omitempty"`
	ViolationEventAdditionalInfo *ViolationEventAdditionalInfo `json:"violationEventAdditionalInfo,omitempty"`
	ViolationID                  string                        `json:"violationId"`
	ThingName                    string                        `json:"thingName"`
	SecurityProfileName          string                        `json:"securityProfileName"`
	VerificationState            string                        `json:"verificationState,omitempty"`
	VerificationStateDescription string                        `json:"verificationStateDescription,omitempty"`
	ViolationStartTime           float64                       `json:"violationStartTime,omitempty"`
	LastViolationTime            float64                       `json:"lastViolationTime,omitempty"`
	Suppressed                   bool                          `json:"-"`
}

func cloneActiveViolation(v *ActiveViolation) *ActiveViolation {
	cp := *v
	if v.Behavior != nil {
		beh := *v.Behavior
		cp.Behavior = &beh
	}
	if v.ViolationEventAdditionalInfo != nil {
		info := *v.ViolationEventAdditionalInfo
		cp.ViolationEventAdditionalInfo = &info
	}
	cp.LastViolationValue = make(map[string]any, len(v.LastViolationValue))
	maps.Copy(cp.LastViolationValue, v.LastViolationValue)

	return &cp
}

// ViolationEvent represents a historical occurrence of a Detect violation
// starting, being cleared, or having its verification state changed.
//
// ViolationEventAdditionalInfo was field-diffed against
// types.ViolationEvent and found missing; now modeled (same field as
// ActiveViolation's, above).
// Suppressed is internal-only (json:"-") for the same reason as
// ActiveViolation's identically-named field above.
type ViolationEvent struct {
	Behavior                     *ViolationBehavior            `json:"behavior,omitempty"`
	MetricValue                  map[string]any                `json:"metricValue,omitempty"`
	ViolationEventAdditionalInfo *ViolationEventAdditionalInfo `json:"violationEventAdditionalInfo,omitempty"`
	ViolationID                  string                        `json:"violationId"`
	ThingName                    string                        `json:"thingName"`
	SecurityProfileName          string                        `json:"securityProfileName"`
	ViolationEventType           string                        `json:"violationEventType,omitempty"`
	VerificationState            string                        `json:"verificationState,omitempty"`
	ViolationEventTime           float64                       `json:"violationEventTime,omitempty"`
	Suppressed                   bool                          `json:"-"`
}

func cloneViolationEvent(e *ViolationEvent) *ViolationEvent {
	cp := *e
	if e.Behavior != nil {
		beh := *e.Behavior
		cp.Behavior = &beh
	}
	if e.ViolationEventAdditionalInfo != nil {
		info := *e.ViolationEventAdditionalInfo
		cp.ViolationEventAdditionalInfo = &info
	}
	cp.MetricValue = make(map[string]any, len(e.MetricValue))
	maps.Copy(cp.MetricValue, e.MetricValue)

	return &cp
}

// SeedActiveViolationInput seeds a Device Defender Detect violation.
type SeedActiveViolationInput struct {
	Behavior                     *ViolationBehavior
	LastViolationValue           map[string]any
	ViolationEventAdditionalInfo *ViolationEventAdditionalInfo
	ViolationID                  string
	ThingName                    string
	SecurityProfileName          string
	VerificationState            string
	Suppressed                   bool
}

// SeedActiveViolation records a Device Defender Detect violation. AWS IoT raises
// violations internally from behavior evaluation against reported device
// metrics rather than through a public "create violation" API; this hook lets
// gopherstack populate realistic state for ListActiveViolations,
// ListViolationEvents, PutVerificationStateOnViolation, and detect
// mitigation-action targeting instead of those always returning an empty set.
func (b *InMemoryBackend) SeedActiveViolation(input *SeedActiveViolationInput) (*ActiveViolation, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if input.ThingName == "" || input.SecurityProfileName == "" {
		return nil, fmt.Errorf("%w: thingName and securityProfileName are required", ErrValidation)
	}

	violationID := input.ViolationID
	if violationID == "" {
		violationID = uuid.NewString()
	}

	verificationState := input.VerificationState
	if verificationState == "" {
		verificationState = "UNSEEN"
	}

	now := float64(time.Now().Unix())
	v := &ActiveViolation{
		ViolationID:                  violationID,
		ThingName:                    input.ThingName,
		SecurityProfileName:          input.SecurityProfileName,
		Behavior:                     input.Behavior,
		LastViolationValue:           input.LastViolationValue,
		ViolationEventAdditionalInfo: input.ViolationEventAdditionalInfo,
		VerificationState:            verificationState,
		ViolationStartTime:           now,
		LastViolationTime:            now,
		Suppressed:                   input.Suppressed,
	}
	b.activeViolations.Put(v)

	b.violationEvents = append(b.violationEvents, &ViolationEvent{
		ViolationID:                  violationID,
		ThingName:                    input.ThingName,
		SecurityProfileName:          input.SecurityProfileName,
		Behavior:                     input.Behavior,
		MetricValue:                  input.LastViolationValue,
		ViolationEventAdditionalInfo: input.ViolationEventAdditionalInfo,
		ViolationEventType:           "in-alarm",
		VerificationState:            verificationState,
		ViolationEventTime:           now,
		Suppressed:                   input.Suppressed,
	})

	return cloneActiveViolation(v), nil
}

// ListActiveViolations returns currently active violations, optionally filtered
// by thing name, security profile name, and/or verification state.
// ListActiveViolations returns currently active violations, optionally
// filtered by thing name, security profile name, verification state, and/or
// suppression state. listSuppressedAlerts mirrors
// ListAuditFindingsFilter.ListSuppressedFindings' tri-state semantics
// (confirmed against the analogous, unambiguous ListAuditFindingsInput doc:
// nil means both suppressed and unsuppressed are returned, true/false
// narrows to one or the other) -- real ListActiveViolationsInput's own doc
// for listSuppressedAlerts is less explicit ("A list of all suppressed
// alerts"), but this is the only self-consistent tri-state reading and
// matches the sibling audit-findings filter's behavior in this same
// service.
func (b *InMemoryBackend) ListActiveViolations(
	thingName, securityProfileName, verificationState string, listSuppressedAlerts *bool,
) []*ActiveViolation {
	b.mu.RLock()
	defer b.mu.RUnlock()

	items := b.activeViolations.Snapshot()
	out := make([]*ActiveViolation, 0, len(items))

	for _, v := range items {
		if thingName != "" && v.ThingName != thingName {
			continue
		}
		if securityProfileName != "" && v.SecurityProfileName != securityProfileName {
			continue
		}
		if verificationState != "" && v.VerificationState != verificationState {
			continue
		}
		if listSuppressedAlerts != nil && v.Suppressed != *listSuppressedAlerts {
			continue
		}
		out = append(out, cloneActiveViolation(v))
	}

	return out
}

// ListViolationEvents returns recorded violation events, optionally filtered
// by thing name, security profile name, verification state, suppression
// state, and/or an [startTime, endTime] window (epoch seconds; zero means
// unbounded). listSuppressedAlerts has the same tri-state semantics as
// ListActiveViolations' (see its doc comment).
func (b *InMemoryBackend) ListViolationEvents(
	thingName, securityProfileName, verificationState string,
	startTime, endTime float64,
	listSuppressedAlerts *bool,
) []*ViolationEvent {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*ViolationEvent, 0, len(b.violationEvents))

	for _, e := range b.violationEvents {
		if thingName != "" && e.ThingName != thingName {
			continue
		}
		if securityProfileName != "" && e.SecurityProfileName != securityProfileName {
			continue
		}
		if verificationState != "" && e.VerificationState != verificationState {
			continue
		}
		if startTime > 0 && e.ViolationEventTime < startTime {
			continue
		}
		if endTime > 0 && e.ViolationEventTime > endTime {
			continue
		}
		if listSuppressedAlerts != nil && e.Suppressed != *listSuppressedAlerts {
			continue
		}
		out = append(out, cloneViolationEvent(e))
	}

	return out
}

// PutVerificationStateOnViolation mutates the verification state (and optional
// description) of an active violation, or returns ErrResourceNotFound if
// violationID is unknown.
func (b *InMemoryBackend) PutVerificationStateOnViolation(violationID, verificationState, description string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	v, ok := b.activeViolations.Get(violationID)
	if !ok {
		return fmt.Errorf("violation %q not found: %w", violationID, ErrResourceNotFound)
	}
	v.VerificationState = verificationState
	v.VerificationStateDescription = description

	return nil
}

// ---------------------------------------------------------------------------
// Persistence helpers
// ---------------------------------------------------------------------------

// deviceDefenderSnapshot bundles the Device Defender fields of backendSnapshot
// so Snapshot/Restore can delegate to a single helper each, keeping their own
// cyclomatic complexity low.
// deviceDefenderSnapshot now only carries the raw (non-Table) Device Defender
// fields. AuditMitigationTaskObjects, DetectMitigationTasks, and
// ActiveViolations moved to store.Table[T]s registered on b.registry (see
// store_setup.go) and round-trip via registry.SnapshotAll()/RestoreAll()
// instead of through this bundle.
type deviceDefenderSnapshot struct {
	AuditMitigationExecutions  map[string][]*AuditMitigationActionExecution
	DetectMitigationExecutions map[string][]*DetectMitigationActionExecution
	ViolationEvents            []*ViolationEvent
}

// snapshotDeviceDefender deep-copies the raw (non-Table) Device Defender
// state. Must be called with b.mu held (read or write).
func (b *InMemoryBackend) snapshotDeviceDefender() deviceDefenderSnapshot {
	auditMitigationExecutions := make(
		map[string][]*AuditMitigationActionExecution,
		len(b.auditMitigationExecutions),
	)
	for k, execs := range b.auditMitigationExecutions {
		cp := make([]*AuditMitigationActionExecution, len(execs))
		for i, e := range execs {
			cp[i] = cloneAuditMitigationExecution(e)
		}
		auditMitigationExecutions[k] = cp
	}

	detectMitigationExecutions := make(
		map[string][]*DetectMitigationActionExecution,
		len(b.detectMitigationExecutions),
	)
	for k, execs := range b.detectMitigationExecutions {
		cp := make([]*DetectMitigationActionExecution, len(execs))
		for i, e := range execs {
			cp[i] = cloneDetectMitigationExecution(e)
		}
		detectMitigationExecutions[k] = cp
	}

	violationEvents := make([]*ViolationEvent, len(b.violationEvents))
	for i, e := range b.violationEvents {
		violationEvents[i] = cloneViolationEvent(e)
	}

	return deviceDefenderSnapshot{
		AuditMitigationExecutions:  auditMitigationExecutions,
		DetectMitigationExecutions: detectMitigationExecutions,
		ViolationEvents:            violationEvents,
	}
}

// restoreDeviceDefender deep-copies snap into the backend's raw (non-Table)
// Device Defender state. Must be called with b.mu held (write).
func (b *InMemoryBackend) restoreDeviceDefender(snap deviceDefenderSnapshot) {
	b.auditMitigationExecutions = make(
		map[string][]*AuditMitigationActionExecution,
		len(snap.AuditMitigationExecutions),
	)
	for k, execs := range snap.AuditMitigationExecutions {
		cp := make([]*AuditMitigationActionExecution, len(execs))
		for i, e := range execs {
			cp[i] = cloneAuditMitigationExecution(e)
		}
		b.auditMitigationExecutions[k] = cp
	}

	b.detectMitigationExecutions = make(
		map[string][]*DetectMitigationActionExecution,
		len(snap.DetectMitigationExecutions),
	)
	for k, execs := range snap.DetectMitigationExecutions {
		cp := make([]*DetectMitigationActionExecution, len(execs))
		for i, e := range execs {
			cp[i] = cloneDetectMitigationExecution(e)
		}
		b.detectMitigationExecutions[k] = cp
	}

	b.violationEvents = make([]*ViolationEvent, len(snap.ViolationEvents))
	for i, e := range snap.ViolationEvents {
		b.violationEvents[i] = cloneViolationEvent(e)
	}
}

// BehaviorModelTrainingSummary reports ML Detect model-training progress for
// one behavior of a security profile.
type BehaviorModelTrainingSummary struct {
	SecurityProfileName             string  `json:"securityProfileName"`
	BehaviorName                    string  `json:"behaviorName"`
	ModelStatus                     string  `json:"modelStatus,omitempty"`
	DatapointsCollectionPercentage  float64 `json:"datapointsCollectionPercentage,omitempty"`
	LastModelRefreshDate            float64 `json:"lastModelRefreshDate,omitempty"`
	TrainingDataCollectionStartDate float64 `json:"trainingDataCollectionStartDate,omitempty"`
}

// GetBehaviorModelTrainingSummaries returns stored per-behavior training
// summaries, optionally scoped to one security profile, paginated.
func (b *InMemoryBackend) GetBehaviorModelTrainingSummaries(
	securityProfileName string, maxResults int32, nextToken string,
) ([]*BehaviorModelTrainingSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var all []*BehaviorModelTrainingSummary

	if securityProfileName != "" {
		if !b.securityProfiles.Has(securityProfileName) {
			return nil, "", fmt.Errorf(
				"security profile %q not found: %w", securityProfileName, ErrResourceNotFound,
			)
		}

		for _, s := range b.behaviorTrainingSummaries[securityProfileName] {
			cp := *s
			all = append(all, &cp)
		}
	} else {
		for _, name := range sortedKeys(b.behaviorTrainingSummaries) {
			for _, s := range b.behaviorTrainingSummaries[name] {
				cp := *s
				all = append(all, &cp)
			}
		}
	}

	page, next := paginateMaps(all, searchPageSize(maxResults), searchStartOffset(nextToken))

	return page, next, nil
}

// AddBehaviorModelTrainingSummaryInternal seeds a training summary for a
// security profile (there is no public API to trigger ML Detect training;
// AWS populates this asynchronously once enough data is collected).
func (b *InMemoryBackend) AddBehaviorModelTrainingSummaryInternal(
	securityProfileName string, s BehaviorModelTrainingSummary,
) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp := s
	cp.SecurityProfileName = securityProfileName
	b.behaviorTrainingSummaries[securityProfileName] = append(b.behaviorTrainingSummaries[securityProfileName], &cp)
}
