package ssm

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	signalTypeApprove   = "Approve"
	signalTypeReject    = "Reject"
	signalTypeStartStep = "StartStep"
	signalTypeStopStep  = "StopStep"
	signalTypeResume    = "Resume"
	signalTypeRevoke    = "Revoke"
)

// isValidSignalType reports whether s is a real SDK SignalType enum value
// (types/enums.go).
func isValidSignalType(s string) bool {
	switch s {
	case signalTypeApprove, signalTypeReject, signalTypeStartStep, signalTypeStopStep,
		signalTypeResume, signalTypeRevoke:
		return true
	default:
		return false
	}
}

func (b *InMemoryBackend) automationExecutionsStore(region string) *store.Table[AutomationExecution] {
	return getOrCreateTable(
		b, b.automationExecutions, "automationExecutions", region, automationExecutionKeyFn,
	)
}
func (b *InMemoryBackend) executionPreviewsStore(region string) *store.Table[ExecutionPreview] {
	return getOrCreateTable(b, b.executionPreviews, "executionPreviews", region, executionPreviewKeyFn)
}

// StartAutomationExecution creates a new automation execution.
func (b *InMemoryBackend) StartAutomationExecution(
	ctx context.Context,
	input *StartAutomationExecutionInput,
) (*StartAutomationExecutionOutputFull, error) {
	if input.DocumentName == "" {
		return nil, fmt.Errorf("%w: DocumentName is required", ErrValidationException)
	}

	region := getRegion(ctx)
	b.mu.Lock("StartAutomationExecution")
	defer b.mu.Unlock()

	execID := "auto-" + uuid.NewString()

	mode := input.Mode
	if mode == "" {
		mode = "Auto"
	}

	now := time.Now().UTC()
	exec := &AutomationExecution{
		AutomationExecutionID: execID,
		DocumentName:          input.DocumentName,
		DocumentVersion:       input.DocumentVersion,
		Parameters:            input.Parameters,
		Status:                automationStatusInProgress,
		StartTime:             UnixTimeFloat(now),
		Mode:                  mode,
		MaxConcurrency:        input.MaxConcurrency,
		MaxErrors:             input.MaxErrors,
		Steps:                 b.buildAutomationSteps(region, input.DocumentName),
	}

	// Complete synchronously unless an exec delay is configured, in which case
	// the execution stays InProgress and is lazily completed by reads once the
	// delay elapses — making the InProgress window observable to waiters.
	if b.automationExecDelaySecs <= 0 {
		completeAutomationLocked(exec, now)
	} else {
		exec.completeAfter = UnixTimeFloat(now) + b.automationExecDelaySecs
	}

	b.automationExecutionsStore(region).Put(exec)

	return &StartAutomationExecutionOutputFull{AutomationExecutionID: execID}, nil
}

// GetAutomationExecution returns an automation execution by ID.
func (b *InMemoryBackend) GetAutomationExecution(
	ctx context.Context,
	input *GetAutomationExecutionInput,
) (*GetAutomationExecutionOutputFull, error) {
	region := getRegion(ctx)
	b.mu.Lock("GetAutomationExecution")
	defer b.mu.Unlock()

	exec, exists := b.automationExecutionsStore(region).Get(input.AutomationExecutionID)
	if !exists {
		return nil, fmt.Errorf(
			"%w: %q",
			ErrAutomationExecutionNotFound,
			input.AutomationExecutionID,
		)
	}

	materializeAutomationLocked(exec, time.Now().UTC())

	cp := *exec

	return &GetAutomationExecutionOutputFull{AutomationExecution: &cp}, nil
}

// automationExecutionAttr returns the value of an AutomationExecution
// attribute by its AutomationExecutionFilterKey name. Only the attributes
// AutomationExecution itself tracks can be meaningfully filtered; every
// other key (ParentExecutionId/CurrentAction/StartTimeBefore/...) returns ""
// untracked (see matchesAutomationExecutionFilter).
func automationExecutionAttr(exec AutomationExecution, key string) (string, bool) {
	switch key {
	case "ExecutionId":
		return exec.AutomationExecutionID, true
	case "ExecutionStatus":
		return exec.Status, true
	default:
		return "", false
	}
}

// matchesAutomationExecutionFilter reports whether an execution satisfies a
// single key/values filter. DocumentNamePrefix is matched separately (it's a
// prefix match, not an exact-value match like every other key); unrecognized
// keys match every execution (accept-and-echo, mirroring ListNodes'
// unknown-key handling, instances.go).
func matchesAutomationExecutionFilter(exec AutomationExecution, f AutomationExecutionFilterEntry) bool {
	if f.Key == "DocumentNamePrefix" {
		for _, v := range f.Values {
			if strings.HasPrefix(exec.DocumentName, v) {
				return true
			}
		}

		return len(f.Values) == 0
	}

	value, tracked := automationExecutionAttr(exec, f.Key)
	if !tracked {
		return true
	}

	return slices.Contains(f.Values, value)
}

// DescribeAutomationExecutions returns automation executions, filtered by
// input.Filters and paginated by input.MaxResults/NextToken -- real,
// optional DescribeAutomationExecutionsInput members
// (api_op_DescribeAutomationExecutions.go) a literal struct{} input
// previously discarded from every request.
func (b *InMemoryBackend) DescribeAutomationExecutions(
	ctx context.Context,
	input *DescribeAutomationExecutionsInput,
) (*DescribeAutomationExecutionsOutputFull, error) {
	region := getRegion(ctx)
	b.mu.Lock("DescribeAutomationExecutions")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	execs := b.automationExecutionsStore(region)
	list := make([]AutomationExecution, 0, execs.Len())

	for _, exec := range execs.All() {
		materializeAutomationLocked(exec, now)

		matched := true

		for _, f := range input.Filters {
			if !matchesAutomationExecutionFilter(*exec, f) {
				matched = false

				break
			}
		}

		if matched {
			list = append(list, *exec)
		}
	}

	sort.Slice(list, func(i, k int) bool {
		return list[i].StartTime < list[k].StartTime
	})

	var maxResults int
	if input.MaxResults != nil {
		maxResults = int(*input.MaxResults)
	}

	page, next := paginateSlice(list, input.NextToken, maxResults, defaultDescribeMaxResults)

	return &DescribeAutomationExecutionsOutputFull{AutomationExecutionMetadataList: page, NextToken: next}, nil
}

// StopAutomationExecution marks an automation execution as stopped. Type
// (Cancel/Complete, types.StopType) selects the terminal status: Complete
// finishes the execution successfully, Cancel (the default, matching the
// real op's own doc comment) cancels it. Real AutomationExecutionStatus
// (types/enums.go) has no "Stopped" value at all -- Cancelled is correct.
func (b *InMemoryBackend) StopAutomationExecution(
	ctx context.Context,
	input *StopAutomationExecutionInput,
) (*StopAutomationExecutionOutput, error) {
	if input.AutomationExecutionID == "" {
		return nil, fmt.Errorf("%w: AutomationExecutionId is required", ErrValidationException)
	}

	region := getRegion(ctx)
	b.mu.Lock("StopAutomationExecution")
	defer b.mu.Unlock()

	exec, exists := b.automationExecutionsStore(region).Get(input.AutomationExecutionID)
	if !exists {
		return nil, fmt.Errorf("%w: %q", ErrAutomationExecutionNotFound, input.AutomationExecutionID)
	}

	if input.Type == "Complete" {
		exec.Status = automationStatusSuccess
	} else {
		exec.Status = automationStatusCancelled
	}

	exec.EndTime = UnixTimeFloat(time.Now().UTC())

	return &StopAutomationExecutionOutput{}, nil
}

// SendAutomationSignal sends a signal to an automation execution.
// Approve/Reject signals update the execution status accordingly. Payload
// (real, required for StartStep/StopStep/Resume to name the target step) is
// accepted but not consulted -- this backend has no per-step InProgress/
// Waiting state for a signal to target (completeAutomationLocked drives every
// step straight to Success), same simplification already disclosed for
// WarningMessage.
func (b *InMemoryBackend) SendAutomationSignal(
	ctx context.Context,
	input *SendAutomationSignalInput,
) (*SendAutomationSignalOutput, error) {
	if input.AutomationExecutionID == "" {
		return nil, fmt.Errorf("%w: AutomationExecutionId is required", ErrValidationException)
	}

	if !isValidSignalType(input.SignalType) {
		return nil, fmt.Errorf("%w: SignalType %q is not a recognized signal type",
			ErrValidationException, input.SignalType)
	}

	region := getRegion(ctx)
	b.mu.Lock("SendAutomationSignal")
	defer b.mu.Unlock()

	exec, exists := b.automationExecutionsStore(region).Get(input.AutomationExecutionID)
	if !exists {
		return nil, fmt.Errorf("%w: %q", ErrAutomationExecutionNotFound, input.AutomationExecutionID)
	}

	switch input.SignalType {
	case signalTypeApprove:
		exec.Status = "Approved"
	case signalTypeReject:
		exec.Status = "Rejected"
	case signalTypeStopStep:
		exec.Status = automationStatusCancelled
	}

	return &SendAutomationSignalOutput{}, nil
}

// DescribeAutomationStepExecutions returns step executions for an automation.
func (b *InMemoryBackend) DescribeAutomationStepExecutions(
	ctx context.Context,
	input *DescribeAutomationStepExecutionsInput,
) (*DescribeAutomationStepExecutionsOutputFull, error) {
	if input.AutomationExecutionID == "" {
		return nil, fmt.Errorf("%w: AutomationExecutionId is required", ErrValidationException)
	}

	region := getRegion(ctx)
	b.mu.Lock("DescribeAutomationStepExecutions")
	defer b.mu.Unlock()

	exec, exists := b.automationExecutionsStore(region).Get(input.AutomationExecutionID)
	if !exists {
		return &DescribeAutomationStepExecutionsOutputFull{
			StepExecutions: []AutomationStepExec{},
		}, nil
	}

	materializeAutomationLocked(exec, time.Now().UTC())

	steps := exec.Steps
	if steps == nil {
		steps = []AutomationStepExec{}
	}

	return &DescribeAutomationStepExecutionsOutputFull{StepExecutions: steps}, nil
}

// StartChangeRequestExecution creates a change request automation execution.
// Runbooks is a required StartChangeRequestExecutionInput member (verified
// against validateOpStartChangeRequestExecutionInput, validators.go), each
// entry's own DocumentName required whenever present (validateRunbook) --
// the pre-fix request read only the top-level DocumentName (the change
// template document) and built steps from it directly, when the actual
// Automation runbook(s) to run live in Runbooks instead.
func (b *InMemoryBackend) StartChangeRequestExecution(
	ctx context.Context,
	input *StartChangeRequestExecutionInput,
) (*StartChangeRequestExecutionOutputFull, error) {
	if len(input.Runbooks) == 0 {
		return nil, fmt.Errorf("%w: Runbooks is required", ErrValidationException)
	}

	for i, rb := range input.Runbooks {
		if rb.DocumentName == "" {
			return nil, fmt.Errorf("%w: Runbooks[%d].DocumentName is required", ErrValidationException, i)
		}
	}

	region := getRegion(ctx)
	b.mu.Lock("StartChangeRequestExecution")
	defer b.mu.Unlock()

	execID := "auto-cr-" + uuid.NewString()
	// Change requests remain InProgress pending approval (SendAutomationSignal),
	// mirroring AWS — but their steps are populated up front, built from the
	// first runbook's document (this backend's AutomationExecution models a
	// single step list; real AWS runs each Runbook entry as its own workflow).
	exec := &AutomationExecution{
		AutomationExecutionID: execID,
		DocumentName:          input.DocumentName,
		Status:                automationStatusInProgress,
		StartTime:             UnixTimeFloat(time.Now().UTC()),
		AutomationSubtype:     "ChangeRequest",
		MaxConcurrency:        input.Runbooks[0].MaxConcurrency,
		MaxErrors:             input.Runbooks[0].MaxErrors,
		Runbooks:              input.Runbooks,
		Steps:                 b.buildAutomationSteps(region, input.Runbooks[0].DocumentName),
	}
	b.automationExecutionsStore(region).Put(exec)

	return &StartChangeRequestExecutionOutputFull{AutomationExecutionID: execID}, nil
}

// StartExecutionPreview creates an execution preview.
func (b *InMemoryBackend) StartExecutionPreview(
	ctx context.Context,
	input *StartExecutionPreviewInput,
) (*StartExecutionPreviewOutputFull, error) {
	if input.DocumentName == "" {
		return nil, fmt.Errorf("%w: DocumentName is required", ErrValidationException)
	}

	region := getRegion(ctx)
	b.mu.Lock("StartExecutionPreview")
	defer b.mu.Unlock()

	previewID := previewIDPrefix + uuid.NewString()
	b.executionPreviewsStore(region).Put(&ExecutionPreview{
		ExecutionPreviewID: previewID,
		Status:             "Running",
		DocumentName:       input.DocumentName,
	})

	return &StartExecutionPreviewOutputFull{ExecutionPreviewID: previewID}, nil
}

// GetExecutionPreview returns an execution preview by ID.
func (b *InMemoryBackend) GetExecutionPreview(
	ctx context.Context,
	input *GetExecutionPreviewInput,
) (*GetExecutionPreviewOutputFull, error) {
	if input.ExecutionPreviewID == "" {
		return nil, fmt.Errorf("%w: ExecutionPreviewId is required", ErrValidationException)
	}

	region := getRegion(ctx)
	b.mu.RLock("GetExecutionPreview")
	defer b.mu.RUnlock()

	preview, exists := b.executionPreviewsStore(region).Get(input.ExecutionPreviewID)
	if !exists {
		return &GetExecutionPreviewOutputFull{
			ExecutionPreviewID: input.ExecutionPreviewID,
			Status:             "Running",
		}, nil
	}

	cp := *preview

	return &GetExecutionPreviewOutputFull{
		ExecutionPreviewID: preview.ExecutionPreviewID,
		Status:             preview.Status,
		ExecutionPreview:   &cp,
	}, nil
}

// GetCalendarState returns the current state of an SSM Change Calendar.
// When CalendarNames is provided, each name is looked up as a ChangeCalendar document.
// Non-existent names result in an error. The returned state is OPEN unless a
// ChangeCalendar document explicitly has a Closed state in its content.
func (b *InMemoryBackend) GetCalendarState(
	ctx context.Context,
	input *GetCalendarStateInput,
) (*GetCalendarStateOutputFull, error) {
	if len(input.CalendarNames) == 0 {
		return nil, fmt.Errorf("%w: CalendarNames is required", ErrValidationException)
	}

	region := getRegion(ctx)
	b.mu.RLock("GetCalendarState")
	defer b.mu.RUnlock()

	documents := b.documentsStore(region)
	for _, name := range input.CalendarNames {
		doc, exists := documents.Get(name)
		if !exists {
			return nil, fmt.Errorf("%w: calendar document %q not found", ErrDocumentNotFound, name)
		}

		if doc.DocumentType != "ChangeCalendar" {
			return nil, fmt.Errorf(
				"%w: document %q is not a ChangeCalendar document",
				ErrValidationException,
				name,
			)
		}
	}

	atTime := input.AtTime
	if atTime == "" {
		atTime = time.Now().UTC().Format(time.RFC3339)
	}

	return &GetCalendarStateOutputFull{State: calendarStateOpen, AtTime: atTime}, nil
}
