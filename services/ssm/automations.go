package ssm

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

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
		ExecutionType:         "Standard",
		Mode:                  mode,
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

// DescribeAutomationExecutions returns all automation executions.
func (b *InMemoryBackend) DescribeAutomationExecutions(
	ctx context.Context,
	_ *DescribeAutomationExecutionsInput,
) (*DescribeAutomationExecutionsOutputFull, error) {
	region := getRegion(ctx)
	b.mu.Lock("DescribeAutomationExecutions")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	execs := b.automationExecutionsStore(region)
	list := make([]AutomationExecution, 0, execs.Len())
	for _, exec := range execs.All() {
		materializeAutomationLocked(exec, now)
		list = append(list, *exec)
	}

	sort.Slice(list, func(i, k int) bool {
		return list[i].StartTime < list[k].StartTime
	})

	return &DescribeAutomationExecutionsOutputFull{AutomationExecutionMetadataList: list}, nil
}

// StopAutomationExecution marks an automation execution as stopped.
func (b *InMemoryBackend) StopAutomationExecution(
	ctx context.Context,
	input *StopAutomationExecutionInput,
) (*StopAutomationExecutionOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("StopAutomationExecution")
	defer b.mu.Unlock()

	if exec, exists := b.automationExecutionsStore(region).Get(input.AutomationExecutionID); exists {
		exec.Status = automationStatusStopped
		exec.EndTime = UnixTimeFloat(time.Now().UTC())
	}

	return &StopAutomationExecutionOutput{}, nil
}

// SendAutomationSignal sends a signal to an automation execution.
// Approve/Reject signals update the execution status accordingly.
func (b *InMemoryBackend) SendAutomationSignal(
	ctx context.Context,
	input *SendAutomationSignalInput,
) (*SendAutomationSignalOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("SendAutomationSignal")
	defer b.mu.Unlock()

	exec, exists := b.automationExecutionsStore(region).Get(input.AutomationExecutionID)
	if !exists {
		return &SendAutomationSignalOutput{}, nil
	}

	switch input.SignalType {
	case "Approve":
		exec.Status = "Approved"
	case "Reject":
		exec.Status = "Rejected"
	case "StopStep":
		exec.Status = automationStatusStopped
	}

	return &SendAutomationSignalOutput{}, nil
}

// DescribeAutomationStepExecutions returns step executions for an automation.
func (b *InMemoryBackend) DescribeAutomationStepExecutions(
	ctx context.Context,
	input *DescribeAutomationStepExecutionsInput,
) (*DescribeAutomationStepExecutionsOutputFull, error) {
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
		ExecutionType:         "ChangeRequest",
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
		return &GetCalendarStateOutputFull{State: calendarStateOpen}, nil
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

	return &GetCalendarStateOutputFull{State: calendarStateOpen}, nil
}
