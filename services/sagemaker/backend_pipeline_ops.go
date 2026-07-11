package sagemaker

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"
)

// ---------------------------------------------------------------------------
// Additional pipeline execution operations
// ---------------------------------------------------------------------------

// PipelineExecutionStep represents a step within a pipeline execution.
type PipelineExecutionStep struct {
	StartTime     time.Time `json:"StartTime"`
	EndTime       time.Time `json:"EndTime"`
	StepName      string    `json:"StepName"`
	StepType      string    `json:"StepType"`
	StepStatus    string    `json:"StepStatus"`
	FailureReason string    `json:"FailureReason,omitempty"`
	// ExecutionArn is the owning pipeline execution's ARN, carried internally so
	// the store.Table's keyFn can derive pipelineExecutionStepsKey(ExecutionArn,
	// StepName) and Restore can rebuild it after a JSON round-trip. Exported
	// (with a json tag) solely so it survives persistence — handler.go never
	// marshals PipelineExecutionStep directly to API callers, so this has no
	// effect on the AWS wire shape.
	ExecutionArn string `json:"executionArn"`
}

const (
	retryTransitionDelay = 200 * time.Millisecond // delay for retry execution to succeed
	stopTransitionDelay  = 100 * time.Millisecond // delay for execution to stop
)

const (
	pipelineStatusExecuting = "Executing"
	pipelineStatusSucceeded = "Succeeded"
	pipelineStatusStopping  = "Stopping"
	pipelineStatusStopped   = "Stopped"
	stepTypeCallback        = "Callback"
	stepStatusSucceeded     = "Succeeded"
	stepStatusFailed        = "Failed"
)

// pipelineExecutionStepsKey builds the map key for step records.
func pipelineExecutionStepsKey(execArn, stepName string) string {
	return execArn + "|" + stepName
}

// RetryPipelineExecution creates a new execution from a failed pipeline execution.
func (b *InMemoryBackend) RetryPipelineExecution(ctx context.Context, execArn string) (*PipelineExecution, error) {
	b.mu.Lock("RetryPipelineExecution")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	pe, ok := b.pipelineExecutionsStore(region).Get(execArn)
	if !ok {
		return nil, fmt.Errorf(
			"%w: pipeline execution %q not found",
			ErrPipelineExecutionNotFound,
			execArn,
		)
	}

	newID := generateID()
	newArn := pe.PipelineArn + "/execution/" + newID
	now := time.Now()

	newExec := &PipelineExecution{
		PipelineArn:             pe.PipelineArn,
		PipelineExecutionArn:    newArn,
		PipelineExecutionStatus: pipelineStatusExecuting,
		StartTime:               now,
	}
	b.pipelineExecutionsStore(region).Put(newExec)

	// Transition to Succeeded after a short delay.
	b.runDelayed(b.lifecycleCtx, retryTransitionDelay, func() {
		b.mu.Lock("RetryPipelineExecution.goroutine")
		defer b.mu.Unlock()

		if exec, exists := b.pipelineExecutionsStore(region).Get(newArn); exists {
			exec.PipelineExecutionStatus = pipelineStatusSucceeded
		}
	})

	return clonePipelineExecution(newExec), nil
}

// StopPipelineExecution stops a running pipeline execution.
func (b *InMemoryBackend) StopPipelineExecution(ctx context.Context, execArn string) (*PipelineExecution, error) {
	b.mu.Lock("StopPipelineExecution")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	pe, ok := b.pipelineExecutionsStore(region).Get(execArn)
	if !ok {
		return nil, fmt.Errorf(
			"%w: pipeline execution %q not found",
			ErrPipelineExecutionNotFound,
			execArn,
		)
	}

	pe.PipelineExecutionStatus = pipelineStatusStopping
	cp := clonePipelineExecution(pe)

	// Transition to Stopped after a short delay.
	b.runDelayed(b.lifecycleCtx, stopTransitionDelay, func() {
		b.mu.Lock("StopPipelineExecution.goroutine")
		defer b.mu.Unlock()

		if exec, exists := b.pipelineExecutionsStore(region).Get(execArn); exists {
			exec.PipelineExecutionStatus = pipelineStatusStopped
		}
	})

	return cp, nil
}

// SendPipelineExecutionStepSuccess records a step success for a callback step.
func (b *InMemoryBackend) SendPipelineExecutionStepSuccess(ctx context.Context, execArn, stepName string) error {
	b.mu.Lock("SendPipelineExecutionStepSuccess")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.pipelineExecutionsStore(region).Get(execArn); !ok {
		return fmt.Errorf(
			"%w: pipeline execution %q not found",
			ErrPipelineExecutionNotFound,
			execArn,
		)
	}

	now := time.Now()

	b.pipelineExecStepsStore(region).Put(&PipelineExecutionStep{
		StartTime:    now,
		EndTime:      now,
		StepName:     stepName,
		StepType:     stepTypeCallback,
		StepStatus:   stepStatusSucceeded,
		ExecutionArn: execArn,
	})

	return nil
}

// SendPipelineExecutionStepFailure records a step failure for a callback step.
func (b *InMemoryBackend) SendPipelineExecutionStepFailure(
	ctx context.Context, execArn, stepName, failureReason string,
) error {
	b.mu.Lock("SendPipelineExecutionStepFailure")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.pipelineExecutionsStore(region).Get(execArn); !ok {
		return fmt.Errorf(
			"%w: pipeline execution %q not found",
			ErrPipelineExecutionNotFound,
			execArn,
		)
	}

	now := time.Now()

	b.pipelineExecStepsStore(region).Put(&PipelineExecutionStep{
		StartTime:     now,
		EndTime:       now,
		StepName:      stepName,
		StepType:      stepTypeCallback,
		StepStatus:    stepStatusFailed,
		FailureReason: failureReason,
		ExecutionArn:  execArn,
	})

	return nil
}

// ListPipelineExecutionSteps lists the steps for a pipeline execution.
func (b *InMemoryBackend) ListPipelineExecutionSteps(
	ctx context.Context, execArn, nextToken string,
) ([]*PipelineExecutionStep, string) {
	b.mu.RLock("ListPipelineExecutionSteps")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	list := make([]*PipelineExecutionStep, 0, b.pipelineExecStepsStore(region).Len())

	for _, step := range b.pipelineExecStepsStore(region).All() {
		if step.ExecutionArn == execArn {
			cp := *step
			list = append(list, &cp)
		}
	}

	sort.Slice(list, func(i, j int) bool { return list[i].StepName < list[j].StepName })

	startIdx := 0
	if nextToken != "" {
		if n, err := strconv.Atoi(nextToken); err == nil {
			startIdx = n
		}
	}

	if startIdx >= len(list) {
		return []*PipelineExecutionStep{}, ""
	}

	end := startIdx + sagemakerDefaultPageSize
	var outToken string

	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}
