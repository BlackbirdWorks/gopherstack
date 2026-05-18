package sagemaker

import (
	"fmt"
	"sort"
	"strconv"
	"sync"
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
func (b *InMemoryBackend) RetryPipelineExecution(execArn string) (*PipelineExecution, error) {
	b.mu.Lock("RetryPipelineExecution")
	defer b.mu.Unlock()

	pe, ok := b.pipelineExecutions[execArn]
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
	b.pipelineExecutions[newArn] = newExec

	// Transition to Succeeded after a short delay.
	go func() {
		time.Sleep(retryTransitionDelay)

		b.mu.Lock("RetryPipelineExecution.goroutine")
		defer b.mu.Unlock()

		if exec, exists := b.pipelineExecutions[newArn]; exists {
			exec.PipelineExecutionStatus = pipelineStatusSucceeded
		}
	}()

	return clonePipelineExecution(newExec), nil
}

// StopPipelineExecution stops a running pipeline execution.
func (b *InMemoryBackend) StopPipelineExecution(execArn string) (*PipelineExecution, error) {
	b.mu.Lock("StopPipelineExecution")
	defer b.mu.Unlock()

	pe, ok := b.pipelineExecutions[execArn]
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
	var once sync.Once

	go func() {
		once.Do(func() {
			time.Sleep(stopTransitionDelay)

			b.mu.Lock("StopPipelineExecution.goroutine")
			defer b.mu.Unlock()

			if exec, exists := b.pipelineExecutions[execArn]; exists {
				exec.PipelineExecutionStatus = pipelineStatusStopped
			}
		})
	}()

	return cp, nil
}

// SendPipelineExecutionStepSuccess records a step success for a callback step.
func (b *InMemoryBackend) SendPipelineExecutionStepSuccess(execArn, stepName string) error {
	b.mu.Lock("SendPipelineExecutionStepSuccess")
	defer b.mu.Unlock()

	if _, ok := b.pipelineExecutions[execArn]; !ok {
		return fmt.Errorf(
			"%w: pipeline execution %q not found",
			ErrPipelineExecutionNotFound,
			execArn,
		)
	}

	key := pipelineExecutionStepsKey(execArn, stepName)
	now := time.Now()

	b.pipelineExecSteps[key] = &PipelineExecutionStep{
		StartTime:  now,
		EndTime:    now,
		StepName:   stepName,
		StepType:   stepTypeCallback,
		StepStatus: stepStatusSucceeded,
	}

	return nil
}

// SendPipelineExecutionStepFailure records a step failure for a callback step.
func (b *InMemoryBackend) SendPipelineExecutionStepFailure(
	execArn, stepName, failureReason string,
) error {
	b.mu.Lock("SendPipelineExecutionStepFailure")
	defer b.mu.Unlock()

	if _, ok := b.pipelineExecutions[execArn]; !ok {
		return fmt.Errorf(
			"%w: pipeline execution %q not found",
			ErrPipelineExecutionNotFound,
			execArn,
		)
	}

	key := pipelineExecutionStepsKey(execArn, stepName)
	now := time.Now()

	b.pipelineExecSteps[key] = &PipelineExecutionStep{
		StartTime:     now,
		EndTime:       now,
		StepName:      stepName,
		StepType:      stepTypeCallback,
		StepStatus:    stepStatusFailed,
		FailureReason: failureReason,
	}

	return nil
}

// ListPipelineExecutionSteps lists the steps for a pipeline execution.
func (b *InMemoryBackend) ListPipelineExecutionSteps(
	execArn, nextToken string,
) ([]*PipelineExecutionStep, string) {
	b.mu.RLock("ListPipelineExecutionSteps")
	defer b.mu.RUnlock()

	prefix := execArn + "|"
	list := make([]*PipelineExecutionStep, 0, len(b.pipelineExecSteps))

	for key, step := range b.pipelineExecSteps {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
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
