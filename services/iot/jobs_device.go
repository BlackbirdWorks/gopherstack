package iot

import (
	"fmt"
	"maps"
	"slices"
	"time"
)

// DeviceJobExecution is a job execution as the device-side Jobs data plane reports it.
type DeviceJobExecution struct {
	StatusDetails   map[string]string
	JobID           string
	ThingName       string
	Status          JobExecutionStatus
	JobDocument     string
	QueuedAt        int64
	StartedAt       int64
	LastUpdatedAt   int64
	VersionNumber   int64
	ExecutionNumber int64
}

// UpdateDeviceJobExecutionInput is a device UpdateJobExecution request.
type UpdateDeviceJobExecutionInput struct {
	StatusDetails   map[string]string
	JobID           string
	ThingName       string
	Status          string
	ExpectedVersion int64
	ExecutionNumber int64
}

func deviceExecution(job *Job, e *JobExecution, withDocument bool) *DeviceJobExecution {
	out := &DeviceJobExecution{
		JobID:           e.JobID,
		ThingName:       e.ThingName,
		Status:          e.Status,
		QueuedAt:        int64(e.QueuedAt),
		StartedAt:       int64(e.StartedAt),
		LastUpdatedAt:   int64(e.LastUpdatedAt),
		VersionNumber:   e.VersionNumber,
		ExecutionNumber: e.ExecutionNumber,
	}

	if e.StatusDetails != nil {
		out.StatusDetails = maps.Clone(e.StatusDetails.DetailsMap)
	}

	if withDocument && job != nil {
		out.JobDocument = job.Document
	}

	return out
}

func (b *InMemoryBackend) requireThingLocked(thingName string) error {
	if _, ok := b.things.Get(thingName); !ok {
		return fmt.Errorf("%w: thing %q", ErrResourceNotFound, thingName)
	}

	return nil
}

// pendingExecutionsLocked returns the thing's QUEUED and IN_PROGRESS executions, oldest first.
func (b *InMemoryBackend) pendingExecutionsLocked(thingName string) ([]*JobExecution, []*JobExecution) {
	var inProgress, queued []*JobExecution

	for _, e := range b.jobExecutions.All() {
		if e.ThingName != thingName {
			continue
		}

		switch e.Status {
		case JobExecInProgress:
			inProgress = append(inProgress, e)
		case JobExecQueued:
			queued = append(queued, e)
		case JobExecSucceeded, JobExecFailed, JobExecCanceled, JobExecRejected, JobExecRemoved, JobExecTimedOut:
		}
	}

	order := func(a, c *JobExecution) int {
		if d := int(a.QueuedAt - c.QueuedAt); d != 0 {
			return d
		}

		return compareStrings(a.JobID, c.JobID)
	}
	slices.SortFunc(inProgress, order)
	slices.SortFunc(queued, order)

	return inProgress, queued
}

func compareStrings(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}

	return 0
}

// GetPendingJobExecutions lists the thing's IN_PROGRESS and QUEUED executions.
func (b *InMemoryBackend) GetPendingJobExecutions(
	thingName string,
) ([]*DeviceJobExecution, []*DeviceJobExecution, error) {
	b.mu.RLock("GetPendingJobExecutions")
	defer b.mu.RUnlock()

	if err := b.requireThingLocked(thingName); err != nil {
		return nil, nil, err
	}

	var inProgress, queued []*DeviceJobExecution

	ip, q := b.pendingExecutionsLocked(thingName)

	for _, e := range ip {
		inProgress = append(inProgress, deviceExecution(nil, e, false))
	}

	for _, e := range q {
		queued = append(queued, deviceExecution(nil, e, false))
	}

	return inProgress, queued, nil
}

// StartNextPendingJobExecution starts the thing's oldest QUEUED execution; an already IN_PROGRESS one is
// returned as is. found is false when nothing is pending.
func (b *InMemoryBackend) StartNextPendingJobExecution(
	thingName string, statusDetails map[string]string,
) (*DeviceJobExecution, bool, error) {
	b.mu.Lock("StartNextPendingJobExecution")
	defer b.mu.Unlock()

	if err := b.requireThingLocked(thingName); err != nil {
		return nil, false, err
	}

	inProgress, queued := b.pendingExecutionsLocked(thingName)

	var exec *JobExecution

	switch {
	case len(inProgress) > 0:
		exec = inProgress[0]
	case len(queued) > 0:
		exec = queued[0]
	default:
		return nil, false, nil
	}

	now := float64(time.Now().Unix())

	if exec.Status == JobExecQueued {
		exec.Status = JobExecInProgress
		exec.StartedAt = now
		exec.LastUpdatedAt = now
		exec.VersionNumber++
		exec.StatusDetails = mergeStatusDetails(exec.StatusDetails, statusDetails)
	}

	job, _ := b.jobs.Get(exec.JobID)

	return deviceExecution(job, exec, true), true, nil
}

func mergeStatusDetails(cur *JobExecutionStatusDetails, in map[string]string) *JobExecutionStatusDetails {
	if len(in) == 0 {
		return cur
	}

	if cur == nil {
		cur = &JobExecutionStatusDetails{DetailsMap: map[string]string{}}
	}

	maps.Copy(cur.DetailsMap, in)

	return cur
}

// DescribeDeviceJobExecution returns one execution for a device; job document is included on request.
func (b *InMemoryBackend) DescribeDeviceJobExecution(
	thingName, jobID string, executionNumber int64, includeDocument bool,
) (*DeviceJobExecution, error) {
	b.mu.RLock("DescribeDeviceJobExecution")
	defer b.mu.RUnlock()

	if err := b.requireThingLocked(thingName); err != nil {
		return nil, err
	}

	exec, ok := b.jobExecutions.Get(jobExecKey(jobID, thingName))
	if !ok || (executionNumber != 0 && executionNumber != exec.ExecutionNumber) {
		return nil, fmt.Errorf("%w: job execution %q for thing %q", ErrResourceNotFound, jobID, thingName)
	}

	job, _ := b.jobs.Get(jobID)

	return deviceExecution(job, exec, includeDocument), nil
}

// UpdateDeviceJobExecution applies a device-reported status. Finished executions are immutable
// (TerminalStateException) and a stale ExpectedVersion is an InvalidStateTransitionException.
func (b *InMemoryBackend) UpdateDeviceJobExecution(in *UpdateDeviceJobExecutionInput) (*DeviceJobExecution, error) {
	status := JobExecutionStatus(in.Status)

	switch status {
	case JobExecInProgress, JobExecSucceeded, JobExecFailed, JobExecRejected, JobExecTimedOut:
	default:
		return nil, fmt.Errorf("%w: status %q cannot be reported by a device", ErrValidation, in.Status)
	}

	b.mu.Lock("UpdateDeviceJobExecution")
	defer b.mu.Unlock()

	if err := b.requireThingLocked(in.ThingName); err != nil {
		return nil, err
	}

	exec, ok := b.jobExecutions.Get(jobExecKey(in.JobID, in.ThingName))
	if !ok || (in.ExecutionNumber != 0 && in.ExecutionNumber != exec.ExecutionNumber) {
		return nil, fmt.Errorf("%w: job execution %q for thing %q", ErrResourceNotFound, in.JobID, in.ThingName)
	}

	if isTerminalJobExecutionStatus(exec.Status) {
		return nil, fmt.Errorf("%w: job execution is already %s", ErrTerminalState, exec.Status)
	}

	if in.ExpectedVersion != 0 && in.ExpectedVersion != exec.VersionNumber {
		return nil, fmt.Errorf(
			"%w: VersionMismatch, execution is at version %d, expected %d",
			ErrInvalidStateTransition, exec.VersionNumber, in.ExpectedVersion,
		)
	}

	now := float64(time.Now().Unix())

	if exec.Status == JobExecQueued && status == JobExecInProgress {
		exec.StartedAt = now
	}

	exec.Status = status
	exec.LastUpdatedAt = now
	exec.VersionNumber++
	exec.StatusDetails = mergeStatusDetails(exec.StatusDetails, in.StatusDetails)

	job, _ := b.jobs.Get(in.JobID)
	b.completeJobIfDoneLocked(job, now)

	return deviceExecution(job, exec, true), nil
}

// completeJobIfDoneLocked moves a SNAPSHOT job to COMPLETED once every execution has finished.
func (b *InMemoryBackend) completeJobIfDoneLocked(job *Job, now float64) {
	if job == nil || job.Status != JobStatusInProgress || job.TargetSelection == jobTargetContinuous {
		return
	}

	execs := b.jobExecutionsForJob(job.JobID)
	if len(execs) == 0 {
		return
	}

	for _, e := range execs {
		if !isTerminalJobExecutionStatus(e.Status) {
			return
		}
	}

	job.Status = JobStatusCompleted
	job.CompletedAt = now
	job.LastUpdatedAt = now
}
