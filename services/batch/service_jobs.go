package batch

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const maxServiceJobNameLength = 128

// cloneServiceJobRetryStrategy deep-copies a ServiceJobRetryStrategy,
// including its EvaluateOnExit slice, returning nil for nil input.
func cloneServiceJobRetryStrategy(rs *ServiceJobRetryStrategy) *ServiceJobRetryStrategy {
	if rs == nil {
		return nil
	}

	cp := *rs
	if len(rs.EvaluateOnExit) > 0 {
		rules := make([]ServiceJobEvaluateOnExit, len(rs.EvaluateOnExit))
		copy(rules, rs.EvaluateOnExit)
		cp.EvaluateOnExit = rules
	}

	return &cp
}

// SubmitServiceJob creates a new service job in SUBMITTED status. Service
// jobs are submitted directly to a job queue (real AWS Batch requires the
// queue to be of type SAGEMAKER_TRAINING; this emulator doesn't enforce that
// cross-field constraint since it doesn't simulate SageMaker Training
// capacity). See models.go's ServiceJob doc comment for why there is no
// separate "service environment" parameter here.
func (b *InMemoryBackend) SubmitServiceJob(
	ctx context.Context,
	name, jobQueue, serviceJobType, serviceRequestPayload string,
	tags map[string]string,
	retryStrategy *ServiceJobRetryStrategy,
	timeoutConfig *ServiceJobTimeout,
	schedulingPriority int32,
	shareIdentifier string,
) (*ServiceJob, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("SubmitServiceJob")
	defer b.mu.Unlock()

	if len(name) == 0 || len(name) > maxServiceJobNameLength {
		return nil, fmt.Errorf(
			"%w: jobName must be between 1 and %d characters", ErrValidation, maxServiceJobNameLength,
		)
	}

	if serviceJobType == "" {
		return nil, fmt.Errorf("%w: serviceJobType is required", ErrValidation)
	}

	if serviceRequestPayload == "" {
		return nil, fmt.Errorf("%w: serviceRequestPayload is required", ErrValidation)
	}

	jq, ok := b.lookupJQByNameOrARN(region, jobQueue)
	if !ok {
		return nil, fmt.Errorf("%w: job queue %s not found", ErrNotFound, jobQueue)
	}

	if jq.State == stateDisabled {
		return nil, fmt.Errorf("%w: job queue %s is %s", ErrValidation, jobQueue, stateDisabled)
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	tagsCopy := tagsCloneOrEmpty(tags)
	now := time.Now().UnixMilli()
	jobID := uuid.NewString()
	jobARN := arn.Build("batch", region, b.accountID, "service-job/"+jobID)

	var timeoutCopy *ServiceJobTimeout
	if timeoutConfig != nil {
		tc := *timeoutConfig
		timeoutCopy = &tc
	}

	sj := &ServiceJob{
		region:                region,
		JobID:                 jobID,
		JobArn:                jobARN,
		JobName:               name,
		JobQueue:              jq.JobQueueArn,
		ServiceJobType:        serviceJobType,
		ServiceRequestPayload: serviceRequestPayload,
		Status:                jobStatusSubmitted,
		CreatedAt:             now,
		Tags:                  tagsCopy,
		RetryStrategy:         cloneServiceJobRetryStrategy(retryStrategy),
		TimeoutConfig:         timeoutCopy,
		SchedulingPriority:    schedulingPriority,
		ShareIdentifier:       shareIdentifier,
	}
	b.serviceJobs.Put(sj)
	cp := *sj

	return &cp, nil
}

// DescribeServiceJob returns a single service job by ID.
func (b *InMemoryBackend) DescribeServiceJob(ctx context.Context, jobID string) (*ServiceJob, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeServiceJob")
	defer b.mu.RUnlock()

	sj, ok := b.serviceJobs.Get(regionKey(region, jobID))
	if !ok {
		return nil, fmt.Errorf("%w: service job %s not found", ErrNotFound, jobID)
	}

	cp := *sj
	cp.Tags = tagsCloneOrEmpty(sj.Tags)

	return &cp, nil
}

// ListServiceJobs returns service jobs for a job queue, optionally filtered
// by status. Matching real AWS Batch's documented ListServiceJobs behavior,
// an unspecified jobStatus defaults to returning only RUNNING jobs.
func (b *InMemoryBackend) ListServiceJobs(ctx context.Context, jobQueue, jobStatus string) ([]*ServiceJob, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListServiceJobs")
	defer b.mu.RUnlock()

	var queueARN string

	if jobQueue != "" {
		jq, ok := b.lookupJQByNameOrARN(region, jobQueue)
		if !ok {
			return nil, fmt.Errorf("%w: job queue %s not found", ErrNotFound, jobQueue)
		}

		queueARN = jq.JobQueueArn
	}

	wantStatus := jobStatus
	if wantStatus == "" {
		wantStatus = jobStatusRunning
	}

	group := b.serviceJobsByRegion.Get(region)
	list := make([]*ServiceJob, 0, len(group))

	for _, sj := range group {
		if queueARN != "" && sj.JobQueue != queueARN {
			continue
		}

		if sj.Status != wantStatus {
			continue
		}

		cp := *sj
		cp.Tags = tagsCloneOrEmpty(sj.Tags)
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].CreatedAt > list[j].CreatedAt })

	return list, nil
}

// TerminateServiceJob marks a service job as FAILED.
func (b *InMemoryBackend) TerminateServiceJob(ctx context.Context, jobID, reason string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("TerminateServiceJob")
	defer b.mu.Unlock()

	sj, ok := b.serviceJobs.Get(regionKey(region, jobID))
	if !ok {
		return fmt.Errorf("%w: service job %s not found", ErrNotFound, jobID)
	}

	now := time.Now().UnixMilli()
	sj.Status = jobStatusFailed
	sj.StatusReason = reason
	sj.StoppedAt = &now
	sj.IsTerminated = true

	return nil
}
