package batch

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	maxServiceJobNameLength = 128

	// maxServiceJobSchedulingPriority is the documented upper bound for
	// UpdateServiceJobInput.SchedulingPriority: minimum supported value 0,
	// maximum supported value 9999.
	maxServiceJobSchedulingPriority = 9999
)

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
	shareIdentifier, quotaShareName string,
	preemptionConfig *ServiceJobPreemptionConfiguration,
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

	var preemptionCopy *ServiceJobPreemptionConfiguration
	if preemptionConfig != nil {
		pc := *preemptionConfig
		preemptionCopy = &pc
	}

	sj := &ServiceJob{
		region:                  region,
		JobID:                   jobID,
		JobArn:                  jobARN,
		JobName:                 name,
		JobQueue:                jq.JobQueueArn,
		ServiceJobType:          serviceJobType,
		ServiceRequestPayload:   serviceRequestPayload,
		Status:                  jobStatusSubmitted,
		CreatedAt:               now,
		Tags:                    tagsCopy,
		RetryStrategy:           cloneServiceJobRetryStrategy(retryStrategy),
		TimeoutConfig:           timeoutCopy,
		SchedulingPriority:      schedulingPriority,
		ShareIdentifier:         shareIdentifier,
		QuotaShareName:          quotaShareName,
		PreemptionConfiguration: preemptionCopy,
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

// serviceJobMatchesFilterValue implements the ListServiceJobs Filters
// vocabulary (api_op_ListServiceJobs.go): JOB_NAME (case-insensitive,
// trailing '*' prefix), SHARE_IDENTIFIER, QUOTA_SHARE_NAME (both exact),
// BEFORE_CREATED_AT/AFTER_CREATED_AT (epoch-ms comparisons).
func serviceJobMatchesFilterValue(sj *ServiceJob, name, v string) bool {
	switch name {
	case filterJobName:
		return filterValueMatches(sj.JobName, v, true)
	case filterShareIdentifier:
		return sj.ShareIdentifier == v
	case filterQuotaShareName:
		return sj.QuotaShareName == v
	case filterBeforeCreatedAt:
		ms, err := strconv.ParseInt(v, 10, 64)

		return err == nil && sj.CreatedAt < ms
	case filterAfterCreatedAt:
		ms, err := strconv.ParseInt(v, 10, 64)

		return err == nil && sj.CreatedAt > ms
	default:
		return false
	}
}

// serviceJobMatchesFilter reports whether sj satisfies a single
// KeyValueFilter entry; Values within one entry are OR'd.
func serviceJobMatchesFilter(sj *ServiceJob, f KeyValueFilter) bool {
	for _, v := range f.Values {
		if serviceJobMatchesFilterValue(sj, f.Name, v) {
			return true
		}
	}

	return false
}

// serviceJobFiltersStatusExempt reports whether filters, if non-empty,
// consists solely of SHARE_IDENTIFIER/QUOTA_SHARE_NAME entries -- the two
// documented exceptions where jobStatus still applies alongside filters
// (api_op_ListServiceJobs.go).
func serviceJobFiltersStatusExempt(filters []KeyValueFilter) bool {
	exempt := len(filters) > 0

	for _, f := range filters {
		if f.Name != filterShareIdentifier && f.Name != filterQuotaShareName {
			return false
		}
	}

	return exempt
}

// selectServiceJobs applies the queue/status/filters selection rules shared
// by ListServiceJobs, returning matches sorted newest-first.
func selectServiceJobs(
	group []*ServiceJob,
	queueARN, wantStatus string,
	applyStatus bool,
	filters []KeyValueFilter,
) []*ServiceJob {
	all := make([]*ServiceJob, 0, len(group))

	for _, sj := range group {
		if queueARN != "" && sj.JobQueue != queueARN {
			continue
		}

		if applyStatus && sj.Status != wantStatus {
			continue
		}

		matched := true

		for _, f := range filters {
			if !serviceJobMatchesFilter(sj, f) {
				matched = false

				break
			}
		}

		if matched {
			all = append(all, sj)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].CreatedAt > all[j].CreatedAt })

	return all
}

// ListServiceJobs returns service jobs for a job queue, optionally filtered
// by status and/or filters. Matching real AWS Batch's documented
// ListServiceJobs behavior (api_op_ListServiceJobs.go): an unspecified
// jobStatus defaults to RUNNING; when filters is non-empty, status is
// ignored (jobs of any status are returned) unless every filter entry is
// SHARE_IDENTIFIER or QUOTA_SHARE_NAME, the two documented exceptions where
// status and filters combine. Pagination is controlled via maxResults and
// nextToken (token encodes an integer offset).
func (b *InMemoryBackend) ListServiceJobs(
	ctx context.Context,
	jobQueue, jobStatus, nextToken string,
	maxResults int32,
	filters []KeyValueFilter,
) ([]*ServiceJob, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListServiceJobs")
	defer b.mu.RUnlock()

	var queueARN string

	if jobQueue != "" {
		jq, ok := b.lookupJQByNameOrARN(region, jobQueue)
		if !ok {
			return nil, "", fmt.Errorf("%w: job queue %s not found", ErrNotFound, jobQueue)
		}

		queueARN = jq.JobQueueArn
	}

	applyStatus := len(filters) == 0 || serviceJobFiltersStatusExempt(filters)

	wantStatus := jobStatus
	if wantStatus == "" {
		wantStatus = jobStatusRunning
	}

	all := selectServiceJobs(b.serviceJobsByRegion.Get(region), queueARN, wantStatus, applyStatus, filters)

	byID := make(map[string]*ServiceJob, len(all))
	keys := make([]string, 0, len(all))

	for _, sj := range all {
		byID[sj.JobID] = sj
		keys = append(keys, sj.JobID)
	}

	pageKeys, next := paginateMapKeys(keys, nextToken, maxResults)

	out := make([]*ServiceJob, 0, len(pageKeys))
	for _, k := range pageKeys {
		sj := byID[k]
		cp := *sj
		cp.Tags = tagsCloneOrEmpty(sj.Tags)
		out = append(out, &cp)
	}

	return out, next, nil
}

// UpdateServiceJob updates the scheduling priority of an existing service
// job. Real AWS Batch's UpdateServiceJobInput has exactly two fields --
// jobId and schedulingPriority, both required -- and no others (see
// aws-sdk-go-v2/service/batch's UpdateServiceJobInput/UpdateServiceJob doc
// comment: "Updates the priority of a specified service job"); there is no
// way to change jobQueue, retryStrategy, tags, or any other field of a
// submitted service job via this or any other operation. Matching the
// terminal-state guard CancelJob already applies to regular jobs (see
// jobs.go), a service job that has already reached a terminal status
// (SUCCEEDED or FAILED) rejects the update: scheduling priority only
// affects a job's position within a quota-share/fair-share queue while it
// is still competing for capacity, which no longer applies once the job has
// finished.
func (b *InMemoryBackend) UpdateServiceJob(
	ctx context.Context,
	jobID string,
	schedulingPriority int32,
) (*ServiceJob, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateServiceJob")
	defer b.mu.Unlock()

	sj, ok := b.serviceJobs.Get(regionKey(region, jobID))
	if !ok {
		return nil, fmt.Errorf("%w: service job %s not found", ErrNotFound, jobID)
	}

	if sj.Status == jobStatusSucceeded || sj.Status == jobStatusFailed {
		return nil, fmt.Errorf(
			"%w: service job %s is already in terminal state %s", ErrValidation, jobID, sj.Status,
		)
	}

	if schedulingPriority < 0 || schedulingPriority > maxServiceJobSchedulingPriority {
		return nil, fmt.Errorf(
			"%w: schedulingPriority must be between 0 and %d", ErrValidation, maxServiceJobSchedulingPriority,
		)
	}

	sj.SchedulingPriority = schedulingPriority

	cp := *sj
	cp.Tags = tagsCloneOrEmpty(sj.Tags)

	return &cp, nil
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
