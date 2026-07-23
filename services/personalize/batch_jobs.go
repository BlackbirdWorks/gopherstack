package personalize

import (
	"fmt"
	"time"
)

// --- BatchInferenceJob ---

// requireJobName rejects an empty batch/async job name, matching the
// "jobName is required" validation shared by every Create*Job op in this
// file.
func requireJobName(jobName string) error {
	if jobName == "" {
		return fmt.Errorf("%w: jobName is required", ErrValidation)
	}

	return nil
}

// requireSolutionVersion FK-validates that solutionVersionArn resolves to a
// real solution version, shared by CreateBatchInferenceJob and
// CreateBatchSegmentJob (both key their training source off a
// solutionVersionArn).
func (b *InMemoryBackend) requireSolutionVersion(solutionVersionArn string) error {
	if !b.solutionVersions.Has(solutionVersionArn) {
		return fmt.Errorf("%w: solution version %q not found", ErrNotFound, solutionVersionArn)
	}

	return nil
}

// CreateBatchInferenceJob creates a new batch inference job.
//
//nolint:dupl // structurally identical to CreateBatchSegmentJob by design; different resource types
func (b *InMemoryBackend) CreateBatchInferenceJob(
	jobName, solutionVersionArn, roleArn string,
	jobInput, jobOutput map[string]any,
	tags map[string]string,
) (*BatchInferenceJob, error) {
	b.mu.Lock("CreateBatchInferenceJob")
	defer b.mu.Unlock()

	if err := requireJobName(jobName); err != nil {
		return nil, err
	}
	if err := b.requireSolutionVersion(solutionVersionArn); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	jobArn := b.personalizeARN("batch-inference-job", jobName)
	job := &BatchInferenceJob{
		BatchInferenceJobArn: jobArn,
		JobName:              jobName,
		SolutionVersionArn:   solutionVersionArn,
		RoleArn:              roleArn,
		JobInput:             jobInput,
		JobOutput:            jobOutput,
		Status:               statusActive,
		CreationDateTime:     now,
		LastUpdatedDateTime:  now,
	}
	b.batchInferenceJobs.Put(job)
	if len(tags) > 0 {
		b.tags[jobArn] = copyStringMap(tags)
	}

	return job, nil
}

// DescribeBatchInferenceJob returns a batch inference job by ARN.
func (b *InMemoryBackend) DescribeBatchInferenceJob(jobArn string) (*BatchInferenceJob, error) {
	b.mu.RLock("DescribeBatchInferenceJob")
	defer b.mu.RUnlock()

	job, ok := b.batchInferenceJobs.Get(jobArn)
	if !ok {
		return nil, fmt.Errorf("%w: batch inference job %q not found", ErrNotFound, jobArn)
	}

	return job, nil
}

// ListBatchInferenceJobs returns batch inference jobs, optionally filtered by solution version ARN.
func (b *InMemoryBackend) ListBatchInferenceJobs(
	solutionVersionArn string,
	maxResults int,
	nextToken string,
) ([]*BatchInferenceJob, string) {
	b.mu.RLock("ListBatchInferenceJobs")
	defer b.mu.RUnlock()

	all := b.batchInferenceJobs.Snapshot()
	filtered := make([]*BatchInferenceJob, 0, len(all))
	for _, job := range all {
		if solutionVersionArn == "" || job.SolutionVersionArn == solutionVersionArn {
			filtered = append(filtered, job)
		}
	}

	return paginateItems(filtered, batchInferenceJobKeyFn, maxResults, nextToken)
}

// --- BatchSegmentJob ---

// CreateBatchSegmentJob creates a new batch segment job.
//
//nolint:dupl // structurally identical to CreateBatchInferenceJob by design; different resource types
func (b *InMemoryBackend) CreateBatchSegmentJob(
	jobName, solutionVersionArn, roleArn string,
	jobInput, jobOutput map[string]any,
	tags map[string]string,
) (*BatchSegmentJob, error) {
	b.mu.Lock("CreateBatchSegmentJob")
	defer b.mu.Unlock()

	if err := requireJobName(jobName); err != nil {
		return nil, err
	}
	if err := b.requireSolutionVersion(solutionVersionArn); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	jobArn := b.personalizeARN("batch-segment-job", jobName)
	job := &BatchSegmentJob{
		BatchSegmentJobArn:  jobArn,
		JobName:             jobName,
		SolutionVersionArn:  solutionVersionArn,
		RoleArn:             roleArn,
		JobInput:            jobInput,
		JobOutput:           jobOutput,
		Status:              statusActive,
		CreationDateTime:    now,
		LastUpdatedDateTime: now,
	}
	b.batchSegmentJobs.Put(job)
	if len(tags) > 0 {
		b.tags[jobArn] = copyStringMap(tags)
	}

	return job, nil
}

// DescribeBatchSegmentJob returns a batch segment job by ARN.
func (b *InMemoryBackend) DescribeBatchSegmentJob(jobArn string) (*BatchSegmentJob, error) {
	b.mu.RLock("DescribeBatchSegmentJob")
	defer b.mu.RUnlock()

	job, ok := b.batchSegmentJobs.Get(jobArn)
	if !ok {
		return nil, fmt.Errorf("%w: batch segment job %q not found", ErrNotFound, jobArn)
	}

	return job, nil
}

// ListBatchSegmentJobs returns batch segment jobs, optionally filtered by solution version ARN.
func (b *InMemoryBackend) ListBatchSegmentJobs(
	solutionVersionArn string,
	maxResults int,
	nextToken string,
) ([]*BatchSegmentJob, string) {
	b.mu.RLock("ListBatchSegmentJobs")
	defer b.mu.RUnlock()

	all := b.batchSegmentJobs.Snapshot()
	filtered := make([]*BatchSegmentJob, 0, len(all))
	for _, job := range all {
		if solutionVersionArn == "" || job.SolutionVersionArn == solutionVersionArn {
			filtered = append(filtered, job)
		}
	}

	return paginateItems(filtered, batchSegmentJobKeyFn, maxResults, nextToken)
}
