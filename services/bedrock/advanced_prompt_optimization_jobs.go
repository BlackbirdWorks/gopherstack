package bedrock

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// maxAdvancedPromptOptimizationModelConfigs is the upper bound AWS enforces
// on CreateAdvancedPromptOptimizationJobInput.ModelConfigurations ("Array
// Members: Minimum number of 1 item. Maximum number of 5 items.").
const maxAdvancedPromptOptimizationModelConfigs = 5

// maxAdvancedPromptOptimizationBatchDelete is the upper bound AWS enforces on
// BatchDeleteAdvancedPromptOptimizationJobInput.JobIdentifiers ("Array
// Members: Minimum number of 1 item. Maximum number of 25 items.").
const maxAdvancedPromptOptimizationBatchDelete = 25

// newAdvancedPromptOptimizationJobID generates a unique advanced prompt
// optimization job ID. Real AWS job ARNs embed a 12-character lowercase
// alphanumeric ID; gopherstack instead uses a short, predictable, zero-padded
// sequence consistent with every other job family in this package (see
// newCustomizationJobID, newEvaluationJobID).
func (b *InMemoryBackend) newAdvancedPromptOptimizationJobID() string {
	b.advancedPromptOptJobCounter++

	return fmt.Sprintf("apoj-%07d", b.advancedPromptOptJobCounter)
}

// validateCreateAdvancedPromptOptimizationJobInput checks the fields real
// AWS documents as "This member is required" on
// CreateAdvancedPromptOptimizationJobInput (jobName, inputConfig.s3Uri,
// outputConfig.s3Uri, modelConfigurations 1-5 items each with a modelId).
func validateCreateAdvancedPromptOptimizationJobInput(in CreateAdvancedPromptOptimizationJobInput) error {
	if in.JobName == "" {
		return fmt.Errorf("%w: jobName is required", ErrValidation)
	}
	if in.InputConfig.S3URI == "" {
		return fmt.Errorf("%w: inputConfig.s3Uri is required", ErrValidation)
	}
	if in.OutputConfig.S3URI == "" {
		return fmt.Errorf("%w: outputConfig.s3Uri is required", ErrValidation)
	}
	if len(in.ModelConfigurations) == 0 {
		return fmt.Errorf("%w: modelConfigurations must contain at least 1 item", ErrValidation)
	}
	if len(in.ModelConfigurations) > maxAdvancedPromptOptimizationModelConfigs {
		return fmt.Errorf(
			"%w: modelConfigurations must contain at most %d items",
			ErrValidation, maxAdvancedPromptOptimizationModelConfigs,
		)
	}
	for _, mc := range in.ModelConfigurations {
		if mc.ModelID == "" {
			return fmt.Errorf("%w: modelConfigurations[].modelId is required", ErrValidation)
		}
	}

	return nil
}

// copyModelConfigurations returns a shallow copy of the slice, mirroring the
// copy-on-read convention this package uses for every other resource slice
// (see copyTags).
func copyModelConfigurations(src []ModelConfiguration) []ModelConfiguration {
	if len(src) == 0 {
		return nil
	}

	dst := make([]ModelConfiguration, len(src))
	copy(dst, src)

	return dst
}

// CreateAdvancedPromptOptimizationJob creates a new advanced prompt
// optimization job in status InProgress.
func (b *InMemoryBackend) CreateAdvancedPromptOptimizationJob(
	in CreateAdvancedPromptOptimizationJobInput,
) (*AdvancedPromptOptimizationJob, error) {
	b.mu.Lock("CreateAdvancedPromptOptimizationJob")
	defer b.mu.Unlock()

	if err := validateCreateAdvancedPromptOptimizationJobInput(in); err != nil {
		return nil, err
	}

	id := b.newAdvancedPromptOptimizationJobID()
	jobARN := arn.Build("bedrock", b.region, b.accountID, "advanced-prompt-optimization-job/"+id)
	now := time.Now().UTC()

	job := &AdvancedPromptOptimizationJob{
		JobArn:              jobARN,
		JobName:             in.JobName,
		JobDescription:      in.JobDescription,
		JobStatus:           statusInProgress,
		EncryptionKeyArn:    in.EncryptionKeyArn,
		InputConfig:         in.InputConfig,
		OutputConfig:        in.OutputConfig,
		ModelConfigurations: copyModelConfigurations(in.ModelConfigurations),
		Tags:                copyTags(in.Tags),
		CreationTime:        now,
		LastModifiedTime:    now,
	}
	b.advancedPromptOptimizationJobs.Put(job)

	cp := *job
	cp.Tags = copyTags(job.Tags)
	cp.ModelConfigurations = copyModelConfigurations(job.ModelConfigurations)

	return &cp, nil
}

// findAdvancedPromptOptimizationJobARN resolves a job ID or ARN (real AWS's
// JobIdentifier accepts either) to its full ARN. Caller must hold at least a
// read lock.
func (b *InMemoryBackend) findAdvancedPromptOptimizationJobARN(idOrARN string) (string, bool) {
	if b.advancedPromptOptimizationJobs.Has(idOrARN) {
		return idOrARN, true
	}

	candidate := arn.Build("bedrock", b.region, b.accountID, "advanced-prompt-optimization-job/"+idOrARN)
	if b.advancedPromptOptimizationJobs.Has(candidate) {
		return candidate, true
	}

	return "", false
}

// GetAdvancedPromptOptimizationJob returns a single job by ARN or ID.
func (b *InMemoryBackend) GetAdvancedPromptOptimizationJob(idOrARN string) (*AdvancedPromptOptimizationJob, error) {
	b.mu.RLock("GetAdvancedPromptOptimizationJob")
	defer b.mu.RUnlock()

	jobARN, ok := b.findAdvancedPromptOptimizationJobARN(idOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: advanced prompt optimization job %s not found", ErrNotFound, idOrARN)
	}

	j, _ := b.advancedPromptOptimizationJobs.Get(jobARN)
	cp := *j
	cp.Tags = copyTags(j.Tags)
	cp.ModelConfigurations = copyModelConfigurations(j.ModelConfigurations)

	return &cp, nil
}

// ListAdvancedPromptOptimizationJobs returns jobs sorted by creation time and
// paginated. in may be nil, matching an unfiltered call with default sort
// order and page size.
func (b *InMemoryBackend) ListAdvancedPromptOptimizationJobs(
	in *ListAdvancedPromptOptimizationJobsInput,
) ([]*AdvancedPromptOptimizationJob, string) {
	b.mu.RLock("ListAdvancedPromptOptimizationJobs")
	defer b.mu.RUnlock()

	jobs := make([]*AdvancedPromptOptimizationJob, 0, b.advancedPromptOptimizationJobs.Len())
	for _, j := range b.advancedPromptOptimizationJobs.All() {
		cp := *j
		cp.Tags = copyTags(j.Tags)
		cp.ModelConfigurations = copyModelConfigurations(j.ModelConfigurations)
		jobs = append(jobs, &cp)
	}

	descending := in != nil && in.SortOrder == sortOrderDescending
	sort.Slice(jobs, func(i, k int) bool {
		if !jobs[i].CreationTime.Equal(jobs[k].CreationTime) {
			if descending {
				return jobs[i].CreationTime.After(jobs[k].CreationTime)
			}

			return jobs[i].CreationTime.Before(jobs[k].CreationTime)
		}

		return jobs[i].JobArn < jobs[k].JobArn
	})

	if in == nil {
		jobs, _ = paginate(jobs, 0, "")

		return jobs, ""
	}

	return paginate(jobs, int(in.MaxResults), in.NextToken)
}

// StopAdvancedPromptOptimizationJob stops a running job. Real AWS transitions
// through an intermediate "Stopping" status before settling on "Stopped";
// this backend follows the same simplification every other Stop* op in this
// package already makes (StopModelCustomizationJob, StopEvaluationJob,
// StopModelInvocationJob) and transitions directly to the terminal status,
// since neither transition fabricates any data the API doesn't already ask
// gopherstack to model.
func (b *InMemoryBackend) StopAdvancedPromptOptimizationJob(idOrARN string) error {
	b.mu.Lock("StopAdvancedPromptOptimizationJob")
	defer b.mu.Unlock()

	jobARN, ok := b.findAdvancedPromptOptimizationJobARN(idOrARN)
	if !ok {
		return fmt.Errorf("%w: advanced prompt optimization job %s not found", ErrNotFound, idOrARN)
	}

	j, _ := b.advancedPromptOptimizationJobs.Get(jobARN)
	if j.JobStatus != statusInProgress {
		return fmt.Errorf(
			"%w: advanced prompt optimization job %s cannot be stopped in status %s",
			ErrValidation, idOrARN, j.JobStatus,
		)
	}

	j.JobStatus = statusStopped
	j.LastModifiedTime = time.Now().UTC()

	return nil
}

// BatchDeleteAdvancedPromptOptimizationJob deletes multiple jobs by ARN or
// ID, returning a per-item result/error list mirroring
// BatchDeleteEvaluationJob's shape.
func (b *InMemoryBackend) BatchDeleteAdvancedPromptOptimizationJob(jobIdentifiers []string) (
	[]BatchDeleteAdvancedPromptOptimizationJobItem, []BatchDeleteAdvancedPromptOptimizationJobError, error,
) {
	b.mu.Lock("BatchDeleteAdvancedPromptOptimizationJob")
	defer b.mu.Unlock()

	if len(jobIdentifiers) == 0 {
		return nil, nil, fmt.Errorf("%w: jobIdentifiers must not be empty", ErrValidation)
	}
	if len(jobIdentifiers) > maxAdvancedPromptOptimizationBatchDelete {
		return nil, nil, fmt.Errorf(
			"%w: jobIdentifiers must contain at most %d items",
			ErrValidation, maxAdvancedPromptOptimizationBatchDelete,
		)
	}

	deleted := make([]BatchDeleteAdvancedPromptOptimizationJobItem, 0, len(jobIdentifiers))

	var errs []BatchDeleteAdvancedPromptOptimizationJobError

	for _, id := range jobIdentifiers {
		jobARN, ok := b.findAdvancedPromptOptimizationJobARN(id)
		if !ok {
			errs = append(errs, BatchDeleteAdvancedPromptOptimizationJobError{
				JobIdentifier: id,
				Code:          "ResourceNotFoundException",
				Message:       fmt.Sprintf("advanced prompt optimization job %s not found", id),
			})

			continue
		}

		b.advancedPromptOptimizationJobs.Delete(jobARN)
		deleted = append(deleted, BatchDeleteAdvancedPromptOptimizationJobItem{
			JobIdentifier: id,
			JobStatus:     "Deleting",
		})
	}

	if errs == nil {
		errs = []BatchDeleteAdvancedPromptOptimizationJobError{}
	}

	return deleted, errs, nil
}

// AdvanceAdvancedPromptOptimizationJobStatuses moves InProgress jobs to
// Completed once minAge has elapsed since creation. Called by the janitor,
// mirroring AdvanceCustomizationJobStatuses.
func (b *InMemoryBackend) AdvanceAdvancedPromptOptimizationJobStatuses(minAge time.Duration) int {
	b.mu.Lock("AdvanceAdvancedPromptOptimizationJobStatuses")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	advanced := 0

	for _, j := range b.advancedPromptOptimizationJobs.All() {
		if j.JobStatus != statusInProgress {
			continue
		}

		if now.Sub(j.CreationTime) >= minAge {
			j.JobStatus = statusCompleted
			j.LastModifiedTime = now
			advanced++
		}
	}

	return advanced
}
