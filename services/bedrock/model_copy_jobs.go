package bedrock

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// CreateModelCopyJob creates a new model copy job. TargetModelArn is built
// from the caller's real targetModelName (bedrock@v1.66.4
// serializers.go:1720-1750, "This member is required") -- it must never be
// a fabricated name of this backend's own choosing.
func (b *InMemoryBackend) CreateModelCopyJob(
	sourceModelARN, targetModelName string,
	tags []Tag,
) (*ModelCopyJob, error) {
	if sourceModelARN == "" {
		return nil, fmt.Errorf("%w: sourceModelArn is required", ErrValidation)
	}

	if targetModelName == "" {
		return nil, fmt.Errorf("%w: targetModelName is required", ErrValidation)
	}

	b.mu.Lock("CreateModelCopyJob")
	defer b.mu.Unlock()

	b.copyJobCounter++
	id := fmt.Sprintf("mcj-%07d", b.copyJobCounter)
	jobARN := arn.Build("bedrock", b.region, b.accountID, "model-copy-job/"+id)
	targetModelARN := arn.Build("bedrock", b.region, b.accountID, "custom-model/"+targetModelName)
	now := time.Now().UTC()

	job := &ModelCopyJob{
		JobArn:           jobARN,
		SourceModelArn:   sourceModelARN,
		TargetModelArn:   targetModelARN,
		TargetModelName:  targetModelName,
		Status:           statusInProgress,
		CreationTime:     now,
		LastModifiedTime: now,
		Tags:             copyTags(tags),
	}
	b.modelCopyJobs.Put(job)

	cp := *job
	cp.Tags = copyTags(job.Tags)

	return &cp, nil
}

// GetModelCopyJob returns a model copy job by ARN.
func (b *InMemoryBackend) GetModelCopyJob(jobARN string) (*ModelCopyJob, error) {
	b.mu.RLock("GetModelCopyJob")
	defer b.mu.RUnlock()

	job, ok := b.modelCopyJobs.Get(jobARN)
	if !ok {
		return nil, fmt.Errorf("%w: model copy job %s not found", ErrNotFound, jobARN)
	}

	cp := *job
	cp.Tags = copyTags(job.Tags)

	return &cp, nil
}

// ListModelCopyJobs returns model copy jobs matching in's filters, sorted and
// paginated. in may be nil, matching an unfiltered call. Structurally
// similar to ListModelImportJobs/ListCustomModelDeployments/
// ListProvisionedModelThroughputs (same filter/sort/paginate shape) but over
// a distinct resource type and filter set; see matchesModelCopyJobFilter.
//
//nolint:dupl // see doc comment above.
func (b *InMemoryBackend) ListModelCopyJobs(in *ListModelCopyJobsInput) ([]*ModelCopyJob, string) {
	b.mu.RLock("ListModelCopyJobs")
	defer b.mu.RUnlock()

	list := make([]*ModelCopyJob, 0, b.modelCopyJobs.Len())

	for _, j := range b.modelCopyJobs.All() {
		if !matchesModelCopyJobFilter(j, in) {
			continue
		}

		cp := *j
		cp.Tags = copyTags(j.Tags)
		list = append(list, &cp)
	}

	descending := in != nil && in.SortOrder == sortOrderDescending
	sort.Slice(list, func(i, k int) bool {
		if descending {
			return list[i].CreationTime.After(list[k].CreationTime)
		}

		return list[i].CreationTime.Before(list[k].CreationTime)
	})

	if in == nil {
		list, _ = paginate(list, 0, "")

		return list, ""
	}

	return paginate(list, int(in.MaxResults), in.NextToken)
}

// matchesModelCopyJobFilter reports whether a model copy job satisfies the
// list filters (statusEquals, sourceAccountEquals, sourceModelArnEquals,
// targetModelNameContains, creationTimeAfter/Before).
func matchesModelCopyJobFilter(j *ModelCopyJob, in *ListModelCopyJobsInput) bool {
	if in == nil {
		return true
	}
	if in.StatusEquals != "" && j.Status != in.StatusEquals {
		return false
	}
	if in.SourceAccountEquals != "" && accountIDFromARN(j.SourceModelArn) != in.SourceAccountEquals {
		return false
	}
	if in.SourceModelArnEquals != "" && j.SourceModelArn != in.SourceModelArnEquals {
		return false
	}
	if in.TargetModelNameContains != "" && !containsIgnoreCase(j.TargetModelName, in.TargetModelNameContains) {
		return false
	}
	if in.CreationTimeAfter != nil && !j.CreationTime.After(*in.CreationTimeAfter) {
		return false
	}
	if in.CreationTimeBefore != nil && !j.CreationTime.Before(*in.CreationTimeBefore) {
		return false
	}

	return true
}

// AdvanceCopyImportJobStatuses moves InProgress copy/import jobs to Completed after the min age elapses.
func (b *InMemoryBackend) AdvanceCopyImportJobStatuses(minAge time.Duration) int {
	b.mu.Lock("AdvanceCopyImportJobStatuses")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	advanced := 0

	for _, job := range b.modelCopyJobs.All() {
		if job.Status == statusInProgress && now.Sub(job.CreationTime) >= minAge {
			job.Status = statusCompleted
			job.LastModifiedTime = now
			advanced++
		}
	}

	for _, job := range b.modelImportJobs.All() {
		if job.Status == statusInProgress && now.Sub(job.CreationTime) >= minAge {
			endTime := now
			job.Status = "Complete"
			job.LastModifiedTime = now
			job.EndTime = &endTime
			advanced++
		}
	}

	return advanced
}
