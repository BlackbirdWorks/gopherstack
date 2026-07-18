package bedrock

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// CreateModelCopyJob creates a new model copy job.
//
//nolint:dupl // Identical structure to CreateModelImportJob; different types.
func (b *InMemoryBackend) CreateModelCopyJob(
	sourceModelARN string,
	tags []Tag,
) (*ModelCopyJob, error) {
	if sourceModelARN == "" {
		return nil, fmt.Errorf("%w: sourceModelArn is required", ErrValidation)
	}

	b.mu.Lock("CreateModelCopyJob")
	defer b.mu.Unlock()

	b.copyJobCounter++
	id := fmt.Sprintf("mcj-%07d", b.copyJobCounter)
	jobARN := arn.Build("bedrock", b.region, b.accountID, "model-copy-job/"+id)
	targetModelARN := arn.Build("bedrock", b.region, b.accountID, "custom-model/copy-"+id)
	now := time.Now().UTC()

	job := &ModelCopyJob{
		JobArn:           jobARN,
		SourceModelArn:   sourceModelARN,
		TargetModelArn:   targetModelARN,
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

// ListModelCopyJobs returns all model copy jobs sorted by creation time.
func (b *InMemoryBackend) ListModelCopyJobs() []*ModelCopyJob {
	b.mu.RLock("ListModelCopyJobs")
	defer b.mu.RUnlock()

	list := make([]*ModelCopyJob, 0, b.modelCopyJobs.Len())

	for _, j := range b.modelCopyJobs.All() {
		cp := *j
		cp.Tags = copyTags(j.Tags)
		list = append(list, &cp)
	}

	sort.Slice(
		list,
		func(i, k int) bool { return list[i].CreationTime.Before(list[k].CreationTime) },
	)

	return list
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
