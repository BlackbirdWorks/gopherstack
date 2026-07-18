package bedrock

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// CreateModelImportJob creates a new model import job.
//
//nolint:dupl // Identical structure to CreateModelCopyJob; different types.
func (b *InMemoryBackend) CreateModelImportJob(
	jobName string,
	tags []Tag,
) (*ModelImportJob, error) {
	if jobName == "" {
		return nil, fmt.Errorf("%w: jobName is required", ErrValidation)
	}

	b.mu.Lock("CreateModelImportJob")
	defer b.mu.Unlock()

	b.importJobCounter++
	id := fmt.Sprintf("mij-%07d", b.importJobCounter)
	jobARN := arn.Build("bedrock", b.region, b.accountID, "model-import-job/"+id)
	importedModelARN := arn.Build("bedrock", b.region, b.accountID, "imported-model/"+id)
	now := time.Now().UTC()

	job := &ModelImportJob{
		JobArn:           jobARN,
		JobName:          jobName,
		ImportedModelArn: importedModelARN,
		Status:           statusInProgress,
		CreationTime:     now,
		LastModifiedTime: now,
		Tags:             copyTags(tags),
	}
	b.modelImportJobs.Put(job)

	cp := *job
	cp.Tags = copyTags(job.Tags)

	return &cp, nil
}

// GetModelImportJob returns a model import job by ARN.
func (b *InMemoryBackend) GetModelImportJob(jobARN string) (*ModelImportJob, error) {
	b.mu.RLock("GetModelImportJob")
	defer b.mu.RUnlock()

	job, ok := b.modelImportJobs.Get(jobARN)
	if !ok {
		return nil, fmt.Errorf("%w: model import job %s not found", ErrNotFound, jobARN)
	}

	cp := *job
	cp.Tags = copyTags(job.Tags)

	return &cp, nil
}

// ListModelImportJobs returns all model import jobs sorted by creation time.
func (b *InMemoryBackend) ListModelImportJobs() []*ModelImportJob {
	b.mu.RLock("ListModelImportJobs")
	defer b.mu.RUnlock()

	list := make([]*ModelImportJob, 0, b.modelImportJobs.Len())

	for _, j := range b.modelImportJobs.All() {
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

// GetImportedModel returns the import job whose importedModelArn matches.
func (b *InMemoryBackend) GetImportedModel(modelARN string) (*ModelImportJob, error) {
	b.mu.RLock("GetImportedModel")
	defer b.mu.RUnlock()

	for _, j := range b.modelImportJobs.All() {
		if j.ImportedModelArn == modelARN {
			cp := *j
			cp.Tags = copyTags(j.Tags)

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: imported model %s not found", ErrNotFound, modelARN)
}

// ListImportedModels returns the import jobs that have completed and have an imported model ARN.
func (b *InMemoryBackend) ListImportedModels() []*ModelImportJob {
	b.mu.RLock("ListImportedModels")
	defer b.mu.RUnlock()

	var models []*ModelImportJob
	for _, j := range b.modelImportJobs.All() {
		if j.ImportedModelArn != "" {
			cp := *j
			cp.Tags = copyTags(j.Tags)
			models = append(models, &cp)
		}
	}

	sort.Slice(models, func(i, k int) bool {
		return models[i].CreationTime.Before(models[k].CreationTime)
	})

	return models
}

// DeleteImportedModel removes the import job whose importedModelArn matches.
func (b *InMemoryBackend) DeleteImportedModel(modelARN string) error {
	b.mu.Lock("DeleteImportedModel")
	defer b.mu.Unlock()

	for _, j := range b.modelImportJobs.All() {
		if j.ImportedModelArn == modelARN {
			b.modelImportJobs.Delete(j.JobArn)

			return nil
		}
	}

	return fmt.Errorf("%w: imported model %s not found", ErrNotFound, modelARN)
}
