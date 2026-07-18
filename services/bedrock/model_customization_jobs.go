package bedrock

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// newCustomizationJobID generates a unique customization job ID.
func (b *InMemoryBackend) newCustomizationJobID() string {
	b.customizationJobCounter++

	return fmt.Sprintf("cj-%07d", b.customizationJobCounter)
}

// CreateModelCustomizationJob creates a new model customization job.
func (b *InMemoryBackend) CreateModelCustomizationJob(
	jobName, baseModelID, customizationType string,
	tags []Tag,
) (*ModelCustomizationJob, error) {
	b.mu.Lock("CreateModelCustomizationJob")
	defer b.mu.Unlock()

	if jobName == "" {
		return nil, fmt.Errorf("%w: jobName is required", ErrValidation)
	}

	if _, exists := b.customizationJobsByName[jobName]; exists {
		return nil, fmt.Errorf("%w: customization job %s already exists", ErrAlreadyExists, jobName)
	}

	id := b.newCustomizationJobID()
	jobARN := arn.Build("bedrock", b.region, b.accountID, "model-customization-job/"+id)
	outputModelARN := arn.Build("bedrock", b.region, b.accountID, "custom-model/output-"+id)
	baseModelARN := arn.Build("bedrock", b.region, b.accountID, "foundation-model/"+baseModelID)
	now := time.Now().UTC()

	job := &ModelCustomizationJob{
		JobArn:            jobARN,
		JobName:           jobName,
		BaseModelArn:      baseModelARN,
		OutputModelArn:    outputModelARN,
		Status:            statusInProgress,
		CustomizationType: customizationType,
		CreationTime:      now,
		LastModifiedTime:  now,
		Tags:              copyTags(tags),
	}
	b.modelCustomizationJobs.Put(job)
	b.customizationJobsByName[jobName] = jobARN
	cp := *job
	cp.Tags = copyTags(job.Tags)

	return &cp, nil
}

// findCustomizationJobARN resolves a job ID or name to its ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findCustomizationJobARN(idOrARN string) (string, bool) {
	if _, ok := b.modelCustomizationJobs.Get(idOrARN); ok {
		return idOrARN, true
	}

	if a := b.customizationJobsByName[idOrARN]; a != "" {
		return a, true
	}

	return "", false
}

// GetModelCustomizationJob returns a model customization job by ARN or name.
func (b *InMemoryBackend) GetModelCustomizationJob(idOrARN string) (*ModelCustomizationJob, error) {
	b.mu.RLock("GetModelCustomizationJob")
	defer b.mu.RUnlock()

	jobARN, ok := b.findCustomizationJobARN(idOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: model customization job %s not found", ErrNotFound, idOrARN)
	}

	j, _ := b.modelCustomizationJobs.Get(jobARN)
	cp := *j
	cp.Tags = copyTags(j.Tags)

	return &cp, nil
}

// ListModelCustomizationJobs returns all customization jobs with optional pagination.
func (b *InMemoryBackend) ListModelCustomizationJobs(
	nextToken string,
) ([]*ModelCustomizationJob, string) {
	b.mu.RLock("ListModelCustomizationJobs")
	defer b.mu.RUnlock()

	list := make([]*ModelCustomizationJob, 0, b.modelCustomizationJobs.Len())

	for _, j := range b.modelCustomizationJobs.All() {
		cp := *j
		cp.Tags = copyTags(j.Tags)
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].JobArn < list[j].JobArn })

	return paginateBedrockSlice(list, nextToken)
}

// StopModelCustomizationJob stops a running customization job.
func (b *InMemoryBackend) StopModelCustomizationJob(idOrARN string) error {
	b.mu.Lock("StopModelCustomizationJob")
	defer b.mu.Unlock()

	jobARN, ok := b.findCustomizationJobARN(idOrARN)
	if !ok {
		return fmt.Errorf("%w: model customization job %s not found", ErrNotFound, idOrARN)
	}

	j, _ := b.modelCustomizationJobs.Get(jobARN)
	j.Status = statusStopped

	return nil
}

// AdvanceCustomizationJobStatuses moves InProgress customization jobs to Completed.
// Called by the janitor after the simulated training delay has elapsed.
func (b *InMemoryBackend) AdvanceCustomizationJobStatuses(minAge time.Duration) int {
	b.mu.Lock("AdvanceCustomizationJobStatuses")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	advanced := 0

	for _, job := range b.modelCustomizationJobs.All() {
		if job.Status != statusInProgress {
			continue
		}

		if now.Sub(job.CreationTime) >= minAge {
			job.Status = statusCompleted
			job.LastModifiedTime = now
			job.EndTime = now
			advanced++
		}
	}

	return advanced
}
