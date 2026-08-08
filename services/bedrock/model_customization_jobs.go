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

// ListModelCustomizationJobs returns customization jobs matching in's filters,
// sorted and paginated. in may be nil, matching an unfiltered call.
// Structurally similar to ListEvaluationJobs/ListModelInvocationJobs (same
// filter/sort/paginate shape) but over a distinct resource type and filter
// set; see matchesCustomizationJobFilter.
//
//nolint:dupl // see doc comment above.
func (b *InMemoryBackend) ListModelCustomizationJobs(
	in *ListModelCustomizationJobsInput,
) ([]*ModelCustomizationJob, string) {
	b.mu.RLock("ListModelCustomizationJobs")
	defer b.mu.RUnlock()

	list := make([]*ModelCustomizationJob, 0, b.modelCustomizationJobs.Len())

	for _, j := range b.modelCustomizationJobs.All() {
		if !matchesCustomizationJobFilter(j, in) {
			continue
		}

		cp := *j
		cp.Tags = copyTags(j.Tags)
		list = append(list, &cp)
	}

	descending := in != nil && in.SortOrder == sortOrderDescending
	sort.Slice(list, func(i, j int) bool {
		if descending {
			return list[i].CreationTime.After(list[j].CreationTime)
		}

		return list[i].CreationTime.Before(list[j].CreationTime)
	})

	nextToken := ""
	if in != nil {
		list, nextToken = paginateBedrockSlice(list, in.NextToken)
	}

	return list, nextToken
}

// matchesCustomizationJobFilter reports whether a job satisfies the list
// filters (statusEquals, nameContains, creationTimeAfter/Before).
func matchesCustomizationJobFilter(j *ModelCustomizationJob, in *ListModelCustomizationJobsInput) bool {
	if in == nil {
		return true
	}
	if in.StatusEquals != "" && j.Status != in.StatusEquals {
		return false
	}
	if in.NameContains != "" && !containsIgnoreCase(j.JobName, in.NameContains) {
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
