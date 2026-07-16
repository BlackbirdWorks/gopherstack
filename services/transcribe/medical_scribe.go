package transcribe

import (
	"fmt"
	"sort"
	"time"
)

// StartMedicalScribeJob creates a new Medical Scribe job.
func (b *InMemoryBackend) StartMedicalScribeJob(input *MedicalScribeJob) (*MedicalScribeJob, error) {
	if err := validateJobName(input.MedicalScribeJobName); err != nil {
		return nil, fmt.Errorf("%w: MedicalScribeJobName is required", ErrValidation)
	}

	if input.DataAccessRoleArn == "" {
		return nil, fmt.Errorf("%w: DataAccessRoleArn is required for MedicalScribeJob", ErrValidation)
	}

	if input.OutputBucketName == "" {
		return nil, fmt.Errorf("%w: OutputBucketName is required for MedicalScribeJob", ErrValidation)
	}

	b.mu.Lock("StartMedicalScribeJob")
	defer b.mu.Unlock()

	if b.medicalScribeJobs.Has(input.MedicalScribeJobName) {
		return nil, fmt.Errorf(
			"%w: medical scribe job %s already exists",
			ErrAlreadyExists,
			input.MedicalScribeJobName,
		)
	}

	now := time.Now()
	job := *input
	job.MedicalScribeJobStatus = jobStatusCompleted
	job.CreationTime = now
	job.StartTime = now
	job.CompletionTime = now
	b.medicalScribeJobs.Put(&job)
	b.recordResourceTagsLocked(resourceARN(resourceTypeMedicalScribeJob, job.MedicalScribeJobName), job.Tags)

	cp := job

	return &cp, nil
}

// GetMedicalScribeJob returns a Medical Scribe job by name.
func (b *InMemoryBackend) GetMedicalScribeJob(jobName string) (*MedicalScribeJob, error) {
	b.mu.RLock("GetMedicalScribeJob")
	defer b.mu.RUnlock()

	job, ok := b.medicalScribeJobs.Get(jobName)
	if !ok {
		return nil, fmt.Errorf("%w: medical scribe job %s not found", ErrNotFound, jobName)
	}

	cp := *job

	return &cp, nil
}

// ListMedicalScribeJobs returns Medical Scribe jobs with optional status filter and pagination.
func (b *InMemoryBackend) ListMedicalScribeJobs(
	statusFilter, nextToken string,
) ([]MedicalScribeJob, string) {
	b.mu.RLock("ListMedicalScribeJobs")
	defer b.mu.RUnlock()

	all := make([]MedicalScribeJob, 0, b.medicalScribeJobs.Len())
	for _, j := range b.medicalScribeJobs.All() {
		if statusFilter == "" || j.MedicalScribeJobStatus == statusFilter {
			all = append(all, *j)
		}
	}

	sort.Slice(
		all,
		func(i, j int) bool { return all[i].MedicalScribeJobName < all[j].MedicalScribeJobName },
	)

	return paginateList(all, nextToken)
}

// DeleteMedicalScribeJob removes a Medical Scribe job by name.
func (b *InMemoryBackend) DeleteMedicalScribeJob(jobName string) error {
	if jobName == "" {
		return fmt.Errorf("%w: MedicalScribeJobName is required", ErrValidation)
	}

	b.mu.Lock("DeleteMedicalScribeJob")
	defer b.mu.Unlock()

	if !b.medicalScribeJobs.Delete(jobName) {
		return fmt.Errorf("%w: medical scribe job %s not found", ErrNotFound, jobName)
	}

	b.forgetResourceTagsLocked(resourceARN(resourceTypeMedicalScribeJob, jobName))

	return nil
}

// AddMedicalScribeJobInternal seeds a Medical Scribe job directly (test helper).
func (b *InMemoryBackend) AddMedicalScribeJobInternal(job *MedicalScribeJob) {
	b.mu.Lock("AddMedicalScribeJobInternal")
	defer b.mu.Unlock()

	cp := *job
	b.medicalScribeJobs.Put(&cp)
}
