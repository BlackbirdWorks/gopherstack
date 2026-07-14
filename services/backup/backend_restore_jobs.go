package backup

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// StartRestoreJob creates a new restore job.
func (b *InMemoryBackend) StartRestoreJob(
	recoveryPointArn, iamRoleArn, resourceType string,
	metadata map[string]string,
) *RestoreJob {
	b.mu.Lock("StartRestoreJob")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	done := now
	job := &RestoreJob{
		RestoreJobID:     "restore-job-" + uuid.New().String()[:8],
		RecoveryPointArn: recoveryPointArn,
		IAMRoleArn:       iamRoleArn,
		ResourceType:     resourceType,
		Metadata:         metadata,
		Status:           statusCompleted,
		PercentDone:      "100.0",
		StartTime:        now,
		CompletionDate:   &done,
	}
	b.restoreJobs.Put(job)

	return job
}

// DescribeRestoreJob returns a restore job by ID.
func (b *InMemoryBackend) DescribeRestoreJob(restoreJobID string) (*RestoreJob, error) {
	b.mu.RLock("DescribeRestoreJob")
	defer b.mu.RUnlock()

	job, ok := b.restoreJobs.Get(restoreJobID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", errRestoreJobNotFound, restoreJobID)
	}

	return job, nil
}

// ListRestoreJobs returns all restore jobs.
func (b *InMemoryBackend) ListRestoreJobs() []*RestoreJob {
	b.mu.RLock("ListRestoreJobs")
	defer b.mu.RUnlock()

	all := b.restoreJobs.All()
	out := make([]*RestoreJob, 0, len(all))
	for _, j := range all {
		cp := *j
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RestoreJobID < out[j].RestoreJobID })

	return out
}

// ListRestoreJobsByProtectedResource returns restore jobs for a given resource ARN.
func (b *InMemoryBackend) ListRestoreJobsByProtectedResource(resourceArn string) []*RestoreJob {
	b.mu.RLock("ListRestoreJobsByProtectedResource")
	defer b.mu.RUnlock()

	var out []*RestoreJob
	for _, j := range b.restoreJobs.All() {
		if j.ResourceArn == resourceArn || j.RecoveryPointArn == resourceArn {
			cp := *j
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RestoreJobID < out[j].RestoreJobID })

	return out
}

// PutRestoreValidationResult records validation for a restore job.
func (b *InMemoryBackend) PutRestoreValidationResult(restoreJobID, validationStatus string) {
	b.mu.Lock("PutRestoreValidationResult")
	defer b.mu.Unlock()

	b.restoreValidations[restoreJobID] = validationStatus
}

// ---- Report Jobs ----
