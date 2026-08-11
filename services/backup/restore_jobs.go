package backup

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// findRecoveryPointByArn scans every tracked recovery point for a matching
// ARN, regardless of vault. recoveryPoints is keyed by vaultName#arn (see
// recoveryPointKey), so a bare-ARN lookup needs a scan; call sites (restore
// and copy jobs) only run this once per Start* call, not per request, so an
// O(n) scan over this emulator's typically-small recovery point set is fine.
func (b *InMemoryBackend) findRecoveryPointByArn(recoveryPointArn string) (*RecoveryPoint, bool) {
	for _, rp := range b.recoveryPoints.All() {
		if rp.RecoveryPointArn == recoveryPointArn {
			return rp, true
		}
	}

	return nil, false
}

// StartRestoreJob creates a new restore job. Real AWS provisions an actual
// new resource of ResourceType once the restore completes; this emulator
// synthesizes a plausible CreatedResourceArn instead so callers exercising
// "restore -> use the restored resource" flows have something ARN-shaped to
// chain off of.
func (b *InMemoryBackend) StartRestoreJob(
	recoveryPointArn, iamRoleArn, resourceType string,
	metadata map[string]string,
) (*RestoreJob, error) {
	b.mu.Lock("StartRestoreJob")
	defer b.mu.Unlock()

	if recoveryPointArn == "" {
		return nil, fmt.Errorf("%w: RecoveryPointArn is required", ErrValidation)
	}
	if iamRoleArn == "" {
		return nil, fmt.Errorf("%w: IamRoleArn is required", ErrValidation)
	}
	if len(metadata) == 0 {
		return nil, fmt.Errorf("%w: Metadata is required", ErrValidation)
	}

	now := time.Now().UTC()
	jobID := "restore-job-" + uuid.New().String()[:8]

	job := &RestoreJob{
		RestoreJobID:     jobID,
		RecoveryPointArn: recoveryPointArn,
		IAMRoleArn:       iamRoleArn,
		ResourceType:     resourceType,
		Metadata:         metadata,
		Status:           statusCompleted,
		PercentDone:      "100.0",
		StartTime:        now,
		CompletionDate:   &now,
		AccountID:        b.accountID,
	}

	// Enrich from the source recovery point when this backend tracks it --
	// a caller may legitimately restore from an ARN it never registered
	// through this emulator's own Backup APIs.
	if srcRP, found := b.findRecoveryPointByArn(recoveryPointArn); found {
		job.ResourceArn = srcRP.ResourceArn
		job.BackupVaultName = srcRP.BackupVaultName
		job.BackupVaultArn = srcRP.BackupVaultArn
		job.BackupSizeInBytes = srcRP.BackupSizeInBytes
		if job.ResourceType == "" {
			job.ResourceType = srcRP.ResourceType
		}
	}

	if job.ResourceType != "" {
		job.CreatedResourceArn = "arn:aws:" + strings.ToLower(job.ResourceType) +
			":" + b.region + ":" + b.accountID + ":restored/" + jobID
	}

	b.restoreJobs.Put(job)
	cp := *job

	return &cp, nil
}

// DescribeRestoreJob returns a restore job by ID.
func (b *InMemoryBackend) DescribeRestoreJob(restoreJobID string) (*RestoreJob, error) {
	b.mu.RLock("DescribeRestoreJob")
	defer b.mu.RUnlock()

	job, ok := b.restoreJobs.Get(restoreJobID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, restoreJobID)
	}
	cp := *job

	return &cp, nil
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

// PutRestoreValidationResult records the validation outcome of a restore
// test on the restore job itself, so DescribeRestoreJob reflects it -- this
// was previously a disguised no-op: the result went into a side map
// (restoreValidations) that DescribeRestoreJob never read.
func (b *InMemoryBackend) PutRestoreValidationResult(
	restoreJobID, validationStatus, validationStatusMessage string,
) error {
	b.mu.Lock("PutRestoreValidationResult")
	defer b.mu.Unlock()

	if restoreJobID == "" {
		return fmt.Errorf("%w: RestoreJobId is required", ErrValidation)
	}
	if validationStatus == "" {
		return fmt.Errorf("%w: ValidationStatus is required", ErrValidation)
	}

	job, ok := b.restoreJobs.Get(restoreJobID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, restoreJobID)
	}

	job.ValidationStatus = validationStatus
	job.ValidationStatusMessage = validationStatusMessage

	return nil
}

// ---- Report Jobs ----
