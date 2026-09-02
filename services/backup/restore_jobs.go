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
	jobID := "restore-job-" + uuid.NewString()[:8]

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

// ListRestoreJobSummaries returns restore job counts grouped by State, real
// RestoreJobSummary's own required grouping key (backup@v1.59.4
// api_op_ListRestoreJobSummaries.go, RestoreJobSummary: AccountId, Count,
// Region, ResourceType, State, StartTime, EndTime). AggregationPeriod
// (per-day/per-week time-bucketed counts) and ResourceType-level grouping
// are not modeled: this backend produces one point-in-time snapshot per
// call, not a historical time series, and every other summary op in this
// package (ListBackupJobSummaries/ListCopyJobSummaries) groups by State
// only, not by the full (Region,AccountId,State,ResourceType) key real AWS
// documents -- kept consistent with that existing precedent rather than
// introducing a different fidelity level for this one sibling op.
func (b *InMemoryBackend) ListRestoreJobSummaries() []map[string]any {
	b.mu.RLock("ListRestoreJobSummaries")
	defer b.mu.RUnlock()

	counts := make(map[string]int)
	for _, j := range b.restoreJobs.All() {
		counts[j.Status]++
	}

	summaries := make([]map[string]any, 0, len(counts))
	for state, count := range counts {
		summaries = append(summaries, map[string]any{
			keyState:         state,
			keySummaryCount:  count,
			keySummaryRegion: b.region,
			keyAccountID:     b.accountID,
		})
	}

	return summaries
}

// ListRestoreJobsFilter contains optional filter parameters for listing
// restore jobs, mirroring ListRestoreJobsInput (api_op_ListRestoreJobs.go,
// backup@v1.59.4). ByParentJobId and ByRestoreTestingPlanArn are not
// included: this backend's RestoreJob has no field to hold either value
// (StartRestoreJob never receives or fabricates one).
type ListRestoreJobsFilter struct {
	CreatedAfter   *time.Time
	CreatedBefore  *time.Time
	CompleteAfter  *time.Time
	CompleteBefore *time.Time
	AccountID      string
	ResourceType   string
	Status         string
	NextToken      string
	MaxResults     int
}

func restoreJobMatchesFilter(j *RestoreJob, f ListRestoreJobsFilter) bool {
	if f.AccountID != "" && j.AccountID != f.AccountID {
		return false
	}
	if f.ResourceType != "" && j.ResourceType != f.ResourceType {
		return false
	}
	if f.Status != "" && j.Status != f.Status {
		return false
	}
	if !inTimeRange(j.StartTime, f.CreatedAfter, f.CreatedBefore) {
		return false
	}
	if j.CompletionDate == nil {
		return f.CompleteAfter == nil && f.CompleteBefore == nil
	}

	return inTimeRange(*j.CompletionDate, f.CompleteAfter, f.CompleteBefore)
}

// ListRestoreJobsFiltered returns restore jobs matching the filter, paginated
// per f.MaxResults/f.NextToken. Returns (jobs, nextToken).
func (b *InMemoryBackend) ListRestoreJobsFiltered(f ListRestoreJobsFilter) ([]*RestoreJob, string) {
	b.mu.RLock("ListRestoreJobsFiltered")
	defer b.mu.RUnlock()

	all := b.restoreJobs.All()
	out := make([]*RestoreJob, 0, len(all))
	for _, j := range all {
		if !restoreJobMatchesFilter(j, f) {
			continue
		}
		cp := *j
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RestoreJobID < out[j].RestoreJobID })

	return paginateByID(out, func(j *RestoreJob) string { return j.RestoreJobID }, f.MaxResults, f.NextToken)
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
