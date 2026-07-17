package backup

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ListCopyJobs returns all copy jobs.
func (b *InMemoryBackend) ListCopyJobs() []*CopyJob {
	b.mu.RLock("ListCopyJobs")
	defer b.mu.RUnlock()

	all := b.copyJobs.All()
	list := make([]*CopyJob, 0, len(all))
	for _, j := range all {
		cp := *j
		list = append(list, &cp)
	}

	slices.SortFunc(list, func(a, b *CopyJob) int {
		if a.CreationDate.After(b.CreationDate) {
			return -1
		}
		if a.CreationDate.Before(b.CreationDate) {
			return 1
		}

		return 0
	})

	return list
}

// DescribeCopyJob returns a copy job by ID.
func (b *InMemoryBackend) DescribeCopyJob(copyJobID string) (*CopyJob, error) {
	b.mu.RLock("DescribeCopyJob")
	defer b.mu.RUnlock()

	j, ok := b.copyJobs.Get(copyJobID)
	if !ok {
		return nil, fmt.Errorf("%w: copy job %s not found", ErrNotFound, copyJobID)
	}

	cp := *j

	return &cp, nil
}

// --- Restore Testing read/update/delete methods ---

// ListCopyJobSummaries returns a summary of copy jobs.
func (b *InMemoryBackend) ListCopyJobSummaries() []map[string]any {
	b.mu.RLock("ListCopyJobSummaries")
	defer b.mu.RUnlock()

	counts := make(map[string]int)
	for _, j := range b.copyJobs.All() {
		counts[j.State]++
	}

	summaries := make([]map[string]any, 0, len(counts))
	for state, count := range counts {
		summaries = append(summaries, map[string]any{
			"State":          state,
			keySummaryCount:  count,
			keySummaryRegion: b.region,
		})
	}

	return summaries
}

// StartCopyJob creates a new copy job.
func (b *InMemoryBackend) StartCopyJob(
	recoveryPointArn, sourceVaultArn, destVaultArn, iamRoleArn string,
) *CopyJob {
	b.mu.Lock("StartCopyJob")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	done := now
	job := &CopyJob{
		CopyJobID:                 "copy-job-" + uuid.New().String()[:8],
		SourceBackupVaultArn:      sourceVaultArn,
		DestinationBackupVaultArn: destVaultArn,
		ResourceArn:               recoveryPointArn,
		IAMRoleArn:                iamRoleArn,
		State:                     statusCompleted,
		AccountID:                 b.accountID,
		Region:                    b.region,
		CreationDate:              now,
		CompletionDate:            &done,
	}
	b.copyJobs.Put(job)

	return job
}

// ---- Backup plan operations ----

// ListCopyJobsFilter contains optional filter parameters for listing copy jobs.
type ListCopyJobsFilter struct {
	CreatedAfter              *time.Time
	CreatedBefore             *time.Time
	State                     string
	ResourceArn               string
	ResourceType              string
	SourceBackupVaultArn      string
	DestinationBackupVaultArn string
	AccountID                 string
	NextToken                 string
	MaxResults                int
}

// copyJobMatchesFilter reports whether j satisfies all active fields in f.
func copyJobMatchesFilter(j *CopyJob, f ListCopyJobsFilter) bool {
	// Vault-specific filters checked before the common time-range check.
	if f.SourceBackupVaultArn != "" && j.SourceBackupVaultArn != f.SourceBackupVaultArn {
		return false
	}
	if f.DestinationBackupVaultArn != "" && j.DestinationBackupVaultArn != f.DestinationBackupVaultArn {
		return false
	}
	if f.State != "" && j.State != f.State {
		return false
	}
	if f.ResourceArn != "" && j.ResourceArn != f.ResourceArn {
		return false
	}
	if f.ResourceType != "" && j.ResourceType != f.ResourceType {
		return false
	}
	if f.AccountID != "" && j.AccountID != f.AccountID {
		return false
	}

	return inTimeRange(j.CreationDate, f.CreatedAfter, f.CreatedBefore)
}

// ListCopyJobsFiltered returns copy jobs matching the filter, with pagination.
//
//nolint:dupl // structurally identical to ListBackupJobsFiltered but operates on a different type
func (b *InMemoryBackend) ListCopyJobsFiltered(f ListCopyJobsFilter) ([]*CopyJob, string) {
	b.mu.RLock("ListCopyJobsFiltered")
	defer b.mu.RUnlock()

	all := b.copyJobs.All()
	list := make([]*CopyJob, 0, len(all))
	for _, j := range all {
		if !copyJobMatchesFilter(j, f) {
			continue
		}
		cp := *j
		list = append(list, &cp)
	}

	slices.SortFunc(list, func(a, b *CopyJob) int {
		if d := b.CreationDate.Compare(a.CreationDate); d != 0 {
			return d
		}

		return strings.Compare(a.CopyJobID, b.CopyJobID)
	})

	return paginateByID(
		list,
		func(j *CopyJob) string { return j.CopyJobID },
		f.MaxResults,
		f.NextToken,
	)
}

// ---- ListBackupVaults filtering + pagination ----
