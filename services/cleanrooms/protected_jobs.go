package cleanrooms

import (
	"sort"

	"github.com/google/uuid"
)

func (b *InMemoryBackend) StartProtectedJob(
	membershipID, jobType string,
	jobParameters map[string]any,
	resultConfig map[string]any,
) (*ProtectedJob, error) {
	b.mu.Lock("StartProtectedJob")
	defer b.mu.Unlock()
	mem, ok := b.memberships.Get(membershipID)
	if !ok {
		return nil, ErrNotFound
	}
	id := uuid.NewString()
	j := &ProtectedJob{
		ID:                   id,
		MembershipIdentifier: membershipID,
		MembershipArn:        mem.Arn,
		// See StartProtectedQuery: the Start response reports the AWS-accurate
		// SUBMITTED status; advanceProtectedJobsLocked (called from every
		// subsequent read) resolves it to a terminal status instead of leaving
		// it stuck at SUBMITTED forever.
		Status:              "SUBMITTED",
		Type:                jobType,
		JobParameters:       jobParameters,
		ResultConfiguration: resultConfig,
		CreateTime:          b.now(),
		MembershipID:        membershipID,
	}
	b.protectedJobs.Put(j)

	return j, nil
}

// advanceProtectedJobsLocked resolves every non-terminal protected job to
// SUCCESS. Called from read paths so GetProtectedJob/ListProtectedJobs always
// observe forward progress. Callers must hold b.mu (write lock).
func (b *InMemoryBackend) advanceProtectedJobsLocked() {
	b.protectedJobs.Range(func(j *ProtectedJob) bool {
		if !isTerminalProtectedJobStatus(j.Status) {
			j.Status = protectedJobStatusSuccess
			j.Statistics = map[string]any{"totalDurationInMillis": mockDurationMillis}
		}

		return true
	})
}

func (b *InMemoryBackend) GetProtectedJob(membershipID, jobID string) (*ProtectedJob, error) {
	b.mu.Lock("GetProtectedJob")
	defer b.mu.Unlock()
	b.advanceProtectedJobsLocked()
	j, ok := b.protectedJobs.Get(membershipKey(membershipID, jobID))
	if !ok {
		return nil, ErrNotFound
	}

	return j, nil
}

func (b *InMemoryBackend) ListProtectedJobs(
	membershipID, status, maxResults, nextToken string,
) ([]*ProtectedJobSummary, string, error) {
	b.mu.Lock("ListProtectedJobs")
	defer b.mu.Unlock()
	b.advanceProtectedJobsLocked()
	if _, ok := b.memberships.Get(membershipID); !ok {
		return nil, "", ErrNotFound
	}
	var items []*ProtectedJobSummary
	for _, j := range b.protectedJobsByMembership.Get(membershipID) {
		if status != "" && j.Status != status {
			continue
		}
		items = append(items, &ProtectedJobSummary{
			ID:                   j.ID,
			MembershipIdentifier: j.MembershipIdentifier,
			MembershipArn:        j.MembershipArn,
			Status:               j.Status,
			Type:                 j.Type,
			CreateTime:           j.CreateTime,
			MembershipID:         j.MembershipID,
		})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	page, next := paginate(items, maxResults, nextToken)

	return page, next, nil
}

func (b *InMemoryBackend) UpdateProtectedJob(
	membershipID, jobID, targetStatus string,
) (*ProtectedJob, error) {
	b.mu.Lock("UpdateProtectedJob")
	defer b.mu.Unlock()
	j, ok := b.protectedJobs.Get(membershipKey(membershipID, jobID))
	if !ok {
		return nil, ErrNotFound
	}
	// UpdateProtectedJob's only valid TargetStatus is CANCELLED, and AWS
	// rejects cancelling a job that has already reached a terminal status
	// with ConflictException.
	if isTerminalProtectedJobStatus(j.Status) {
		return nil, ErrConflict
	}
	j.Status = targetStatus

	return j, nil
}

// isTerminalProtectedJobStatus reports whether a ProtectedJobStatus value is
// terminal (no further transitions permitted).
func isTerminalProtectedJobStatus(status string) bool {
	switch status {
	case "SUCCESS", "FAILED", statusCancelled:
		return true
	default:
		return false
	}
}
