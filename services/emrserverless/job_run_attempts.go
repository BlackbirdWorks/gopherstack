package emrserverless

import "fmt"

// ListJobRunAttempts returns paginated attempt summaries for a job run.
func (b *InMemoryBackend) ListJobRunAttempts(
	applicationID, jobRunID, nextToken string,
	maxResults int,
) ([]*JobRunAttemptSummary, string, error) {
	b.mu.RLock("ListJobRunAttempts")
	defer b.mu.RUnlock()

	if !b.applications.Has(applicationID) {
		return nil, "", fmt.Errorf("%w: application %s not found", ErrNotFound, applicationID)
	}

	jr, ok := b.jobRuns.Get(jobRunID)
	if !ok || jr.ApplicationID != applicationID {
		return nil, "", fmt.Errorf("%w: job run %s not found", ErrNotFound, jobRunID)
	}

	// The in-memory backend synthesises a single attempt (attempt 0) from the
	// job run itself. ReleaseLabel and StateDetails are both required on
	// JobRunAttemptSummary (types.JobRunAttemptSummary) and are already
	// tracked on JobRun -- mirror them rather than reporting empty. Only
	// CreatedBy has no independent source and uses the execution role ARN
	// as a best-effort substitute, matching JobRun's own convention.
	attempt := &JobRunAttemptSummary{
		ApplicationID: jr.ApplicationID,
		Arn:           jr.Arn,
		CreatedAt:     jr.CreatedAt,
		UpdatedAt:     jr.UpdatedAt,
		JobCreatedAt:  jr.CreatedAt,
		CreatedBy:     jr.ExecutionRoleArn,
		ExecutionRole: jr.ExecutionRoleArn,
		ID:            jr.JobRunID,
		ReleaseLabel:  jr.ReleaseLabel,
		State:         jr.State,
		StateDetails:  jr.StateDetails,
		Name:          jr.Name,
		Mode:          jr.Mode,
		// Attempt index starts at 0; the backend does not track retries.
		Attempt: 0,
	}

	all := []*JobRunAttemptSummary{attempt}
	page, token := emrPaginate(all, nextToken, maxResults)

	return page, token, nil
}
