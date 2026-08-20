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

	// The in-memory backend synthesises a single attempt (attempt 0) from the job
	// run itself.  Fields that are not tracked by the backend (ReleaseLabel,
	// StateDetails, CreatedBy) use sensible placeholders or empty values.
	attempt := &JobRunAttemptSummary{
		ApplicationID: jr.ApplicationID,
		Arn:           jr.Arn,
		CreatedAt:     jr.CreatedAt,
		UpdatedAt:     jr.UpdatedAt,
		JobCreatedAt:  jr.CreatedAt,
		// CreatedBy is set to the execution role ARN as a best-effort substitute;
		// the in-memory backend does not record the IAM principal that submitted the run.
		CreatedBy:     jr.ExecutionRoleArn,
		ExecutionRole: jr.ExecutionRoleArn,
		ID:            jr.JobRunID,
		// ReleaseLabel is not stored on JobRun in this backend.
		ReleaseLabel: "",
		State:        jr.State,
		// StateDetails are not tracked in the in-memory backend.
		StateDetails: "",
		Name:         jr.Name,
		Mode:         jr.Mode,
		// Attempt index starts at 0; the backend does not track retries.
		Attempt: 0,
	}

	all := []*JobRunAttemptSummary{attempt}
	page, token := emrPaginate(all, nextToken, maxResults)

	return page, token, nil
}
