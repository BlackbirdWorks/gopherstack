package codepipeline

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// validateThirdPartyClientToken checks clientToken against the ClientId
// issued for job by PollForThirdPartyJobs. A job that was never polled has
// no ClientID, so any token is rejected -- there is nothing it could
// legitimately match.
func validateThirdPartyClientToken(job *Job, clientToken string) error {
	if job.ClientID == "" || job.ClientID != clientToken {
		return fmt.Errorf("%w: third-party job %q", ErrInvalidClientToken, job.ID)
	}

	return nil
}

// AcknowledgeThirdPartyJob acknowledges that a third-party job worker has received a job.
func (b *InMemoryBackend) AcknowledgeThirdPartyJob(
	ctx context.Context,
	jobID, nonce, clientToken string,
) (string, error) {
	b.mu.Lock("AcknowledgeThirdPartyJob")
	defer b.mu.Unlock()

	job, err := b.getJobLocked(ctx, jobID)
	if err != nil {
		return "", err
	}

	if err = validateThirdPartyClientToken(job, clientToken); err != nil {
		return "", err
	}

	if job.Nonce == nonce {
		job.Status = statusInProgress
	}

	return job.Status, nil
}

// PollForThirdPartyJobs returns available third-party jobs, lazily issuing
// each returned job's ClientId the first time it is handed out.
func (b *InMemoryBackend) PollForThirdPartyJobs(
	ctx context.Context,
	category, provider, version string,
) ([]*Job, error) {
	b.mu.Lock("PollForThirdPartyJobs")
	defer b.mu.Unlock()

	jobs := b.pollForJobsLocked(ctx, category, ownerThirdParty, provider, version, nil)

	result := make([]*Job, len(jobs))
	for i, j := range jobs {
		if j.ClientID == "" {
			j.ClientID = uuid.NewString()
		}

		cp := *j
		result[i] = &cp
	}

	return result, nil
}

// GetThirdPartyJobDetails returns details for a third-party job.
func (b *InMemoryBackend) GetThirdPartyJobDetails(ctx context.Context, jobID, clientToken string) (*Job, error) {
	b.mu.RLock("GetThirdPartyJobDetails")
	defer b.mu.RUnlock()

	job, err := b.getJobLocked(ctx, jobID)
	if err != nil {
		return nil, err
	}

	if err = validateThirdPartyClientToken(job, clientToken); err != nil {
		return nil, err
	}

	cp := *job

	return &cp, nil
}

// PutThirdPartyJobSuccessResult acknowledges third-party job success.
func (b *InMemoryBackend) PutThirdPartyJobSuccessResult(ctx context.Context, jobID, clientToken string) error {
	return b.PutThirdPartyJobSuccessResultWith(ctx, jobID, clientToken, JobSuccess{})
}

// PutThirdPartyJobSuccessResultWith is PutThirdPartyJobSuccessResult carrying the optional result members.
func (b *InMemoryBackend) PutThirdPartyJobSuccessResultWith(
	ctx context.Context, jobID, clientToken string, res JobSuccess,
) error {
	b.mu.Lock("PutThirdPartyJobSuccessResult")
	defer b.mu.Unlock()

	job, err := b.getJobLocked(ctx, jobID)
	if err != nil {
		return err
	}

	if err = validateThirdPartyClientToken(job, clientToken); err != nil {
		return err
	}

	return b.completeJobSuccessLocked(job, res)
}

// PutThirdPartyJobFailureResult acknowledges third-party job failure.
func (b *InMemoryBackend) PutThirdPartyJobFailureResult(
	ctx context.Context,
	jobID, clientToken, message, failureType string,
) error {
	return b.PutThirdPartyJobFailureResultWith(ctx, jobID, clientToken, JobFailure{Message: message, Type: failureType})
}

// PutThirdPartyJobFailureResultWith is PutThirdPartyJobFailureResult carrying the full FailureDetails.
func (b *InMemoryBackend) PutThirdPartyJobFailureResultWith(
	ctx context.Context, jobID, clientToken string, fail JobFailure,
) error {
	b.mu.Lock("PutThirdPartyJobFailureResult")
	defer b.mu.Unlock()

	job, err := b.getJobLocked(ctx, jobID)
	if err != nil {
		return err
	}

	if err = validateThirdPartyClientToken(job, clientToken); err != nil {
		return err
	}

	return b.completeJobFailureLocked(job, fail)
}
