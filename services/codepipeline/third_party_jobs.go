package codepipeline

import (
	"context"
	"fmt"
)

// AcknowledgeThirdPartyJob acknowledges that a third-party job worker has received a job.
func (b *InMemoryBackend) AcknowledgeThirdPartyJob(
	ctx context.Context,
	jobID, nonce, clientToken string,
) (string, error) {
	b.mu.Lock("AcknowledgeThirdPartyJob")
	defer b.mu.Unlock()

	job, ok := b.jobs.Get(regionKey(getRegion(ctx, b.region), jobID))
	if !ok {
		return "", fmt.Errorf("%w: third-party job %q with client token %q", ErrJobNotFound, jobID, clientToken)
	}

	if job.Nonce == nonce {
		job.Status = statusInProgress
	}

	return job.Status, nil
}

// PollForThirdPartyJobs returns available third-party jobs.
func (b *InMemoryBackend) PollForThirdPartyJobs(
	ctx context.Context,
	category, provider, version string,
) ([]*Job, error) {
	return b.PollForJobs(ctx, category, "ThirdParty", provider, version)
}

// GetThirdPartyJobDetails returns details for a third-party job.
func (b *InMemoryBackend) GetThirdPartyJobDetails(ctx context.Context, jobID, clientToken string) (*Job, error) {
	b.mu.RLock("GetThirdPartyJobDetails")
	defer b.mu.RUnlock()

	job, ok := b.jobs.Get(regionKey(getRegion(ctx, b.region), jobID))
	if !ok {
		return nil, ErrJobNotFound
	}

	_ = clientToken

	cp := *job

	return &cp, nil
}

// PutThirdPartyJobSuccessResult acknowledges third-party job success.
func (b *InMemoryBackend) PutThirdPartyJobSuccessResult(ctx context.Context, jobID, _ string) error {
	return b.PutJobSuccessResult(ctx, jobID)
}

// PutThirdPartyJobFailureResult acknowledges third-party job failure.
func (b *InMemoryBackend) PutThirdPartyJobFailureResult(ctx context.Context, jobID, _, message string) error {
	return b.PutJobFailureResult(ctx, jobID, message)
}
