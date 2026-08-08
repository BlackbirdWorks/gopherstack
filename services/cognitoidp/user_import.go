package cognitoidp

import (
	"fmt"
	"sort"
	"time"
)

// CreateUserImportJob creates a new import job for a user pool. cloudWatchLogsRoleArn
// is a required AWS request field (the IAM role Cognito logs import results to);
// passwordHashingAlgorithm is optional.
func (b *InMemoryBackend) CreateUserImportJob(
	userPoolID, jobName, cloudWatchLogsRoleArn, passwordHashingAlgorithm string,
) (*UserImportJob, error) {
	b.mu.Lock("CreateUserImportJob")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	jobID := "import-" + randomAlphanumeric(userImportJobIDLen)
	job := &UserImportJob{
		JobID:                    jobID,
		JobName:                  jobName,
		UserPoolID:               userPoolID,
		Status:                   "Created",
		CreatedAt:                time.Now(),
		CloudWatchLogsRoleArn:    cloudWatchLogsRoleArn,
		PasswordHashingAlgorithm: passwordHashingAlgorithm,
		// AWS's real PreSignedUrl targets an S3 bucket gopherstack has no upload
		// pipeline behind; synthesized the same way domains.go fabricates
		// CloudFrontDistribution/S3Bucket for informational-only response fields.
		PreSignedURL: "https://cognito-idp-import." + b.region + ".amazonaws.com/" + jobID + "?X-Amz-Signature=mock",
	}
	b.userImportJobs.Put(job)

	cp := *job

	return &cp, nil
}

// DescribeUserImportJob returns a user import job by pool and job ID.
func (b *InMemoryBackend) DescribeUserImportJob(userPoolID, jobID string) (*UserImportJob, error) {
	b.mu.RLock("DescribeUserImportJob")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	job, ok := b.userImportJobs.Get(userImportJobKey(userPoolID, jobID))
	if !ok {
		return nil, fmt.Errorf("%w: import job %q not found in pool %q",
			ErrUserPoolNotFound, jobID, userPoolID)
	}

	cp := *job

	return &cp, nil
}

// ListUserImportJobs returns all import jobs for a pool sorted by creation time.
func (b *InMemoryBackend) ListUserImportJobs(userPoolID string) ([]*UserImportJob, error) {
	b.mu.RLock("ListUserImportJobs")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	poolJobs := b.userImportJobsByPool.Get(userPoolID)
	out := make([]*UserImportJob, 0, len(poolJobs))

	for _, job := range poolJobs {
		cp := *job
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })

	return out, nil
}

// StartUserImportJob transitions a Created job to InProgress.
func (b *InMemoryBackend) StartUserImportJob(userPoolID, jobID string) (*UserImportJob, error) {
	b.mu.Lock("StartUserImportJob")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	job, ok := b.userImportJobs.Get(userImportJobKey(userPoolID, jobID))
	if !ok {
		return nil, fmt.Errorf("%w: import job %q not found in pool %q",
			ErrUserPoolNotFound, jobID, userPoolID)
	}

	if job.Status != "Created" && job.Status != "Pending" {
		return nil, fmt.Errorf("%w: import job %q cannot be started from status %q",
			ErrInvalidParameter, jobID, job.Status)
	}

	job.Status = "InProgress"
	job.StartedAt = time.Now()
	cp := *job

	return &cp, nil
}

// StopUserImportJob transitions an InProgress job to Stopped.
func (b *InMemoryBackend) StopUserImportJob(userPoolID, jobID string) (*UserImportJob, error) {
	b.mu.Lock("StopUserImportJob")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	job, ok := b.userImportJobs.Get(userImportJobKey(userPoolID, jobID))
	if !ok {
		return nil, fmt.Errorf("%w: import job %q not found in pool %q",
			ErrUserPoolNotFound, jobID, userPoolID)
	}

	if job.Status != "InProgress" {
		return nil, fmt.Errorf("%w: import job %q cannot be stopped from status %q",
			ErrInvalidParameter, jobID, job.Status)
	}

	job.Status = "Stopped"
	// This backend has no real CSV-processing pipeline, so a stop is the only
	// completion path a job ever reaches; CompletionDate marks that transition.
	job.CompletedAt = time.Now()
	cp := *job

	return &cp, nil
}
