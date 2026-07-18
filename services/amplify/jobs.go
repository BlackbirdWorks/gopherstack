package amplify

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// StartJob creates and starts a new deployment job for a branch.
func (b *InMemoryBackend) StartJob(
	appID, branchName, jobType, commitID, commitMsg string,
) (*Job, error) {
	b.mu.Lock("StartJob")
	defer b.mu.Unlock()

	if !b.apps.Has(appID) {
		return nil, fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	if !b.branches.Has(branchKey(appID, branchName)) {
		return nil, fmt.Errorf("%w: branch %s not found for app %s", ErrNotFound, branchName, appID)
	}

	jobID := randomID()
	now := time.Now().UTC()

	jt := JobType(jobType)
	if jt == "" {
		jt = JobTypeRelease
	}

	job := &Job{
		JobID:      jobID,
		JobARN:     b.jobARN(appID, branchName, jobID),
		CommitID:   commitID,
		CommitMsg:  commitMsg,
		Status:     JobStatusRunning,
		Type:       jt,
		StartTime:  now,
		AppID:      appID,
		BranchName: branchName,
	}

	b.jobs.Put(job)

	cp := *job

	return &cp, nil
}

// jobARN builds the ARN for a deployment job. Real Amplify's JobSummary.JobArn
// is a required response field; every job created by this backend must carry
// one so the aws-sdk-go-v2 deserializer doesn't leave it nil.
func (b *InMemoryBackend) jobARN(appID, branchName, jobID string) string {
	return arn.Build(
		"amplify",
		b.region,
		b.accountID,
		fmt.Sprintf("apps/%s/branches/%s/jobs/%s", appID, branchName, jobID),
	)
}

// StopJob cancels a running job.
func (b *InMemoryBackend) StopJob(appID, branchName, jobID string) (*Job, error) {
	b.mu.Lock("StopJob")
	defer b.mu.Unlock()

	job, err := b.findJob(appID, branchName, jobID)
	if err != nil {
		return nil, err
	}

	job.Status = JobStatusCancelled
	job.EndTime = time.Now().UTC()

	cp := *job

	return &cp, nil
}

// GetJob returns a job by ID.
func (b *InMemoryBackend) GetJob(appID, branchName, jobID string) (*Job, error) {
	b.mu.RLock("GetJob")
	defer b.mu.RUnlock()

	job, err := b.findJob(appID, branchName, jobID)
	if err != nil {
		return nil, err
	}

	cp := *job

	return &cp, nil
}

// ListJobs lists all jobs for a branch.
func (b *InMemoryBackend) ListJobs(
	appID, branchName, nextToken string,
	maxResults int,
) ([]*Job, string, error) {
	b.mu.RLock("ListJobs")
	defer b.mu.RUnlock()

	if !b.apps.Has(appID) {
		return nil, "", fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	var all []*Job

	for _, job := range b.jobsByBranch.Get(branchKey(appID, branchName)) {
		cp := *job
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].JobID < all[j].JobID })

	page, token := amplifyPaginate(all, nextToken, maxResults)

	return page, token, nil
}

// DeleteJob deletes a job record.
func (b *InMemoryBackend) DeleteJob(appID, branchName, jobID string) (*Job, error) {
	b.mu.Lock("DeleteJob")
	defer b.mu.Unlock()

	job, err := b.findJob(appID, branchName, jobID)
	if err != nil {
		return nil, err
	}

	cp := *job
	b.jobs.Delete(jobKey(appID, branchName, jobID))

	return &cp, nil
}

// findJob locates a job in the jobs table. Must be called while holding a lock.
func (b *InMemoryBackend) findJob(appID, branchName, jobID string) (*Job, error) {
	job, ok := b.jobs.Get(jobKey(appID, branchName, jobID))
	if !ok {
		return nil, fmt.Errorf(
			"%w: job %s not found for branch %s app %s",
			ErrNotFound,
			jobID,
			branchName,
			appID,
		)
	}

	return job, nil
}
