package amplify

import (
	"fmt"
	"time"
)

// CreateDeployment creates a pre-signed upload URL for a manual deployment.
func (b *InMemoryBackend) CreateDeployment(appID, branchName string) (string, string, error) {
	b.mu.RLock("CreateDeployment")
	defer b.mu.RUnlock()

	if !b.apps.Has(appID) {
		return "", "", fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	if !b.branches.Has(branchKey(appID, branchName)) {
		return "", "", fmt.Errorf(
			"%w: branch %s not found for app %s",
			ErrNotFound,
			branchName,
			appID,
		)
	}

	jobID := randomID()
	uploadURL := "https://s3.amazonaws.com/amplify-upload-" + appID + "/" + branchName + "/" + jobID + ".zip"

	return jobID, uploadURL, nil
}

// StartDeployment starts a deployment from a pre-uploaded artifact.
func (b *InMemoryBackend) StartDeployment(
	appID, branchName, jobID, sourceURL string,
) (*Job, error) {
	b.mu.Lock("StartDeployment")
	defer b.mu.Unlock()

	if !b.apps.Has(appID) {
		return nil, fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	if !b.branches.Has(branchKey(appID, branchName)) {
		return nil, fmt.Errorf("%w: branch %s not found for app %s", ErrNotFound, branchName, appID)
	}

	if jobID == "" {
		jobID = randomID()
	}

	now := time.Now().UTC()

	job := &Job{
		JobID:      jobID,
		JobARN:     b.jobARN(appID, branchName, jobID),
		CommitID:   sourceURL,
		Status:     JobStatusRunning,
		Type:       JobTypeManual,
		StartTime:  now,
		AppID:      appID,
		BranchName: branchName,
	}

	b.jobs.Put(job)

	cp := *job

	return &cp, nil
}
