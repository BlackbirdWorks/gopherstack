package amplify

import (
	"fmt"
	"net/url"
	"time"
)

const (
	sourceURLTypeZip          = "ZIP"
	sourceURLTypeBucketPrefix = "BUCKET_PREFIX"
)

// CreateDeployment creates a pre-signed upload URL for a manual deployment.
func (b *InMemoryBackend) CreateDeployment(appID, branchName string) (string, string, error) {
	urls, err := b.CreateDeploymentWithFiles(appID, branchName, nil)
	if err != nil {
		return "", "", err
	}

	return urls.JobID, urls.ZipUploadURL, nil
}

// DeploymentURLs is CreateDeployment's result: the job and its upload targets.
type DeploymentURLs struct {
	FileUploadURLs map[string]string
	JobID          string
	ZipUploadURL   string
}

// CreateDeploymentWithFiles also issues one upload URL per fileMap key.
func (b *InMemoryBackend) CreateDeploymentWithFiles(
	appID, branchName string, fileMap map[string]string,
) (DeploymentURLs, error) {
	b.mu.Lock("CreateDeployment")
	defer b.mu.Unlock()

	if !b.apps.Has(appID) {
		return DeploymentURLs{}, fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	if !b.branches.Has(branchKey(appID, branchName)) {
		return DeploymentURLs{}, fmt.Errorf(
			"%w: branch %s not found for app %s",
			ErrNotFound,
			branchName,
			appID,
		)
	}

	jobID := b.nextJobIDLocked(appID, branchName)
	base := "https://s3.amazonaws.com/amplify-upload-" + appID + "/" + branchName + "/" + jobID

	fileURLs := make(map[string]string, len(fileMap))
	for name := range fileMap {
		fileURLs[name] = base + "/" + url.PathEscape(name)
	}

	return DeploymentURLs{JobID: jobID, ZipUploadURL: base + ".zip", FileUploadURLs: fileURLs}, nil
}

// StartDeployment starts a deployment from a pre-uploaded artifact.
func (b *InMemoryBackend) StartDeployment(
	appID, branchName, jobID, sourceURL, sourceURLType string,
) (*Job, error) {
	if sourceURLType != "" && sourceURLType != sourceURLTypeZip && sourceURLType != sourceURLTypeBucketPrefix {
		return nil, fmt.Errorf("%w: invalid sourceUrlType %q", ErrValidation, sourceURLType)
	}

	b.mu.Lock("StartDeployment")
	defer b.mu.Unlock()

	if !b.apps.Has(appID) {
		return nil, fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	if !b.branches.Has(branchKey(appID, branchName)) {
		return nil, fmt.Errorf("%w: branch %s not found for app %s", ErrNotFound, branchName, appID)
	}

	if jobID == "" {
		jobID = b.nextJobIDLocked(appID, branchName)
	}

	if sourceURL != "" && sourceURLType == "" {
		sourceURLType = sourceURLTypeZip
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
		SourceURL:  sourceURL,
	}

	if sourceURL != "" {
		job.SourceURLType = sourceURLType
	}

	b.jobs.Put(job)

	cp := *job

	return &cp, nil
}
