package amplify

import (
	"fmt"
	"sort"
)

// GenerateAccessLogs generates a presigned URL for access logs.
func (b *InMemoryBackend) GenerateAccessLogs(
	appID, domainName, startTime, endTime string,
) (string, error) {
	b.mu.RLock("GenerateAccessLogs")
	defer b.mu.RUnlock()

	if !b.apps.Has(appID) {
		return "", fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	logURL := "https://s3.amazonaws.com/amplify-logs-" + appID + "/" + domainName +
		"/access-" + startTime + "-" + endTime + ".log"

	return logURL, nil
}

// GetArtifactURL returns the download URL for an artifact.
func (b *InMemoryBackend) GetArtifactURL(artifactID string) (string, string, error) {
	b.mu.RLock("GetArtifactURL")
	defer b.mu.RUnlock()

	artifact, ok := b.artifacts.Get(artifactID)
	if !ok {
		return "", "", fmt.Errorf("%w: artifact %s not found", ErrNotFound, artifactID)
	}

	url := "https://s3.amazonaws.com/amplify-artifacts/" + artifactID + "/" + artifact.ArtifactFileName

	return artifact.ArtifactType, url, nil
}

// ListArtifacts lists the build artifacts produced by a job. Artifacts are
// created by the janitor (see janitor.go's advanceJobs) when a job reaches
// SUCCEED -- a job that hasn't completed yet, or that failed/was cancelled,
// legitimately has none.
func (b *InMemoryBackend) ListArtifacts(
	appID, branchName, jobID, nextToken string,
	maxResults int,
) ([]*Artifact, string, error) {
	b.mu.RLock("ListArtifacts")
	defer b.mu.RUnlock()

	if !b.apps.Has(appID) {
		return nil, "", fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	if !b.branches.Has(branchKey(appID, branchName)) {
		return nil, "", fmt.Errorf("%w: branch %s not found for app %s", ErrNotFound, branchName, appID)
	}

	if !b.jobs.Has(jobKey(appID, branchName, jobID)) {
		return nil, "", fmt.Errorf(
			"%w: job %s not found for branch %s app %s", ErrNotFound, jobID, branchName, appID,
		)
	}

	src := b.artifactsByJob.Get(jobKey(appID, branchName, jobID))
	all := make([]*Artifact, 0, len(src))

	for _, art := range src {
		cp := *art
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ArtifactID < all[j].ArtifactID })

	page, token := amplifyPaginate(all, nextToken, maxResults)

	return page, token, nil
}

// newBuildArtifact synthesizes the single build-output artifact real
// Amplify produces when a job completes successfully. gopherstack has no
// real build fleet behind it, so the artifact carries no actual file
// content -- only the wire-shape metadata (ArtifactId/ArtifactType/
// ArtifactFileName) a client would use to call GetArtifactUrl.
func newBuildArtifact(job *Job) *Artifact {
	const buildArtifactType = "BUILD"

	return &Artifact{
		ArtifactID:       randomID(),
		ArtifactType:     buildArtifactType,
		ArtifactFileName: job.JobID + "-build-output.zip",
		AppID:            job.AppID,
		BranchName:       job.BranchName,
		JobID:            job.JobID,
	}
}
