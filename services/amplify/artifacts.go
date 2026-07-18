package amplify

import "fmt"

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

// ListArtifacts lists artifacts for a job.
func (b *InMemoryBackend) ListArtifacts(
	appID, _, _, nextToken string,
	maxResults int,
) ([]*Artifact, string, error) {
	b.mu.RLock("ListArtifacts")
	defer b.mu.RUnlock()

	if !b.apps.Has(appID) {
		return nil, "", fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	// For in-memory backend, return empty artifacts list (no actual build artifacts).
	page, token := amplifyPaginate([]*Artifact{}, nextToken, maxResults)

	return page, token, nil
}
