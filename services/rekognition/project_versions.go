package rekognition

import (
	"fmt"
	"sort"
	"time"
)

func (b *InMemoryBackend) projectVersionARN(projectARN, versionName string) string {
	return fmt.Sprintf("%s/version/%s", projectARN, versionName)
}

// CreateProjectVersion creates a new model version within a project.
func (b *InMemoryBackend) CreateProjectVersion(projectARN, versionName string) (*ProjectVersion, error) {
	b.mu.Lock("CreateProjectVersion")
	defer b.mu.Unlock()

	if !b.projects.Has(projectARN) {
		return nil, ErrProjectNotFound
	}

	arn := b.projectVersionARN(projectARN, versionName)

	if b.projectVersions.Has(arn) {
		return nil, ErrProjectVersionAlreadyExists
	}

	v := &storedProjectVersion{
		CreationTimestamp: time.Now(),
		ProjectVersionARN: arn,
		ProjectARN:        projectARN,
		VersionName:       versionName,
		Status:            "TRAINING_IN_PROGRESS",
	}
	b.projectVersions.Put(v)

	return v.toProjectVersion(), nil
}

// DeleteProjectVersion deletes a project version.
func (b *InMemoryBackend) DeleteProjectVersion(projectVersionARN string) error {
	b.mu.Lock("DeleteProjectVersion")
	defer b.mu.Unlock()

	if !b.projectVersions.Has(projectVersionARN) {
		return ErrProjectVersionNotFound
	}

	b.projectVersions.Delete(projectVersionARN)

	return nil
}

// DescribeProjectVersions lists versions for a project, optionally filtered by version names.
func (b *InMemoryBackend) DescribeProjectVersions(
	projectARN string, versionNames []string, maxResults int32, nextToken string,
) ([]*ProjectVersion, string, error) {
	b.mu.RLock("DescribeProjectVersions")
	defer b.mu.RUnlock()

	// Collect and sort version ARN keys that belong to this project.
	keys := make([]string, 0)
	for _, v := range b.projectVersions.All() {
		if v.ProjectARN == projectARN {
			keys = append(keys, v.ProjectVersionARN)
		}
	}
	sort.Strings(keys)

	// Build a filter set if requested.
	filter := make(map[string]bool, len(versionNames))
	for _, name := range versionNames {
		filter[name] = true
	}

	start := 0
	if nextToken != "" {
		for i, k := range keys {
			if k == nextToken {
				start = i

				break
			}
		}
	}

	const maxPerPage = 100
	limit := int32(maxPerPage)
	if maxResults > 0 && maxResults < limit {
		limit = maxResults
	}

	var result []*ProjectVersion
	var outToken string
	count := int32(0)

	for i := start; i < len(keys); i++ {
		k := keys[i]
		v, _ := b.projectVersions.Get(k)

		if len(filter) > 0 && !filter[v.VersionName] {
			continue
		}

		if count >= limit {
			outToken = k

			break
		}

		result = append(result, v.toProjectVersion())
		count++
	}

	return result, outToken, nil
}

// CopyProjectVersion copies a project version to another project.
func (b *InMemoryBackend) CopyProjectVersion(
	sourceProjectVersionARN, destinationProjectARN, versionName string,
) (*ProjectVersion, error) {
	b.mu.Lock("CopyProjectVersion")
	defer b.mu.Unlock()

	src, exists := b.projectVersions.Get(sourceProjectVersionARN)
	if !exists {
		return nil, ErrProjectVersionNotFound
	}

	if !b.projects.Has(destinationProjectARN) {
		return nil, ErrProjectNotFound
	}

	name := versionName
	if name == "" {
		name = src.VersionName
	}

	newARN := b.projectVersionARN(destinationProjectARN, name)

	v := &storedProjectVersion{
		CreationTimestamp: time.Now(),
		ProjectVersionARN: newARN,
		ProjectARN:        destinationProjectARN,
		VersionName:       name,
		Status:            "COPYING_IN_PROGRESS",
	}
	b.projectVersions.Put(v)

	return v.toProjectVersion(), nil
}

// StartProjectVersion sets a project version status to RUNNING.
func (b *InMemoryBackend) StartProjectVersion(projectVersionARN string, minInferenceUnits int32) error {
	b.mu.Lock("StartProjectVersion")
	defer b.mu.Unlock()

	v, exists := b.projectVersions.Get(projectVersionARN)
	if !exists {
		return ErrProjectVersionNotFound
	}

	v.Status = processorRunning
	v.MinInferenceUnits = minInferenceUnits

	return nil
}

// StopProjectVersion sets a project version status to STOPPED.
func (b *InMemoryBackend) StopProjectVersion(projectVersionARN string) error {
	b.mu.Lock("StopProjectVersion")
	defer b.mu.Unlock()

	v, exists := b.projectVersions.Get(projectVersionARN)
	if !exists {
		return ErrProjectVersionNotFound
	}

	v.Status = processorStopped

	return nil
}
