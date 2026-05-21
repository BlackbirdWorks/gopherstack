package ecr

// ImageCount returns the total number of images stored across all repositories.
// Used only in tests to verify backend state without going through the HTTP handler.
func (b *InMemoryBackend) ImageCount() int {
	b.mu.RLock("ImageCount")
	defer b.mu.RUnlock()

	total := 0

	for _, imgs := range b.images {
		total += len(imgs)
	}

	return total
}

// PullThroughCacheRuleCount returns the number of pull-through cache rules stored.
func (b *InMemoryBackend) PullThroughCacheRuleCount() int {
	b.mu.RLock("PullThroughCacheRuleCount")
	defer b.mu.RUnlock()

	return len(b.pullThroughCacheRules)
}

// RepositoryCreationTemplateCount returns the number of repository creation templates stored.
func (b *InMemoryBackend) RepositoryCreationTemplateCount() int {
	b.mu.RLock("RepositoryCreationTemplateCount")
	defer b.mu.RUnlock()

	return len(b.repositoryCreationTemplates)
}

// RepositoryCount returns the number of repositories.
// Used only in tests to verify backend state without going through the HTTP handler.
func (b *InMemoryBackend) RepositoryCount() int {
	b.mu.RLock("RepositoryCount")
	defer b.mu.RUnlock()

	return len(b.repos)
}

// LifecyclePolicyCount returns the number of lifecycle policies stored.
func (b *InMemoryBackend) LifecyclePolicyCount() int {
	b.mu.RLock("LifecyclePolicyCount")
	defer b.mu.RUnlock()

	return len(b.lifecyclePolicies)
}

// UploadedLayerCount returns the total number of uploaded layers.
// Used only in tests to verify backend state without going through the HTTP handler.
func (b *InMemoryBackend) UploadedLayerCount() int {
	b.mu.RLock("UploadedLayerCount")
	defer b.mu.RUnlock()

	total := 0
	for _, layers := range b.uploadedLayers {
		total += len(layers)
	}

	return total
}

// LayerUploadCount returns the number of in-progress layer upload sessions.
// Used only in tests to verify that DeleteRepository cleans up layer uploads.
func (b *InMemoryBackend) LayerUploadCount() int {
	b.mu.RLock("LayerUploadCount")
	defer b.mu.RUnlock()

	return len(b.layerUploads)
}

// TagIndexCount returns the total number of tag→digest entries across all repos.
func (b *InMemoryBackend) TagIndexCount() int {
	b.mu.RLock("TagIndexCount")
	defer b.mu.RUnlock()

	total := 0
	for _, idx := range b.tagIndex {
		total += len(idx)
	}

	return total
}

// RepoTagCount returns the number of tag→digest entries for a specific repository.
func (b *InMemoryBackend) RepoTagCount(repositoryName string) int {
	b.mu.RLock("RepoTagCount")
	defer b.mu.RUnlock()

	return len(b.tagIndex[repositoryName])
}

// TagDigest returns the digest a tag resolves to, or "" if not found.
func (b *InMemoryBackend) TagDigest(repositoryName, tag string) string {
	b.mu.RLock("TagDigest")
	defer b.mu.RUnlock()

	if idx, ok := b.tagIndex[repositoryName]; ok {
		return idx[tag]
	}

	return ""
}
