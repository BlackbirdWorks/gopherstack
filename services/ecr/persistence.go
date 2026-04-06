package ecr

import (
	"encoding/json"
	"maps"
)

type backendSnapshot struct {
	Repos                       map[string]*Repository                 `json:"repos"`
	Images                      map[string]map[string]*Image           `json:"images"`
	PullThroughCacheRules       map[string]*PullThroughCacheRule       `json:"pullThroughCacheRules"`
	RepositoryCreationTemplates map[string]*RepositoryCreationTemplate `json:"repositoryCreationTemplates"`
	LifecyclePolicies           map[string]string                      `json:"lifecyclePolicies"`
	UploadedLayers              map[string]map[string]int64            `json:"uploadedLayers"`
	RepoTags                    map[string]map[string]string           `json:"repoTags,omitempty"`
	RegistryPolicy              string                                 `json:"registryPolicy"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	repos := make(map[string]*Repository, len(b.repos))
	for k, v := range b.repos {
		cp := *v
		repos[k] = &cp
	}

	images := make(map[string]map[string]*Image, len(b.images))
	for repo, imgMap := range b.images {
		cp := make(map[string]*Image, len(imgMap))
		for digest, img := range imgMap {
			imgCp := *img
			cp[digest] = &imgCp
		}
		images[repo] = cp
	}

	ptcRules := make(map[string]*PullThroughCacheRule, len(b.pullThroughCacheRules))
	for k, v := range b.pullThroughCacheRules {
		cp := *v
		ptcRules[k] = &cp
	}

	templates := make(map[string]*RepositoryCreationTemplate, len(b.repositoryCreationTemplates))
	for k, v := range b.repositoryCreationTemplates {
		cp := *v
		templates[k] = &cp
	}

	lifecyclePolicies := make(map[string]string, len(b.lifecyclePolicies))
	maps.Copy(lifecyclePolicies, b.lifecyclePolicies)

	uploadedLayers := make(map[string]map[string]int64, len(b.uploadedLayers))
	for repo, layers := range b.uploadedLayers {
		cp := make(map[string]int64, len(layers))
		maps.Copy(cp, layers)
		uploadedLayers[repo] = cp
	}

	repoTags := make(map[string]map[string]string, len(b.repoTags))
	for arn, tags := range b.repoTags {
		cp := make(map[string]string, len(tags))
		maps.Copy(cp, tags)
		repoTags[arn] = cp
	}

	snap := backendSnapshot{
		Repos:                       repos,
		Images:                      images,
		PullThroughCacheRules:       ptcRules,
		RepositoryCreationTemplates: templates,
		LifecyclePolicies:           lifecyclePolicies,
		RegistryPolicy:              b.registryPolicy,
		UploadedLayers:              uploadedLayers,
		RepoTags:                    repoTags,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Repos == nil {
		snap.Repos = make(map[string]*Repository)
	}

	repos := make(map[string]*Repository, len(snap.Repos))
	for k, v := range snap.Repos {
		cp := *v
		repos[k] = &cp
	}

	images := make(map[string]map[string]*Image, len(snap.Images))
	for repo, imgMap := range snap.Images {
		cp := make(map[string]*Image, len(imgMap))
		for digest, img := range imgMap {
			imgCp := *img
			cp[digest] = &imgCp
		}
		images[repo] = cp
	}

	ptcRules := make(map[string]*PullThroughCacheRule, len(snap.PullThroughCacheRules))
	for k, v := range snap.PullThroughCacheRules {
		cp := *v
		ptcRules[k] = &cp
	}

	templates := make(map[string]*RepositoryCreationTemplate, len(snap.RepositoryCreationTemplates))
	for k, v := range snap.RepositoryCreationTemplates {
		cp := *v
		templates[k] = &cp
	}

	lifecyclePolicies := make(map[string]string, len(snap.LifecyclePolicies))
	maps.Copy(lifecyclePolicies, snap.LifecyclePolicies)

	uploadedLayers := make(map[string]map[string]int64, len(snap.UploadedLayers))
	for repo, layers := range snap.UploadedLayers {
		cp := make(map[string]int64, len(layers))
		maps.Copy(cp, layers)
		uploadedLayers[repo] = cp
	}

	repoTags := make(map[string]map[string]string, len(snap.RepoTags))
	for arn, tags := range snap.RepoTags {
		cp := make(map[string]string, len(tags))
		maps.Copy(cp, tags)
		repoTags[arn] = cp
	}

	b.repos = repos
	b.images = images
	b.pullThroughCacheRules = ptcRules
	b.repositoryCreationTemplates = templates
	b.lifecyclePolicies = lifecyclePolicies
	b.registryPolicy = snap.RegistryPolicy
	b.uploadedLayers = uploadedLayers
	b.repoTags = repoTags

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend
// when it implements Snapshottable. Returns nil for non-snapshottable backends.
func (h *Handler) Snapshot() []byte {
	if s, ok := h.Backend.(Snapshottable); ok {
		return s.Snapshot()
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend
// when it implements Snapshottable. Non-snapshottable backends are skipped.
func (h *Handler) Restore(data []byte) error {
	if s, ok := h.Backend.(Snapshottable); ok {
		return s.Restore(data)
	}

	return nil
}
