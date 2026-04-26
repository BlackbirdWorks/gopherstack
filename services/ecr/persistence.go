package ecr

import (
	"encoding/json"
	"maps"
)

type backendSnapshot struct {
	Repos                       map[string]*Repository                         `json:"repos"`
	Images                      map[string]map[string]*Image                   `json:"images"`
	PullThroughCacheRules       map[string]*PullThroughCacheRule               `json:"pullThroughCacheRules"`
	RepositoryCreationTemplates map[string]*RepositoryCreationTemplate         `json:"repositoryCreationTemplates"`
	LifecyclePolicies           map[string]string                              `json:"lifecyclePolicies"`
	LifecyclePolicyPreviews     map[string]*LifecyclePolicyPreviewResult       `json:"lifecyclePolicyPreviews,omitempty"`
	UploadedLayers              map[string]map[string]int64                    `json:"uploadedLayers"`
	RepoTags                    map[string]map[string]string                   `json:"repoTags,omitempty"`
	RepositoryPolicies          map[string]string                              `json:"repositoryPolicies,omitempty"`
	ImageScanFindings           map[string]map[string]*ImageScanFindingsResult `json:"imageScanFindings,omitempty"`
	AccountSettings             map[string]string                              `json:"accountSettings,omitempty"`
	PullTimeUpdateExclusions    map[string]*PullTimeUpdateExclusion            `json:"pullTimeUpdateExclusions,omitempty"`
	RegistryScanningConfig      *RegistryScanningSettings                      `json:"registryScanningConfig,omitempty"`
	ReplicationConfig           *ReplicationConfig                             `json:"replicationConfig,omitempty"`
	RegistryPolicy              string                                         `json:"registryPolicy"`
	SigningConfig               *SigningSettings                               `json:"signingConfig,omitempty"`
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

	lifecyclePolicyPreviews := make(map[string]*LifecyclePolicyPreviewResult, len(b.lifecyclePolicyPreviews))
	for repo, preview := range b.lifecyclePolicyPreviews {
		cp := *preview
		cp.PreviewResults = append([]ImageIdentifier(nil), preview.PreviewResults...)
		lifecyclePolicyPreviews[repo] = &cp
	}

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

	repositoryPolicies := make(map[string]string, len(b.repositoryPolicies))
	maps.Copy(repositoryPolicies, b.repositoryPolicies)

	imageScanFindings := make(map[string]map[string]*ImageScanFindingsResult, len(b.imageScanFindings))
	for repo, findings := range b.imageScanFindings {
		cp := make(map[string]*ImageScanFindingsResult, len(findings))
		for digest, result := range findings {
			resultCp := copyImageScanFindingsResult(result)
			cp[digest] = &resultCp
		}
		imageScanFindings[repo] = cp
	}

	accountSettings := make(map[string]string, len(b.accountSettings))
	maps.Copy(accountSettings, b.accountSettings)

	pullTimeUpdateExclusions := make(map[string]*PullTimeUpdateExclusion, len(b.pullTimeUpdateExclusions))
	for principal, exclusion := range b.pullTimeUpdateExclusions {
		cp := *exclusion
		pullTimeUpdateExclusions[principal] = &cp
	}

	snap := backendSnapshot{
		Repos:                       repos,
		Images:                      images,
		PullThroughCacheRules:       ptcRules,
		RepositoryCreationTemplates: templates,
		LifecyclePolicies:           lifecyclePolicies,
		LifecyclePolicyPreviews:     lifecyclePolicyPreviews,
		RegistryPolicy:              b.registryPolicy,
		UploadedLayers:              uploadedLayers,
		RepoTags:                    repoTags,
		RepositoryPolicies:          repositoryPolicies,
		ImageScanFindings:           imageScanFindings,
		AccountSettings:             accountSettings,
		PullTimeUpdateExclusions:    pullTimeUpdateExclusions,
		RegistryScanningConfig:      copyRegistryScanningSettings(b.registryScanningConfig),
		ReplicationConfig:           copyReplicationConfig(b.replicationConfig),
		SigningConfig:               copySigningSettings(b.signingConfig),
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

	lifecyclePolicyPreviews := make(map[string]*LifecyclePolicyPreviewResult, len(snap.LifecyclePolicyPreviews))
	for repo, preview := range snap.LifecyclePolicyPreviews {
		cp := *preview
		cp.PreviewResults = append([]ImageIdentifier(nil), preview.PreviewResults...)
		lifecyclePolicyPreviews[repo] = &cp
	}

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

	repositoryPolicies := make(map[string]string, len(snap.RepositoryPolicies))
	maps.Copy(repositoryPolicies, snap.RepositoryPolicies)

	imageScanFindings := make(map[string]map[string]*ImageScanFindingsResult, len(snap.ImageScanFindings))
	for repo, findings := range snap.ImageScanFindings {
		cp := make(map[string]*ImageScanFindingsResult, len(findings))
		for digest, result := range findings {
			resultCp := copyImageScanFindingsResult(result)
			cp[digest] = &resultCp
		}
		imageScanFindings[repo] = cp
	}

	accountSettings := make(map[string]string, len(snap.AccountSettings))
	maps.Copy(accountSettings, snap.AccountSettings)

	pullTimeUpdateExclusions := make(map[string]*PullTimeUpdateExclusion, len(snap.PullTimeUpdateExclusions))
	for principal, exclusion := range snap.PullTimeUpdateExclusions {
		cp := *exclusion
		pullTimeUpdateExclusions[principal] = &cp
	}

	b.repos = repos
	b.images = images
	b.pullThroughCacheRules = ptcRules
	b.repositoryCreationTemplates = templates
	b.lifecyclePolicies = lifecyclePolicies
	b.lifecyclePolicyPreviews = lifecyclePolicyPreviews
	b.registryPolicy = snap.RegistryPolicy
	b.uploadedLayers = uploadedLayers
	b.repoTags = repoTags
	b.repositoryPolicies = repositoryPolicies
	b.imageScanFindings = imageScanFindings
	b.accountSettings = accountSettings
	b.pullTimeUpdateExclusions = pullTimeUpdateExclusions
	b.registryScanningConfig = copyRegistryScanningSettings(snap.RegistryScanningConfig)
	b.replicationConfig = copyReplicationConfig(snap.ReplicationConfig)
	b.signingConfig = copySigningSettings(snap.SigningConfig)

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
