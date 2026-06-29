package ecr

import (
	"context"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type backendSnapshot struct {
	RepositoryPolicies          map[string]string                              `json:"repositoryPolicies,omitempty"`
	ImageScanFindings           map[string]map[string]*ImageScanFindingsResult `json:"imageScanFindings,omitempty"`
	PullThroughCacheRules       map[string]*PullThroughCacheRule               `json:"pullThroughCacheRules"`
	RepositoryCreationTemplates map[string]*RepositoryCreationTemplate         `json:"repositoryCreationTemplates"`
	LifecyclePolicies           map[string]string                              `json:"lifecyclePolicies"`
	LifecyclePolicyPreviews     map[string]*LifecyclePolicyPreviewResult       `json:"lifecyclePolicyPreviews,omitempty"`
	Images                      map[string]map[string]*Image                   `json:"images"`
	UploadedLayers              map[string]map[string]int64                    `json:"uploadedLayers"`
	Repos                       map[string]*Repository                         `json:"repos"`
	RepoTags                    map[string]map[string]string                   `json:"repoTags,omitempty"`
	AccountSettings             map[string]string                              `json:"accountSettings,omitempty"`
	PullTimeUpdateExclusions    map[string]*PullTimeUpdateExclusion            `json:"pullTimeUpdateExclusions,omitempty"`
	RegistryScanningConfig      *RegistryScanningSettings                      `json:"registryScanningConfig,omitempty"`
	ReplicationConfig           *ReplicationConfig                             `json:"replicationConfig,omitempty"`
	SigningConfig               *SigningSettings                               `json:"signingConfig,omitempty"`
	RegistryPolicy              string                                         `json:"registryPolicy"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Repos:                       copyPointerMap(b.repos),
		Images:                      copyNestedPointerMap(b.images),
		PullThroughCacheRules:       copyPointerMap(b.pullThroughCacheRules),
		RepositoryCreationTemplates: copyPointerMap(b.repositoryCreationTemplates),
		LifecyclePolicies:           copyMap(b.lifecyclePolicies),
		LifecyclePolicyPreviews:     copyLifecyclePolicyPreviews(b.lifecyclePolicyPreviews),
		RegistryPolicy:              b.registryPolicy,
		UploadedLayers:              copyNestedMap(b.uploadedLayers),
		RepoTags:                    copyNestedMap(b.repoTags),
		RepositoryPolicies:          copyMap(b.repositoryPolicies),
		ImageScanFindings:           copyImageScanFindingsMap(b.imageScanFindings),
		AccountSettings:             copyMap(b.accountSettings),
		PullTimeUpdateExclusions:    copyPointerMap(b.pullTimeUpdateExclusions),
		RegistryScanningConfig:      copyRegistryScanningSettings(b.registryScanningConfig),
		ReplicationConfig:           copyReplicationConfig(b.replicationConfig),
		SigningConfig:               copySigningSettings(b.signingConfig),
	}

	return persistence.MarshalSnapshot(ctx, "ecr", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "ecr", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.repos = copyPointerMap(snap.Repos)
	b.images = copyNestedPointerMap(snap.Images)
	b.pullThroughCacheRules = copyPointerMap(snap.PullThroughCacheRules)
	b.repositoryCreationTemplates = copyPointerMap(snap.RepositoryCreationTemplates)
	b.lifecyclePolicies = copyMap(snap.LifecyclePolicies)
	b.lifecyclePolicyPreviews = copyLifecyclePolicyPreviews(snap.LifecyclePolicyPreviews)
	b.registryPolicy = snap.RegistryPolicy
	b.uploadedLayers = copyNestedMap(snap.UploadedLayers)
	b.repoTags = copyNestedMap(snap.RepoTags)
	b.repositoryPolicies = copyMap(snap.RepositoryPolicies)
	b.imageScanFindings = copyImageScanFindingsMap(snap.ImageScanFindings)
	b.accountSettings = copyMap(snap.AccountSettings)
	b.pullTimeUpdateExclusions = copyPointerMap(snap.PullTimeUpdateExclusions)
	b.registryScanningConfig = copyRegistryScanningSettings(snap.RegistryScanningConfig)
	b.replicationConfig = copyReplicationConfig(snap.ReplicationConfig)
	b.signingConfig = copySigningSettings(snap.SigningConfig)
	b.layerUploads = make(map[string]*layerUploadState)
	b.repoUploadIndex = make(map[string]map[string]struct{})
	b.layerUploadQueue = make([]layerUploadQueueEntry, 0)

	return nil
}

func copyMap[T any](in map[string]T) map[string]T {
	if in == nil {
		return make(map[string]T)
	}

	cp := make(map[string]T, len(in))
	maps.Copy(cp, in)

	return cp
}

func copyNestedMap[T any](in map[string]map[string]T) map[string]map[string]T {
	cp := make(map[string]map[string]T, len(in))
	for key, value := range in {
		cp[key] = copyMap(value)
	}

	return cp
}

func copyPointerMap[T any](in map[string]*T) map[string]*T {
	cp := make(map[string]*T, len(in))
	for key, value := range in {
		valueCp := *value
		cp[key] = &valueCp
	}

	return cp
}

func copyNestedPointerMap[T any](in map[string]map[string]*T) map[string]map[string]*T {
	cp := make(map[string]map[string]*T, len(in))
	for key, value := range in {
		cp[key] = copyPointerMap(value)
	}

	return cp
}

func copyLifecyclePolicyPreviews(
	in map[string]*LifecyclePolicyPreviewResult,
) map[string]*LifecyclePolicyPreviewResult {
	cp := make(map[string]*LifecyclePolicyPreviewResult, len(in))
	for repo, preview := range in {
		previewCp := *preview
		previewCp.PreviewResults = append([]ImageIdentifier(nil), preview.PreviewResults...)
		cp[repo] = &previewCp
	}

	return cp
}

func copyImageScanFindingsMap(
	in map[string]map[string]*ImageScanFindingsResult,
) map[string]map[string]*ImageScanFindingsResult {
	cp := make(map[string]map[string]*ImageScanFindingsResult, len(in))
	for repo, findings := range in {
		repoCp := make(map[string]*ImageScanFindingsResult, len(findings))
		for digest, result := range findings {
			resultCp := copyImageScanFindingsResult(result)
			repoCp[digest] = &resultCp
		}
		cp[repo] = repoCp
	}

	return cp
}

// Snapshot implements persistence.Persistable by delegating to the backend
// when it implements Snapshottable. Returns nil for non-snapshottable backends.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	if s, ok := h.Backend.(Snapshottable); ok {
		return s.Snapshot(ctx)
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend
// when it implements Snapshottable. Non-snapshottable backends are skipped.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if s, ok := h.Backend.(Snapshottable); ok {
		return s.Restore(ctx, data)
	}

	return nil
}
