package codeconnections

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// CreateSyncConfiguration creates a new sync configuration.
func (b *InMemoryBackend) CreateSyncConfiguration(
	ctx context.Context,
	branch, configFile, repositoryLinkID, resourceName, roleArn, syncType string,
	publishDeploymentStatus, triggerResourceUpdateOn, pullRequestComment string,
) (*SyncConfiguration, error) {
	if !validSyncTypes()[syncType] {
		return nil, fmt.Errorf("%w: invalid SyncType %q", ErrValidation, syncType)
	}

	if !validEnabledDisabled()[publishDeploymentStatus] {
		return nil, fmt.Errorf("%w: invalid PublishDeploymentStatus %q", ErrValidation, publishDeploymentStatus)
	}

	if !validTriggerResourceUpdateOn()[triggerResourceUpdateOn] {
		return nil, fmt.Errorf("%w: invalid TriggerResourceUpdateOn %q", ErrValidation, triggerResourceUpdateOn)
	}

	if !validEnabledDisabled()[pullRequestComment] {
		return nil, fmt.Errorf("%w: invalid PullRequestComment %q", ErrValidation, pullRequestComment)
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("CreateSyncConfiguration")
	defer b.mu.Unlock()

	// Derive owner/provider/repo from repository link if present.
	ownerID := ""
	providerType := ""
	repoName := ""

	if link, ok := b.repositoryLinks.Get(regionKey(region, repositoryLinkID)); ok {
		ownerID = link.OwnerID
		providerType = link.ProviderType
		repoName = link.RepositoryName
	}

	// Check for duplicate: the real CreateSyncConfiguration operation registers
	// a dedicated ResourceAlreadyExistsException for an existing ResourceName+SyncType.
	key := regionKey(region, syncConfigKey(resourceName, syncType))
	if b.syncConfigurations.Has(key) {
		return nil, fmt.Errorf(
			"%w: sync configuration for %q/%q already exists", ErrAlreadyExists, resourceName, syncType,
		)
	}

	cfg := &SyncConfiguration{
		Branch:                  branch,
		ConfigFile:              configFile,
		RepositoryLinkID:        repositoryLinkID,
		ResourceName:            resourceName,
		RoleArn:                 roleArn,
		SyncType:                syncType,
		OwnerID:                 ownerID,
		ProviderType:            providerType,
		RepositoryName:          repoName,
		PublishDeploymentStatus: publishDeploymentStatus,
		TriggerResourceUpdateOn: triggerResourceUpdateOn,
		PullRequestComment:      pullRequestComment,
		CreatedAt:               time.Now().UTC(),
		region:                  region,
	}

	b.syncConfigurations.Put(cfg)

	cp := *cfg

	return &cp, nil
}

// DeleteSyncConfiguration removes a sync configuration.
func (b *InMemoryBackend) DeleteSyncConfiguration(
	ctx context.Context,
	resourceName, syncType string,
) error {
	if syncType != "" && !validSyncTypes()[syncType] {
		return fmt.Errorf("%w: invalid SyncType %q", ErrValidation, syncType)
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteSyncConfiguration")
	defer b.mu.Unlock()

	key := regionKey(region, syncConfigKey(resourceName, syncType))
	if !b.syncConfigurations.Has(key) {
		return ErrNotFound
	}

	b.syncConfigurations.Delete(key)

	// DeleteSyncConfiguration's real error list has no "blocker still
	// exists"-style error (botocore codeconnections/2023-12-01/
	// service-2.json), so deletion is unconditional -- but any sync blockers
	// for this resource+syncType are now unreachable through
	// GetSyncBlockerSummary (which requires the sync configuration to still
	// exist) while still occupying b.syncBlockers forever. Without this
	// cleanup they are also a ghost-data-resurrection bug: recreating a sync
	// configuration for the same resourceName+syncType would make the OLD,
	// already-resolved blockers reappear via GetSyncBlockerSummary.
	for _, blocker := range b.syncBlockersByResource.Get(key) {
		b.syncBlockers.Delete(blocker.ID)
	}

	return nil
}

// GetSyncConfiguration retrieves a sync configuration by resource name and sync type.
func (b *InMemoryBackend) GetSyncConfiguration(
	ctx context.Context,
	resourceName, syncType string,
) (*SyncConfiguration, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("GetSyncConfiguration")
	defer b.mu.RUnlock()

	cfg, ok := b.syncConfigurations.Get(regionKey(region, syncConfigKey(resourceName, syncType)))
	if !ok {
		return nil, ErrNotFound
	}

	cp := *cfg

	return &cp, nil
}

// ListSyncConfigurations returns all sync configurations for a repository link and sync type.
func (b *InMemoryBackend) ListSyncConfigurations(
	ctx context.Context,
	repositoryLinkID, syncType string,
) []*SyncConfiguration {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListSyncConfigurations")
	defer b.mu.RUnlock()

	cfgs := b.syncConfigurationsByRegion.Get(region)
	result := make([]*SyncConfiguration, 0, len(cfgs))

	for _, cfg := range cfgs {
		if cfg.RepositoryLinkID != repositoryLinkID {
			continue
		}

		if syncType != "" && cfg.SyncType != syncType {
			continue
		}

		cp := *cfg
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ResourceName < result[j].ResourceName
	})

	return result
}

// UpdateSyncConfiguration updates fields on an existing sync configuration.
func (b *InMemoryBackend) UpdateSyncConfiguration(
	ctx context.Context,
	resourceName, syncType, branch, configFile, repositoryLinkID, roleArn string,
	publishDeploymentStatus, triggerResourceUpdateOn, pullRequestComment string,
) (*SyncConfiguration, error) {
	if syncType != "" && !validSyncTypes()[syncType] {
		return nil, fmt.Errorf("%w: invalid SyncType %q", ErrValidation, syncType)
	}

	if !validEnabledDisabled()[publishDeploymentStatus] {
		return nil, fmt.Errorf("%w: invalid PublishDeploymentStatus %q", ErrValidation, publishDeploymentStatus)
	}

	if !validTriggerResourceUpdateOn()[triggerResourceUpdateOn] {
		return nil, fmt.Errorf("%w: invalid TriggerResourceUpdateOn %q", ErrValidation, triggerResourceUpdateOn)
	}

	if !validEnabledDisabled()[pullRequestComment] {
		return nil, fmt.Errorf("%w: invalid PullRequestComment %q", ErrValidation, pullRequestComment)
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("UpdateSyncConfiguration")
	defer b.mu.Unlock()

	key := regionKey(region, syncConfigKey(resourceName, syncType))
	cfg, ok := b.syncConfigurations.Get(key)

	if !ok {
		return nil, ErrNotFound
	}

	// None of the fields below are part of the syncConfigurations key
	// (region|ResourceName/SyncType) or the byRegion index, so mutating the
	// stored *SyncConfiguration in place is safe -- no Delete+Put needed.
	if branch != "" {
		cfg.Branch = branch
	}

	if configFile != "" {
		cfg.ConfigFile = configFile
	}

	if repositoryLinkID != "" {
		cfg.RepositoryLinkID = repositoryLinkID
	}

	if roleArn != "" {
		cfg.RoleArn = roleArn
	}

	if publishDeploymentStatus != "" {
		cfg.PublishDeploymentStatus = publishDeploymentStatus
	}

	if triggerResourceUpdateOn != "" {
		cfg.TriggerResourceUpdateOn = triggerResourceUpdateOn
	}

	if pullRequestComment != "" {
		cfg.PullRequestComment = pullRequestComment
	}

	cp := *cfg

	return &cp, nil
}

// GetSyncBlockerSummary returns the real sync blocker summary for a resource.
func (b *InMemoryBackend) GetSyncBlockerSummary(
	ctx context.Context,
	resourceName, syncType string,
) (*SyncBlockerSummary, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("GetSyncBlockerSummary")
	defer b.mu.RUnlock()

	key := regionKey(region, syncConfigKey(resourceName, syncType))
	if !b.syncConfigurations.Has(key) {
		return nil, ErrNotFound
	}

	group := b.syncBlockersByResource.Get(key)
	blockers := make([]SyncBlocker, 0, len(group))

	for _, blocker := range group {
		blockers = append(blockers, *blocker)
	}

	sort.Slice(blockers, func(i, j int) bool {
		return blockers[i].CreatedAt.After(blockers[j].CreatedAt)
	})

	return &SyncBlockerSummary{
		ResourceName:   resourceName,
		LatestBlockers: blockers,
	}, nil
}

// CreateSyncBlocker creates a new sync blocker for a resource (test helper + internal use).
func (b *InMemoryBackend) CreateSyncBlocker(
	ctx context.Context,
	resourceName, syncType, blockerType, createdReason string,
) (*SyncBlocker, error) {
	if !validSyncTypes()[syncType] {
		return nil, fmt.Errorf("%w: invalid SyncType %q", ErrValidation, syncType)
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("CreateSyncBlocker")
	defer b.mu.Unlock()

	key := regionKey(region, syncConfigKey(resourceName, syncType))
	if !b.syncConfigurations.Has(key) {
		return nil, fmt.Errorf("%w: sync configuration not found: %s/%s", ErrNotFound, resourceName, syncType)
	}

	id := uuid.NewString()
	blocker := &SyncBlocker{
		ID:            id,
		Type:          blockerType,
		Status:        SyncBlockerStatusActive,
		CreatedAt:     time.Now().UTC(),
		CreatedReason: createdReason,
		ResourceName:  resourceName,
		SyncType:      syncType,
		region:        region,
	}

	b.syncBlockers.Put(blocker)

	cp := *blocker

	return &cp, nil
}

// UpdateSyncBlocker resolves a sync blocker by ID. If the blocker ID is not
// found (or was created in a different region than the caller's context),
// returns ErrSyncBlockerNotFound -- the real operation documents
// SyncBlockerDoesNotExistException for exactly this case, it does not resolve
// unknown IDs gracefully.
func (b *InMemoryBackend) UpdateSyncBlocker(
	ctx context.Context,
	id, resolvedReason string,
) (*SyncBlockerSummary, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("UpdateSyncBlocker")
	defer b.mu.Unlock()

	blocker, ok := b.syncBlockers.Get(id)
	if !ok || blocker.region != region {
		return nil, fmt.Errorf("%w: sync blocker not found: %s", ErrSyncBlockerNotFound, id)
	}

	now := time.Now().UTC()
	// Status/ResolvedReason/ResolvedAt are not part of any index key
	// (syncBlockers is keyed by ID; byResource derives from
	// region/ResourceName/SyncType, none of which change here), so mutating
	// the stored *SyncBlocker in place is safe -- no Delete+Put needed.
	blocker.Status = SyncBlockerStatusResolved
	blocker.ResolvedReason = resolvedReason
	blocker.ResolvedAt = &now

	key := regionKey(region, syncConfigKey(blocker.ResourceName, blocker.SyncType))
	summary := &SyncBlockerSummary{
		ResourceName:   blocker.ResourceName,
		LatestBlockers: []SyncBlocker{},
	}

	for _, b2 := range b.syncBlockersByResource.Get(key) {
		summary.LatestBlockers = append(summary.LatestBlockers, *b2)
	}

	sort.Slice(summary.LatestBlockers, func(i, j int) bool {
		return summary.LatestBlockers[i].CreatedAt.After(summary.LatestBlockers[j].CreatedAt)
	})

	return summary, nil
}
