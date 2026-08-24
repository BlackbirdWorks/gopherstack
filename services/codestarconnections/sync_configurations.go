package codestarconnections

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"
)

// CreateSyncConfiguration creates a new sync configuration.
func (b *InMemoryBackend) CreateSyncConfiguration(
	ctx context.Context,
	branch, configFile, repositoryLinkID, resourceName, roleArn, syncType string,
) (*SyncConfiguration, error) {
	return b.CreateSyncConfigurationFull(
		ctx, branch, configFile, repositoryLinkID, resourceName, roleArn, syncType, "", "",
	)
}

// CreateSyncConfigurationFull creates a sync configuration with optional
// PublishDeploymentStatus and TriggerResourceUpdateOn.
func (b *InMemoryBackend) CreateSyncConfigurationFull(
	ctx context.Context,
	branch, configFile, repositoryLinkID, resourceName, roleArn, syncType,
	publishDeploymentStatus, triggerResourceUpdateOn string,
) (*SyncConfiguration, error) {
	if !validSyncTypes()[syncType] {
		return nil, fmt.Errorf("%w: invalid SyncType %q", ErrValidation, syncType)
	}

	if strings.Contains(resourceName, "/") {
		return nil, fmt.Errorf("%w: ResourceName must not contain \"/\"", ErrValidation)
	}

	if publishDeploymentStatus != "" && !validPublishDeploymentStatus()[publishDeploymentStatus] {
		return nil, fmt.Errorf("%w: invalid PublishDeploymentStatus %q", ErrValidation, publishDeploymentStatus)
	}

	if triggerResourceUpdateOn != "" && !validTriggerResourceUpdateOn()[triggerResourceUpdateOn] {
		return nil, fmt.Errorf("%w: invalid TriggerResourceUpdateOn %q", ErrValidation, triggerResourceUpdateOn)
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("CreateSyncConfiguration")
	defer b.mu.Unlock()

	// Derive owner/provider/repo from the link if it exists.
	ownerID := ""
	providerType := ""
	repoName := ""

	if link, ok := b.repositoryLinks.Get(regionKey(region, repositoryLinkID)); ok {
		ownerID = link.OwnerID
		providerType = link.ProviderType
		repoName = link.RepositoryName
	}

	// Check for duplicate.
	key := regionKey(region, syncConfigKey(resourceName, syncType))
	if b.syncConfigurations.Has(key) {
		return nil, fmt.Errorf("%w: sync configuration for %q/%q already exists",
			ErrResourceAlreadyExists, resourceName, syncType)
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
		CreatedAt:               time.Now().UTC(),
		region:                  region,
	}

	b.syncConfigurations.Put(cfg)

	// Seed an initial sync status for this resource. resourceSyncStatusKeyFn
	// (store_setup.go) derives the exact same composite key from
	// ResourceName/SyncType/region, so key is reused as-is.
	b.resourceSyncStatuses.Put(&ResourceSyncStatus{
		StartedAt:    time.Now().UTC(),
		Status:       SyncStatusSucceeded,
		Events:       []SyncEvent{},
		region:       region,
		resourceName: resourceName,
		syncType:     syncType,
	})

	cp := *cfg

	return &cp, nil
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
		return nil, fmt.Errorf("%w: sync configuration not found: %s/%s", ErrNotFound, resourceName, syncType)
	}

	cp := *cfg

	return &cp, nil
}

// DeleteSyncConfiguration removes a sync configuration.
func (b *InMemoryBackend) DeleteSyncConfiguration(ctx context.Context, resourceName, syncType string) error {
	if resourceName == "" {
		return fmt.Errorf("%w: ResourceName is required", ErrValidation)
	}

	if !validSyncTypes()[syncType] {
		return fmt.Errorf("%w: invalid SyncType %q", ErrValidation, syncType)
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteSyncConfiguration")
	defer b.mu.Unlock()

	key := regionKey(region, syncConfigKey(resourceName, syncType))
	if !b.syncConfigurations.Has(key) {
		// DeleteSyncConfiguration is idempotent in real AWS:
		// DeleteSyncConfigurationOutput carries no fields at all
		// (codestarconnections@v1.38.4 api_op_DeleteSyncConfiguration.go),
		// and its own error switch has no ResourceNotFoundException case,
		// unlike GetSyncConfiguration (inference, confirmed independently
		// against sibling codeconnections@v1.13.4's identical switch).
		return nil
	}

	b.syncConfigurations.Delete(key)

	// Remove associated sync status (same composite key shape as above).
	b.resourceSyncStatuses.Delete(key)

	// Remove associated sync blockers. The Index result slice mutates as
	// entries are deleted from the underlying table, so it must be cloned
	// before the delete loop runs.
	for _, blocker := range slices.Clone(b.syncBlockersByResource.Get(key)) {
		b.syncBlockers.Delete(blocker.ID)
	}

	return nil
}

// ListRepositorySyncDefinitions returns the sync definitions derived from the
// sync configurations linked to repositoryLinkID, optionally filtered by
// syncType. Directory is sourced from each sync configuration's ConfigFile
// (per AWS docs: "This value comes from creating or updating the config-file
// field of a sync-configuration"). For CFN_STACK_SYNC -- the only SyncType
// gopherstack supports -- AWS docs state "the parent and target resource are
// the same", so Parent and Target both equal ResourceName.
func (b *InMemoryBackend) ListRepositorySyncDefinitions(
	ctx context.Context,
	repositoryLinkID, syncType string,
) ([]RepositorySyncDefinition, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListRepositorySyncDefinitions")
	defer b.mu.RUnlock()

	if !b.repositoryLinks.Has(regionKey(region, repositoryLinkID)) {
		return nil, fmt.Errorf("%w: repository link not found: %s", ErrNotFound, repositoryLinkID)
	}

	cfgs := b.syncConfigurationsByRegion.Get(region)
	result := make([]RepositorySyncDefinition, 0, len(cfgs))

	for _, cfg := range cfgs {
		if cfg.RepositoryLinkID != repositoryLinkID {
			continue
		}

		if syncType != "" && cfg.SyncType != syncType {
			continue
		}

		result = append(result, RepositorySyncDefinition{
			Branch:    cfg.Branch,
			Directory: cfg.ConfigFile,
			Parent:    cfg.ResourceName,
			Target:    cfg.ResourceName,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Target < result[j].Target
	})

	return result, nil
}

// ListSyncConfigurations returns all sync configurations for a given repository link and sync type.
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

// UpdateSyncConfiguration updates branch, config file, role ARN, or repository link for a sync configuration.
func (b *InMemoryBackend) UpdateSyncConfiguration(
	ctx context.Context,
	resourceName, syncType, branch, configFile, repositoryLinkID, roleArn string,
) (*SyncConfiguration, error) {
	return b.UpdateSyncConfigurationFull(
		ctx, resourceName, syncType, branch, configFile, repositoryLinkID, roleArn, "", "",
	)
}

// UpdateSyncConfigurationFull updates a sync configuration including optional publish/trigger fields.
func (b *InMemoryBackend) UpdateSyncConfigurationFull(
	ctx context.Context,
	resourceName, syncType, branch, configFile, repositoryLinkID, roleArn,
	publishDeploymentStatus, triggerResourceUpdateOn string,
) (*SyncConfiguration, error) {
	if syncType != "" && !validSyncTypes()[syncType] {
		return nil, fmt.Errorf("%w: invalid SyncType %q", ErrValidation, syncType)
	}

	if publishDeploymentStatus != "" && !validPublishDeploymentStatus()[publishDeploymentStatus] {
		return nil, fmt.Errorf("%w: invalid PublishDeploymentStatus %q", ErrValidation, publishDeploymentStatus)
	}

	if triggerResourceUpdateOn != "" && !validTriggerResourceUpdateOn()[triggerResourceUpdateOn] {
		return nil, fmt.Errorf("%w: invalid TriggerResourceUpdateOn %q", ErrValidation, triggerResourceUpdateOn)
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("UpdateSyncConfiguration")
	defer b.mu.Unlock()

	key := regionKey(region, syncConfigKey(resourceName, syncType))

	cfg, ok := b.syncConfigurations.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: sync configuration not found: %s/%s", ErrNotFound, resourceName, syncType)
	}

	// None of the fields below (Branch/ConfigFile/RepositoryLinkID/RoleArn/
	// PublishDeploymentStatus/TriggerResourceUpdateOn) are part of the
	// syncConfigurations key (region|ResourceName/SyncType) or the byRegion
	// index, so mutating the stored *SyncConfiguration in place is safe --
	// no Delete+Put needed.
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

	cp := *cfg

	return &cp, nil
}
