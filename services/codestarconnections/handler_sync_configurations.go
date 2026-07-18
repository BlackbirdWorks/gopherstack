package codestarconnections

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type createSyncConfigurationInput struct {
	Branch                  string `json:"Branch"`
	ConfigFile              string `json:"ConfigFile"`
	RepositoryLinkID        string `json:"RepositoryLinkId"`
	ResourceName            string `json:"ResourceName"`
	RoleArn                 string `json:"RoleArn"`
	SyncType                string `json:"SyncType"`
	PublishDeploymentStatus string `json:"PublishDeploymentStatus"`
	TriggerResourceUpdateOn string `json:"TriggerResourceUpdateOn"`
}

type syncConfigurationItem struct {
	Branch                  string `json:"Branch"`
	ConfigFile              string `json:"ConfigFile"`
	OwnerID                 string `json:"OwnerId"`
	ProviderType            string `json:"ProviderType"`
	RepositoryLinkID        string `json:"RepositoryLinkId"`
	RepositoryName          string `json:"RepositoryName"`
	ResourceName            string `json:"ResourceName"`
	RoleArn                 string `json:"RoleArn"`
	SyncType                string `json:"SyncType"`
	PublishDeploymentStatus string `json:"PublishDeploymentStatus,omitempty"`
	TriggerResourceUpdateOn string `json:"TriggerResourceUpdateOn,omitempty"`
}

type createSyncConfigurationOutput struct {
	SyncConfiguration syncConfigurationItem `json:"SyncConfiguration"`
}

func (h *Handler) handleCreateSyncConfiguration(
	ctx context.Context,
	in *createSyncConfigurationInput,
) (*createSyncConfigurationOutput, error) {
	if in.Branch == "" {
		return nil, fmt.Errorf("%w: Branch is required", errInvalidRequest)
	}

	if in.ConfigFile == "" {
		return nil, fmt.Errorf("%w: ConfigFile is required", errInvalidRequest)
	}

	if in.RepositoryLinkID == "" {
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", errInvalidRequest)
	}

	if in.ResourceName == "" {
		return nil, fmt.Errorf("%w: ResourceName is required", errInvalidRequest)
	}

	if in.RoleArn == "" {
		return nil, fmt.Errorf("%w: RoleArn is required", errInvalidRequest)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", errInvalidRequest)
	}

	cfg, err := h.Backend.CreateSyncConfigurationFull(
		ctx, in.Branch, in.ConfigFile, in.RepositoryLinkID, in.ResourceName, in.RoleArn, in.SyncType,
		in.PublishDeploymentStatus, in.TriggerResourceUpdateOn,
	)
	if err != nil {
		return nil, err
	}

	return &createSyncConfigurationOutput{SyncConfiguration: syncConfigToItem(cfg)}, nil
}

type getSyncConfigurationInput struct {
	ResourceName string `json:"ResourceName"`
	SyncType     string `json:"SyncType"`
}

type getSyncConfigurationOutput struct {
	SyncConfiguration syncConfigurationItem `json:"SyncConfiguration"`
}

func (h *Handler) handleGetSyncConfiguration(
	ctx context.Context,
	in *getSyncConfigurationInput,
) (*getSyncConfigurationOutput, error) {
	if in.ResourceName == "" {
		return nil, fmt.Errorf("%w: ResourceName is required", errInvalidRequest)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", errInvalidRequest)
	}

	cfg, err := h.Backend.GetSyncConfiguration(ctx, in.ResourceName, in.SyncType)
	if err != nil {
		return nil, err
	}

	return &getSyncConfigurationOutput{SyncConfiguration: syncConfigToItem(cfg)}, nil
}

type deleteSyncConfigurationInput struct {
	ResourceName string `json:"ResourceName"`
	SyncType     string `json:"SyncType"`
}

type deleteSyncConfigurationOutput struct{}

func (h *Handler) handleDeleteSyncConfiguration(
	ctx context.Context,
	in *deleteSyncConfigurationInput,
) (*deleteSyncConfigurationOutput, error) {
	if in.ResourceName == "" {
		return nil, fmt.Errorf("%w: ResourceName is required", errInvalidRequest)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteSyncConfiguration(ctx, in.ResourceName, in.SyncType); err != nil {
		return nil, err
	}

	return &deleteSyncConfigurationOutput{}, nil
}

func syncConfigToItem(cfg *SyncConfiguration) syncConfigurationItem {
	return syncConfigurationItem{
		Branch:                  cfg.Branch,
		ConfigFile:              cfg.ConfigFile,
		OwnerID:                 cfg.OwnerID,
		ProviderType:            cfg.ProviderType,
		RepositoryLinkID:        cfg.RepositoryLinkID,
		RepositoryName:          cfg.RepositoryName,
		ResourceName:            cfg.ResourceName,
		RoleArn:                 cfg.RoleArn,
		SyncType:                cfg.SyncType,
		PublishDeploymentStatus: cfg.PublishDeploymentStatus,
		TriggerResourceUpdateOn: cfg.TriggerResourceUpdateOn,
	}
}

type listRepositorySyncDefinitionsInput struct {
	RepositoryLinkID string `json:"RepositoryLinkId"`
	SyncType         string `json:"SyncType"`
}

type repositorySyncDefinitionItem struct {
	Branch    string `json:"Branch"`
	Directory string `json:"Directory"`
	Parent    string `json:"Parent,omitempty"`
	Target    string `json:"Target"`
}

type listRepositorySyncDefinitionsOutput struct {
	RepositorySyncDefinitions []repositorySyncDefinitionItem `json:"RepositorySyncDefinitions"`
}

func (h *Handler) handleListRepositorySyncDefinitions(
	ctx context.Context,
	in *listRepositorySyncDefinitionsInput,
) (*listRepositorySyncDefinitionsOutput, error) {
	if in.RepositoryLinkID == "" {
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", errInvalidRequest)
	}

	defs, err := h.Backend.ListRepositorySyncDefinitions(ctx, in.RepositoryLinkID, in.SyncType)
	if err != nil {
		return nil, err
	}

	items := make([]repositorySyncDefinitionItem, len(defs))
	for i, d := range defs {
		items[i] = repositorySyncDefinitionItem(d)
	}

	return &listRepositorySyncDefinitionsOutput{RepositorySyncDefinitions: items}, nil
}

type listSyncConfigurationsInput struct {
	RepositoryLinkID string `json:"RepositoryLinkId"`
	SyncType         string `json:"SyncType"`
	NextToken        string `json:"NextToken"`
	MaxResults       int    `json:"MaxResults"`
}

type listSyncConfigurationsOutput struct {
	NextToken          string                  `json:"NextToken,omitempty"`
	SyncConfigurations []syncConfigurationItem `json:"SyncConfigurations"`
}

func (h *Handler) handleListSyncConfigurations(
	ctx context.Context,
	in *listSyncConfigurationsInput,
) (*listSyncConfigurationsOutput, error) {
	if in.RepositoryLinkID == "" {
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", errInvalidRequest)
	}

	cfgs := h.Backend.ListSyncConfigurations(ctx, in.RepositoryLinkID, in.SyncType)
	items := make([]syncConfigurationItem, len(cfgs))

	for i, cfg := range cfgs {
		items[i] = syncConfigToItem(cfg)
	}

	p := page.New(items, in.NextToken, in.MaxResults, defaultCSCMaxResults)

	return &listSyncConfigurationsOutput{SyncConfigurations: p.Data, NextToken: p.Next}, nil
}

type updateSyncConfigurationInput struct {
	ResourceName            string `json:"ResourceName"`
	SyncType                string `json:"SyncType"`
	Branch                  string `json:"Branch"`
	ConfigFile              string `json:"ConfigFile"`
	RepositoryLinkID        string `json:"RepositoryLinkId"`
	RoleArn                 string `json:"RoleArn"`
	PublishDeploymentStatus string `json:"PublishDeploymentStatus"`
	TriggerResourceUpdateOn string `json:"TriggerResourceUpdateOn"`
}

type updateSyncConfigurationOutput struct {
	SyncConfiguration syncConfigurationItem `json:"SyncConfiguration"`
}

func (h *Handler) handleUpdateSyncConfiguration(
	ctx context.Context,
	in *updateSyncConfigurationInput,
) (*updateSyncConfigurationOutput, error) {
	if in.ResourceName == "" {
		return nil, fmt.Errorf("%w: ResourceName is required", errInvalidRequest)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", errInvalidRequest)
	}

	cfg, err := h.Backend.UpdateSyncConfigurationFull(
		ctx, in.ResourceName, in.SyncType, in.Branch, in.ConfigFile, in.RepositoryLinkID, in.RoleArn,
		in.PublishDeploymentStatus, in.TriggerResourceUpdateOn,
	)
	if err != nil {
		return nil, err
	}

	return &updateSyncConfigurationOutput{SyncConfiguration: syncConfigToItem(cfg)}, nil
}
