package codeconnections

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
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
	PullRequestComment      string `json:"PullRequestComment"`
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
	PullRequestComment      string `json:"PullRequestComment,omitempty"`
}

type createSyncConfigurationOutput struct {
	SyncConfiguration syncConfigurationItem `json:"SyncConfiguration"`
}

func (h *Handler) handleCreateSyncConfiguration(
	ctx context.Context,
	in *createSyncConfigurationInput,
) (*createSyncConfigurationOutput, error) {
	if in.Branch == "" {
		return nil, fmt.Errorf("%w: Branch is required", ErrValidation)
	}

	if in.ConfigFile == "" {
		return nil, fmt.Errorf("%w: ConfigFile is required", ErrValidation)
	}

	if in.RepositoryLinkID == "" {
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", ErrValidation)
	}

	if in.ResourceName == "" {
		return nil, fmt.Errorf("%w: ResourceName is required", ErrValidation)
	}

	if in.RoleArn == "" {
		return nil, fmt.Errorf("%w: RoleArn is required", ErrValidation)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", ErrValidation)
	}

	cfg, err := h.Backend.CreateSyncConfiguration(
		ctx,
		in.Branch,
		in.ConfigFile,
		in.RepositoryLinkID,
		in.ResourceName,
		in.RoleArn,
		in.SyncType,
		in.PublishDeploymentStatus,
		in.TriggerResourceUpdateOn,
		in.PullRequestComment,
	)
	if err != nil {
		return nil, err
	}

	return &createSyncConfigurationOutput{SyncConfiguration: syncConfigToItem(cfg)}, nil
}

type deleteSyncConfigurationInput struct {
	ResourceName string `json:"ResourceName"`
	SyncType     string `json:"SyncType"`
}

func (h *Handler) handleDeleteSyncConfiguration(
	ctx context.Context,
	in *deleteSyncConfigurationInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteSyncConfiguration(ctx, in.ResourceName, in.SyncType); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
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
		PullRequestComment:      cfg.PullRequestComment,
	}
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
		return nil, fmt.Errorf("%w: ResourceName is required", ErrValidation)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", ErrValidation)
	}

	cfg, err := h.Backend.GetSyncConfiguration(ctx, in.ResourceName, in.SyncType)
	if err != nil {
		return nil, err
	}

	return &getSyncConfigurationOutput{SyncConfiguration: syncConfigToItem(cfg)}, nil
}

type listSyncConfigurationsInput struct {
	NextToken        *string `json:"NextToken"`
	MaxResults       *int32  `json:"MaxResults"`
	RepositoryLinkID string  `json:"RepositoryLinkId"`
	SyncType         string  `json:"SyncType"`
}

type listSyncConfigurationsOutput struct {
	NextToken          *string                 `json:"NextToken,omitempty"`
	SyncConfigurations []syncConfigurationItem `json:"SyncConfigurations"`
}

func (h *Handler) handleListSyncConfigurations(
	ctx context.Context,
	in *listSyncConfigurationsInput,
) (*listSyncConfigurationsOutput, error) {
	if in.RepositoryLinkID == "" {
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", ErrValidation)
	}

	cfgs := h.Backend.ListSyncConfigurations(ctx, in.RepositoryLinkID, in.SyncType)
	items := make([]syncConfigurationItem, len(cfgs))

	for i, cfg := range cfgs {
		items[i] = syncConfigToItem(cfg)
	}

	var limit int
	if in.MaxResults != nil && *in.MaxResults > 0 {
		limit = int(*in.MaxResults)
	}

	token := ""
	if in.NextToken != nil {
		token = *in.NextToken
	}

	p := page.New(items, token, limit, ccDefaultPageSize)

	var nextToken *string
	if p.Next != "" {
		nextToken = &p.Next
	}

	return &listSyncConfigurationsOutput{SyncConfigurations: p.Data, NextToken: nextToken}, nil
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
	PullRequestComment      string `json:"PullRequestComment"`
}

type updateSyncConfigurationOutput struct {
	SyncConfiguration syncConfigurationItem `json:"SyncConfiguration"`
}

func (h *Handler) handleUpdateSyncConfiguration(
	ctx context.Context,
	in *updateSyncConfigurationInput,
) (*updateSyncConfigurationOutput, error) {
	if in.ResourceName == "" {
		return nil, fmt.Errorf("%w: ResourceName is required", ErrValidation)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", ErrValidation)
	}

	cfg, err := h.Backend.UpdateSyncConfiguration(
		ctx,
		in.ResourceName,
		in.SyncType,
		in.Branch,
		in.ConfigFile,
		in.RepositoryLinkID,
		in.RoleArn,
		in.PublishDeploymentStatus,
		in.TriggerResourceUpdateOn,
		in.PullRequestComment,
	)
	if err != nil {
		return nil, err
	}

	return &updateSyncConfigurationOutput{SyncConfiguration: syncConfigToItem(cfg)}, nil
}

type getSyncBlockerSummaryInput struct {
	ResourceName string `json:"ResourceName"`
	SyncType     string `json:"SyncType"`
}

// syncBlockerItem is the wire shape of a single sync blocker.
// CreatedAt/ResolvedAt are epoch-seconds JSON numbers on the wire (see
// smithytime.ParseEpochSeconds in the real SDK deserializer), not RFC3339
// strings.
type syncBlockerItem struct {
	ID             string  `json:"Id"`
	Type           string  `json:"Type"`
	Status         string  `json:"Status"`
	CreatedReason  string  `json:"CreatedReason"`
	ResolvedReason string  `json:"ResolvedReason,omitempty"`
	CreatedAt      float64 `json:"CreatedAt"`
	ResolvedAt     float64 `json:"ResolvedAt,omitempty"`
}

// syncBlockerToItem converts a backend SyncBlocker to its wire shape.
func syncBlockerToItem(b SyncBlocker) syncBlockerItem {
	item := syncBlockerItem{
		ID:            b.ID,
		Type:          b.Type,
		Status:        b.Status,
		CreatedAt:     awstime.Epoch(b.CreatedAt),
		CreatedReason: b.CreatedReason,
	}

	if b.ResolvedAt != nil {
		item.ResolvedAt = awstime.Epoch(*b.ResolvedAt)
		item.ResolvedReason = b.ResolvedReason
	}

	return item
}

type syncBlockerSummaryItem struct {
	ResourceName       string            `json:"ResourceName"`
	ParentResourceName string            `json:"ParentResourceName,omitempty"`
	LatestBlockers     []syncBlockerItem `json:"LatestBlockers"`
}

type getSyncBlockerSummaryOutput struct {
	SyncBlockerSummary syncBlockerSummaryItem `json:"SyncBlockerSummary"`
}

func (h *Handler) handleGetSyncBlockerSummary(
	ctx context.Context,
	in *getSyncBlockerSummaryInput,
) (*getSyncBlockerSummaryOutput, error) {
	if in.ResourceName == "" {
		return nil, fmt.Errorf("%w: ResourceName is required", ErrValidation)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", ErrValidation)
	}

	summary, err := h.Backend.GetSyncBlockerSummary(ctx, in.ResourceName, in.SyncType)
	if err != nil {
		return nil, err
	}

	blockers := make([]syncBlockerItem, len(summary.LatestBlockers))
	for i, b := range summary.LatestBlockers {
		blockers[i] = syncBlockerToItem(b)
	}

	return &getSyncBlockerSummaryOutput{
		SyncBlockerSummary: syncBlockerSummaryItem{
			ResourceName:       summary.ResourceName,
			ParentResourceName: summary.ParentResourceName,
			LatestBlockers:     blockers,
		},
	}, nil
}

type updateSyncBlockerInput struct {
	ID             string `json:"Id"`
	ResolvedReason string `json:"ResolvedReason"`
	ResourceName   string `json:"ResourceName"`
	SyncType       string `json:"SyncType"`
}

// updateSyncBlockerOutput is the UpdateSyncBlocker response shape. The real
// operation returns the single updated SyncBlocker object under the
// "SyncBlocker" key -- NOT a "SyncBlockerSummary" list (confirmed against
// aws-sdk-go-v2's UpdateSyncBlockerOutput, which only recognizes
// ResourceName/ParentResourceName/SyncBlocker).
type updateSyncBlockerOutput struct {
	ResourceName       string          `json:"ResourceName"`
	ParentResourceName string          `json:"ParentResourceName,omitempty"`
	SyncBlocker        syncBlockerItem `json:"SyncBlocker"`
}

func (h *Handler) handleUpdateSyncBlocker(
	ctx context.Context,
	in *updateSyncBlockerInput,
) (*updateSyncBlockerOutput, error) {
	if in.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", ErrValidation)
	}

	summary, err := h.Backend.UpdateSyncBlocker(ctx, in.ID, in.ResolvedReason)
	if err != nil {
		return nil, err
	}

	// The backend returns every blocker for the owning resource; pick out the
	// one that was just resolved (backend.UpdateSyncBlocker only succeeds when
	// in.ID exists, so it is always present here).
	var resolved SyncBlocker

	for _, b := range summary.LatestBlockers {
		if b.ID == in.ID {
			resolved = b

			break
		}
	}

	return &updateSyncBlockerOutput{
		ResourceName:       summary.ResourceName,
		ParentResourceName: summary.ParentResourceName,
		SyncBlocker:        syncBlockerToItem(resolved),
	}, nil
}
