package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// ---------------------------------------------------------------------------
// FeatureGroup handlers
// ---------------------------------------------------------------------------

// createFeatureGroupInput mirrors CreateFeatureGroupInput
// (api_op_CreateFeatureGroup.go:29-119). EventTimeFeatureName,
// FeatureDefinitions and RecordIdentifierFeatureName are all "This member is
// required" but were previously never validated -- a request supplying only
// FeatureGroupName still succeeded.
type createFeatureGroupInput struct {
	FeatureGroupName            string              `json:"FeatureGroupName"`
	RecordIdentifierFeatureName string              `json:"RecordIdentifierFeatureName"`
	EventTimeFeatureName        string              `json:"EventTimeFeatureName"`
	Description                 string              `json:"Description,omitempty"`
	RoleArn                     string              `json:"RoleArn,omitempty"`
	OnlineStoreConfig           *OnlineStoreConfig  `json:"OnlineStoreConfig,omitempty"`
	OfflineStoreConfig          *OfflineStoreConfig `json:"OfflineStoreConfig,omitempty"`
	ThroughputConfig            *ThroughputConfig   `json:"ThroughputConfig,omitempty"`
	FeatureDefinitions          []FeatureDefinition `json:"FeatureDefinitions"`
	Tags                        []tagObject         `json:"Tags"`
}

func (h *Handler) handleCreateFeatureGroup(ctx context.Context, body []byte) ([]byte, error) {
	var req createFeatureGroupInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.FeatureGroupName == "" {
		return nil, fmt.Errorf("%w: FeatureGroupName is required", errInvalidRequest)
	}

	if req.RecordIdentifierFeatureName == "" {
		return nil, fmt.Errorf("%w: RecordIdentifierFeatureName is required", errInvalidRequest)
	}

	if req.EventTimeFeatureName == "" {
		return nil, fmt.Errorf("%w: EventTimeFeatureName is required", errInvalidRequest)
	}

	if len(req.FeatureDefinitions) == 0 {
		return nil, fmt.Errorf("%w: FeatureDefinitions is required", errInvalidRequest)
	}

	fg, err := h.Backend.CreateFeatureGroup(ctx, CreateFeatureGroupOptions{
		FeatureGroupName:            req.FeatureGroupName,
		RecordIdentifierFeatureName: req.RecordIdentifierFeatureName,
		EventTimeFeatureName:        req.EventTimeFeatureName,
		Description:                 req.Description,
		RoleArn:                     req.RoleArn,
		FeatureDefinitions:          req.FeatureDefinitions,
		Tags:                        fromTagObjects(req.Tags),
		OnlineStoreConfig:           req.OnlineStoreConfig,
		OfflineStoreConfig:          req.OfflineStoreConfig,
		ThroughputConfig:            req.ThroughputConfig,
	})
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).
		InfoContext(ctx, "sagemaker: created feature group", "name", fg.FeatureGroupName)

	return json.Marshal(map[string]string{keyFeatureGroupArn: fg.FeatureGroupArn})
}

// describeFeatureGroupInput mirrors DescribeFeatureGroupInput
// (api_op_DescribeFeatureGroup.go:29-42). NextToken paginates
// FeatureDefinitions on the real op; this backend never paginates them (a
// FeatureGroup here always returns every definition at once), so the
// request value is accepted but has no effect -- disclosed, not silently
// dropped, since the response always carries its own required NextToken="".
type describeFeatureGroupInput struct {
	FeatureGroupName string `json:"FeatureGroupName"`
	NextToken        string `json:"NextToken,omitempty"`
}

func (h *Handler) handleDescribeFeatureGroup(ctx context.Context, body []byte) ([]byte, error) {
	var req describeFeatureGroupInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.FeatureGroupName == "" {
		return nil, fmt.Errorf("%w: FeatureGroupName is required", errInvalidRequest)
	}

	fg, err := h.Backend.DescribeFeatureGroup(ctx, req.FeatureGroupName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		keyFeatureGroupName:            fg.FeatureGroupName,
		keyFeatureGroupArn:             fg.FeatureGroupArn,
		keyFeatureGroupStatus:          fg.FeatureGroupStatus,
		keyRecordIdentifierFeatureName: fg.RecordIdentifierFeatureName,
		keyEventTimeFeatureName:        fg.EventTimeFeatureName,
		keyFeatureDefinitions:          fg.FeatureDefinitions,
		keyCreationTime:                epochSeconds(fg.CreationTime),
		keyLastModifiedTime:            epochSeconds(fg.LastModifiedTime),
		// api_op_DescribeFeatureGroup.go:60-63: NextToken is "This member is
		// required" on the response -- previously absent entirely.
		keyNextToken: "",
	}
	if fg.Description != "" {
		resp["Description"] = fg.Description
	}
	if fg.RoleArn != "" {
		resp[keyRoleArn] = fg.RoleArn
	}

	if fg.OnlineStoreConfig != nil {
		resp["OnlineStoreConfig"] = fg.OnlineStoreConfig
	}

	if fg.OfflineStoreConfig != nil {
		resp["OfflineStoreConfig"] = fg.OfflineStoreConfig
	}

	if fg.ThroughputConfig != nil {
		resp["ThroughputConfig"] = fg.ThroughputConfig
	}

	if fg.OfflineStoreStatus != "" {
		resp["OfflineStoreStatus"] = fg.OfflineStoreStatus
	}

	if fg.LastUpdateStatus != nil {
		resp["LastUpdateStatus"] = fg.LastUpdateStatus
	}

	return json.Marshal(resp)
}

type featureGroupSummary struct {
	FeatureGroupName   string  `json:"FeatureGroupName"`
	FeatureGroupArn    string  `json:"FeatureGroupArn"`
	FeatureGroupStatus string  `json:"FeatureGroupStatus"`
	OfflineStoreStatus string  `json:"OfflineStoreStatus,omitempty"`
	CreationTime       float64 `json:"CreationTime"`
}

// listFeatureGroupsInput mirrors ListFeatureGroupsInput
// (api_op_ListFeatureGroups.go:29-64). Previously this decoded only
// NextToken.
type listFeatureGroupsInput struct {
	CreationTimeAfter        *float64 `json:"CreationTimeAfter"`
	CreationTimeBefore       *float64 `json:"CreationTimeBefore"`
	FeatureGroupStatusEquals string   `json:"FeatureGroupStatusEquals"`
	NameContains             string   `json:"NameContains"`
	OfflineStoreStatusEquals string   `json:"OfflineStoreStatusEquals"`
	SortBy                   string   `json:"SortBy"`
	SortOrder                string   `json:"SortOrder"`
	NextToken                string   `json:"NextToken"`
	MaxResults               int32    `json:"MaxResults"`
}

func (h *Handler) handleListFeatureGroups(ctx context.Context, body []byte) ([]byte, error) {
	var req listFeatureGroupsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	fgs, nextToken := h.Backend.ListFeatureGroups(ctx, req.NextToken, ListFeatureGroupsFilter{
		CreationTimeAfter:        epochPtr(req.CreationTimeAfter),
		CreationTimeBefore:       epochPtr(req.CreationTimeBefore),
		FeatureGroupStatusEquals: req.FeatureGroupStatusEquals,
		NameContains:             req.NameContains,
		OfflineStoreStatusEquals: req.OfflineStoreStatusEquals,
		SortBy:                   req.SortBy,
		SortOrder:                req.SortOrder,
		MaxResults:               req.MaxResults,
	})
	summaries := make([]featureGroupSummary, 0, len(fgs))

	for _, fg := range fgs {
		summaries = append(summaries, featureGroupSummary{
			FeatureGroupName:   fg.FeatureGroupName,
			FeatureGroupArn:    fg.FeatureGroupArn,
			FeatureGroupStatus: fg.FeatureGroupStatus,
			OfflineStoreStatus: fg.OfflineStoreStatus,
			CreationTime:       epochSeconds(fg.CreationTime),
		})
	}

	resp := map[string]any{"FeatureGroupSummaries": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

// deleteFeatureGroupInput mirrors DeleteFeatureGroupInput
// (api_op_DeleteFeatureGroup.go:29-37).
type deleteFeatureGroupInput struct {
	FeatureGroupName string `json:"FeatureGroupName"`
}

func (h *Handler) handleDeleteFeatureGroup(ctx context.Context, body []byte) error {
	var req deleteFeatureGroupInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.FeatureGroupName == "" {
		return fmt.Errorf("%w: FeatureGroupName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteFeatureGroup(ctx, req.FeatureGroupName); err != nil {
		return err
	}

	logger.Load(ctx).
		InfoContext(ctx, "sagemaker: deleted feature group", "name", req.FeatureGroupName)

	return nil
}

// ---------------------------------------------------------------------------
// UpdateFeatureGroup handler (gap #19)
// ---------------------------------------------------------------------------

// updateFeatureGroupInput mirrors UpdateFeatureGroupInput
// (api_op_UpdateFeatureGroup.go:38-63). OnlineStoreConfig and
// ThroughputConfig were previously entirely absent from decode -- see
// [UpdateFeatureGroupOptions]'s doc for the bug this fixes.
type updateFeatureGroupInput struct {
	FeatureGroupName   string                   `json:"FeatureGroupName"`
	OnlineStoreConfig  *OnlineStoreConfigUpdate `json:"OnlineStoreConfig,omitempty"`
	ThroughputConfig   *ThroughputConfigUpdate  `json:"ThroughputConfig,omitempty"`
	FeatureDefinitions []FeatureDefinition      `json:"FeatureAdditions,omitempty"`
}

func (h *Handler) handleUpdateFeatureGroup(ctx context.Context, body []byte) ([]byte, error) {
	var req updateFeatureGroupInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.FeatureGroupName == "" {
		return nil, fmt.Errorf("%w: FeatureGroupName is required", errInvalidRequest)
	}

	fg, err := h.Backend.UpdateFeatureGroup(ctx, req.FeatureGroupName, UpdateFeatureGroupOptions{
		FeatureAdditions:  req.FeatureDefinitions,
		OnlineStoreConfig: req.OnlineStoreConfig,
		ThroughputConfig:  req.ThroughputConfig,
	})
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: updated feature group", "name", fg.FeatureGroupName)

	return json.Marshal(map[string]string{keyFeatureGroupArn: fg.FeatureGroupArn})
}
