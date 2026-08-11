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

func (h *Handler) handleCreateFeatureGroup(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
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

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.FeatureGroupName == "" {
		return nil, fmt.Errorf("%w: FeatureGroupName is required", errInvalidRequest)
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

func (h *Handler) handleDescribeFeatureGroup(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		FeatureGroupName string `json:"FeatureGroupName"`
	}

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

	return json.Marshal(resp)
}

type featureGroupSummary struct {
	FeatureGroupName   string  `json:"FeatureGroupName"`
	FeatureGroupArn    string  `json:"FeatureGroupArn"`
	FeatureGroupStatus string  `json:"FeatureGroupStatus"`
	CreationTime       float64 `json:"CreationTime"`
}

func (h *Handler) handleListFeatureGroups(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	fgs, nextToken := h.Backend.ListFeatureGroups(ctx, req.NextToken)
	summaries := make([]featureGroupSummary, 0, len(fgs))

	for _, fg := range fgs {
		summaries = append(summaries, featureGroupSummary{
			FeatureGroupName:   fg.FeatureGroupName,
			FeatureGroupArn:    fg.FeatureGroupArn,
			FeatureGroupStatus: fg.FeatureGroupStatus,
			CreationTime:       epochSeconds(fg.CreationTime),
		})
	}

	resp := map[string]any{"FeatureGroupSummaries": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleDeleteFeatureGroup(ctx context.Context, body []byte) error {
	var req struct {
		FeatureGroupName string `json:"FeatureGroupName"`
	}

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

func (h *Handler) handleUpdateFeatureGroup(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		FeatureGroupName   string              `json:"FeatureGroupName"`
		FeatureDefinitions []FeatureDefinition `json:"FeatureAdditions,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.FeatureGroupName == "" {
		return nil, fmt.Errorf("%w: FeatureGroupName is required", errInvalidRequest)
	}

	fg, err := h.Backend.UpdateFeatureGroup(ctx, req.FeatureGroupName, req.FeatureDefinitions)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: updated feature group", "name", fg.FeatureGroupName)

	return json.Marshal(map[string]string{keyFeatureGroupArn: fg.FeatureGroupArn})
}
