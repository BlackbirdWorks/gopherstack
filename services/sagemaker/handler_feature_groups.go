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
		FeatureDefinitions          []FeatureDefinition `json:"FeatureDefinitions"`
		Tags                        []tagObject         `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.FeatureGroupName == "" {
		return nil, fmt.Errorf("%w: FeatureGroupName is required", errInvalidRequest)
	}

	fg, err := h.Backend.CreateFeatureGroup(
		ctx,
		req.FeatureGroupName,
		req.RecordIdentifierFeatureName,
		req.EventTimeFeatureName,
		req.FeatureDefinitions,
		fromTagObjects(req.Tags),
	)
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

	return json.Marshal(map[string]any{
		keyFeatureGroupName:            fg.FeatureGroupName,
		keyFeatureGroupArn:             fg.FeatureGroupArn,
		keyFeatureGroupStatus:          fg.FeatureGroupStatus,
		keyRecordIdentifierFeatureName: fg.RecordIdentifierFeatureName,
		keyEventTimeFeatureName:        fg.EventTimeFeatureName,
		keyFeatureDefinitions:          fg.FeatureDefinitions,
		keyCreationTime:                epochSeconds(fg.CreationTime),
	})
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
