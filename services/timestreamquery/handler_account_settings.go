package timestreamquery

import (
	"context"
	"encoding/json"
	"fmt"
)

func (h *Handler) handleDescribeAccountSettings(ctx context.Context) ([]byte, error) {
	settings := h.Backend.DescribeAccountSettings(ctx)

	return json.Marshal(buildAccountSettingsResponse(settings))
}

func buildAccountSettingsResponse(settings AccountSettings) map[string]any {
	resp := map[string]any{
		"QueryPricingModel": settings.QueryPricingModel,
	}
	if settings.MaxQueryTCU != nil {
		resp["MaxQueryTCU"] = *settings.MaxQueryTCU
	}
	if settings.LastUpdatedTime != nil {
		resp["LastUpdatedTime"] = settings.LastUpdatedTime.Unix()
	}
	if settings.QueryCompute != nil {
		resp["QueryCompute"] = settings.QueryCompute
	}

	return resp
}

func (h *Handler) handlePrepareQuery(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		QueryString  string `json:"QueryString"`
		ValidateOnly bool   `json:"ValidateOnly"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	if req.QueryString == "" {
		return nil, fmt.Errorf("%w: QueryString is required", ErrValidation)
	}

	result, err := h.Backend.PrepareQuery(ctx, req.QueryString, req.ValidateOnly)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"QueryString": result.QueryString,
		"Columns":     marshalColumnInfos(result.Columns),
		"Parameters":  marshalColumnInfos(result.Parameters),
	})
}

func (h *Handler) handleUpdateAccountSettings(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		MaxQueryTCU  *int32 `json:"MaxQueryTCU"`
		QueryCompute *struct {
			ProvisionedCapacity *struct {
				TargetQueryTCU *int32 `json:"TargetQueryTCU"`
			} `json:"ProvisionedCapacity"`
			ComputeMode string `json:"ComputeMode"`
		} `json:"QueryCompute"`
		QueryPricingModel string `json:"QueryPricingModel"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	var queryCompute *QueryComputeUpdate
	if req.QueryCompute != nil {
		queryCompute = &QueryComputeUpdate{ComputeMode: req.QueryCompute.ComputeMode}
		if req.QueryCompute.ProvisionedCapacity != nil {
			queryCompute.TargetQueryTCU = req.QueryCompute.ProvisionedCapacity.TargetQueryTCU
		}
	}

	settings, err := h.Backend.UpdateAccountSettings(ctx, req.QueryPricingModel, req.MaxQueryTCU, queryCompute)
	if err != nil {
		return nil, err
	}

	return json.Marshal(buildAccountSettingsResponse(settings))
}
