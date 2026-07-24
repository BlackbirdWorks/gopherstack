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

// updateAccountSettingsRequest is the parsed request body for
// UpdateAccountSettings. Nested wire-shape types mirror
// types.QueryComputeRequest / types.ProvisionedCapacityRequest /
// types.AccountSettingsNotificationConfiguration / types.SnsConfiguration.
type updateAccountSettingsRequest struct {
	MaxQueryTCU       *int32                   `json:"MaxQueryTCU"`
	QueryCompute      *queryComputeRequestWire `json:"QueryCompute"`
	QueryPricingModel string                   `json:"QueryPricingModel"`
}

type queryComputeRequestWire struct {
	ProvisionedCapacity *provisionedCapacityRequestWire `json:"ProvisionedCapacity"`
	ComputeMode         string                          `json:"ComputeMode"`
}

type provisionedCapacityRequestWire struct {
	NotificationConfiguration *accountSettingsNotificationConfigWire `json:"NotificationConfiguration"`
	TargetQueryTCU            *int32                                 `json:"TargetQueryTCU"`
}

type accountSettingsNotificationConfigWire struct {
	SnsConfiguration *snsConfigurationWire `json:"SnsConfiguration"`
	RoleArn          string                `json:"RoleArn"`
}

type snsConfigurationWire struct {
	TopicArn string `json:"TopicArn"`
}

// toQueryComputeUpdate converts the parsed wire request into the backend's
// QueryComputeUpdate shape, or nil if QueryCompute was omitted entirely.
func (req updateAccountSettingsRequest) toQueryComputeUpdate() *QueryComputeUpdate {
	if req.QueryCompute == nil {
		return nil
	}

	update := &QueryComputeUpdate{ComputeMode: req.QueryCompute.ComputeMode}

	pc := req.QueryCompute.ProvisionedCapacity
	if pc == nil {
		return update
	}

	update.TargetQueryTCU = pc.TargetQueryTCU
	update.NotificationConfiguration = pc.NotificationConfiguration.toModel()

	return update
}

// toModel converts the parsed wire NotificationConfiguration into the
// backend's typed AccountSettingsNotificationConfiguration, or nil if absent.
func (nc *accountSettingsNotificationConfigWire) toModel() *AccountSettingsNotificationConfiguration {
	if nc == nil {
		return nil
	}

	cfg := &AccountSettingsNotificationConfiguration{RoleArn: nc.RoleArn}
	if nc.SnsConfiguration != nil {
		cfg.SnsConfiguration = &SnsConfiguration{TopicArn: nc.SnsConfiguration.TopicArn}
	}

	return cfg
}

func (h *Handler) handleUpdateAccountSettings(ctx context.Context, body []byte) ([]byte, error) {
	var req updateAccountSettingsRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	settings, err := h.Backend.UpdateAccountSettings(
		ctx, req.QueryPricingModel, req.MaxQueryTCU, req.toQueryComputeUpdate(),
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(buildAccountSettingsResponse(settings))
}
