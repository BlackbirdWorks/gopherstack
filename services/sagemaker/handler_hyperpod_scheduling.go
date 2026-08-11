package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// ClusterSchedulerConfig handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateClusterSchedulerConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		SchedulerConfig            *SchedulerConfig `json:"SchedulerConfig"`
		ClusterSchedulerConfigName string           `json:"Name"`
		ClusterArn                 string           `json:"ClusterArn"`
		Description                string           `json:"Description"`
		Tags                       []tagObject      `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	c, err := h.Backend.CreateClusterSchedulerConfig(ctx, CreateClusterSchedulerConfigOptions{
		ClusterSchedulerConfigName: req.ClusterSchedulerConfigName,
		ClusterArn:                 req.ClusterArn,
		Description:                req.Description,
		SchedulerConfig:            req.SchedulerConfig,
		Tags:                       fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyClusterSchedulerConfigArn: c.ClusterSchedulerConfigArn,
		keyClusterSchedulerConfigID:  c.ClusterSchedulerConfigID,
	})
}

func (h *Handler) handleDescribeClusterSchedulerConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ClusterSchedulerConfigID string `json:"ClusterSchedulerConfigId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	c, err := h.Backend.DescribeClusterSchedulerConfig(ctx, req.ClusterSchedulerConfigID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(c)
}

func (h *Handler) handleListClusterSchedulerConfigs(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	configs, next := h.Backend.ListClusterSchedulerConfigs(ctx, req.NextToken)

	items := make([]map[string]any, 0, len(configs))
	for _, c := range configs {
		items = append(items, map[string]any{
			keyGenericName:                   c.ClusterSchedulerConfigName,
			keyClusterSchedulerConfigArn:     c.ClusterSchedulerConfigArn,
			keyClusterSchedulerConfigID:      c.ClusterSchedulerConfigID,
			keyClusterSchedulerConfigVersion: c.ClusterSchedulerConfigVersion,
			keyStatus:                        c.Status,
			keyCreationTime:                  epochSeconds(c.CreationTime),
			keyLastModifiedTime:              epochSeconds(c.LastModifiedTime),
		})
	}

	return listResp("ClusterSchedulerConfigSummaries", items, next)
}

func (h *Handler) handleUpdateClusterSchedulerConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		SchedulerConfig          *SchedulerConfig `json:"SchedulerConfig"`
		Description              *string          `json:"Description"`
		ClusterSchedulerConfigID string           `json:"ClusterSchedulerConfigId"`
		TargetVersion            int32            `json:"TargetVersion"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if req.TargetVersion == 0 {
		return nil, fmt.Errorf("%w: TargetVersion is required", ErrValidation)
	}

	c, err := h.Backend.UpdateClusterSchedulerConfig(ctx, req.ClusterSchedulerConfigID, req.TargetVersion,
		UpdateClusterSchedulerConfigOptions{
			SchedulerConfig: req.SchedulerConfig,
			Description:     req.Description,
		})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyClusterSchedulerConfigArn:     c.ClusterSchedulerConfigArn,
		keyClusterSchedulerConfigVersion: c.ClusterSchedulerConfigVersion,
	})
}

func (h *Handler) handleDeleteClusterSchedulerConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ClusterSchedulerConfigID string `json:"ClusterSchedulerConfigId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteClusterSchedulerConfig(ctx, req.ClusterSchedulerConfigID); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

// ---------------------------------------------------------------------------
// ComputeQuota handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateComputeQuota(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ComputeQuotaConfig *ComputeQuotaConfig `json:"ComputeQuotaConfig"`
		ComputeQuotaTarget *ComputeQuotaTarget `json:"ComputeQuotaTarget"`
		ComputeQuotaName   string              `json:"Name"`
		ClusterArn         string              `json:"ClusterArn"`
		ActivationState    string              `json:"ActivationState"`
		Description        string              `json:"Description"`
		Tags               []tagObject         `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	q, err := h.Backend.CreateComputeQuota(ctx, CreateComputeQuotaOptions{
		ComputeQuotaName:   req.ComputeQuotaName,
		ClusterArn:         req.ClusterArn,
		ActivationState:    req.ActivationState,
		Description:        req.Description,
		ComputeQuotaConfig: req.ComputeQuotaConfig,
		ComputeQuotaTarget: req.ComputeQuotaTarget,
		Tags:               fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyComputeQuotaArn: q.ComputeQuotaArn,
		keyComputeQuotaID:  q.ComputeQuotaID,
	})
}

func (h *Handler) handleDescribeComputeQuota(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ComputeQuotaID string `json:"ComputeQuotaId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	q, err := h.Backend.DescribeComputeQuota(ctx, req.ComputeQuotaID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(q)
}

func (h *Handler) handleListComputeQuotas(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	quotas, next := h.Backend.ListComputeQuotas(ctx, req.NextToken)

	items := make([]map[string]any, 0, len(quotas))
	for _, q := range quotas {
		items = append(items, map[string]any{
			keyGenericName:         q.ComputeQuotaName,
			keyComputeQuotaArn:     q.ComputeQuotaArn,
			keyComputeQuotaID:      q.ComputeQuotaID,
			keyComputeQuotaVersion: q.ComputeQuotaVersion,
			keyComputeQuotaConfig:  q.ComputeQuotaConfig,
			keyComputeQuotaTarget:  q.ComputeQuotaTarget,
			keyActivationState:     q.ActivationState,
			keyClusterArn:          q.ClusterArn,
			keyStatus:              q.Status,
			keyCreationTime:        epochSeconds(q.CreationTime),
			keyLastModifiedTime:    epochSeconds(q.LastModifiedTime),
		})
	}

	return listResp("ComputeQuotaSummaries", items, next)
}

func (h *Handler) handleUpdateComputeQuota(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ComputeQuotaConfig *ComputeQuotaConfig `json:"ComputeQuotaConfig"`
		ComputeQuotaTarget *ComputeQuotaTarget `json:"ComputeQuotaTarget"`
		ActivationState    *string             `json:"ActivationState"`
		Description        *string             `json:"Description"`
		ComputeQuotaID     string              `json:"ComputeQuotaId"`
		TargetVersion      int32               `json:"TargetVersion"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if req.TargetVersion == 0 {
		return nil, fmt.Errorf("%w: TargetVersion is required", ErrValidation)
	}

	q, err := h.Backend.UpdateComputeQuota(ctx, req.ComputeQuotaID, req.TargetVersion, UpdateComputeQuotaOptions{
		ComputeQuotaConfig: req.ComputeQuotaConfig,
		ComputeQuotaTarget: req.ComputeQuotaTarget,
		ActivationState:    req.ActivationState,
		Description:        req.Description,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyComputeQuotaArn:     q.ComputeQuotaArn,
		keyComputeQuotaVersion: q.ComputeQuotaVersion,
	})
}

func (h *Handler) handleDeleteComputeQuota(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ComputeQuotaID string `json:"ComputeQuotaId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteComputeQuota(ctx, req.ComputeQuotaID); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}
