package sagemaker

import (
	"context"
	"encoding/json"
)

// ---------------------------------------------------------------------------
// ClusterSchedulerConfig handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateClusterSchedulerConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ClusterSchedulerConfigName string      `json:"Name"`
		ClusterArn                 string      `json:"ClusterArn"`
		Tags                       []tagObject `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	c, err := h.Backend.CreateClusterSchedulerConfig(ctx, CreateClusterSchedulerConfigOptions{
		ClusterSchedulerConfigName: req.ClusterSchedulerConfigName,
		ClusterArn:                 req.ClusterArn,
		Tags:                       fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyClusterSchedulerConfigArn: c.ClusterSchedulerConfigArn,
	})
}

func (h *Handler) handleDescribeClusterSchedulerConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ClusterSchedulerConfigName string `json:"ClusterSchedulerConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	c, err := h.Backend.DescribeClusterSchedulerConfig(ctx, req.ClusterSchedulerConfigName)
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
			"ClusterSchedulerConfigName": c.ClusterSchedulerConfigName,
			keyClusterSchedulerConfigArn: c.ClusterSchedulerConfigArn,
			keyStatus:                    c.Status,
			keyCreationTime:              epochSeconds(c.CreationTime),
			keyLastModifiedTime:          epochSeconds(c.LastModifiedTime),
		})
	}

	return listResp("ClusterSchedulerConfigSummaries", items, next)
}

func (h *Handler) handleUpdateClusterSchedulerConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ClusterSchedulerConfigName string `json:"ClusterSchedulerConfigName"`
		ClusterArn                 string `json:"ClusterArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateClusterSchedulerConfig(ctx, req.ClusterSchedulerConfigName, req.ClusterArn); err != nil {
		return nil, err
	}

	c, err := h.Backend.DescribeClusterSchedulerConfig(ctx, req.ClusterSchedulerConfigName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyClusterSchedulerConfigArn: c.ClusterSchedulerConfigArn,
	})
}

func (h *Handler) handleDeleteClusterSchedulerConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ClusterSchedulerConfigName string `json:"ClusterSchedulerConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteClusterSchedulerConfig(ctx, req.ClusterSchedulerConfigName); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

// ---------------------------------------------------------------------------
// ComputeQuota handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateComputeQuota(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ComputeQuotaName string      `json:"Name"`
		ClusterArn       string      `json:"ClusterArn"`
		Tags             []tagObject `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	q, err := h.Backend.CreateComputeQuota(ctx, CreateComputeQuotaOptions{
		ComputeQuotaName: req.ComputeQuotaName,
		ClusterArn:       req.ClusterArn,
		Tags:             fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyComputeQuotaArn: q.ComputeQuotaArn,
	})
}

func (h *Handler) handleDescribeComputeQuota(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ComputeQuotaName string `json:"ComputeQuotaName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	q, err := h.Backend.DescribeComputeQuota(ctx, req.ComputeQuotaName)
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
			"ComputeQuotaName":  q.ComputeQuotaName,
			keyComputeQuotaArn:  q.ComputeQuotaArn,
			keyStatus:           q.Status,
			keyCreationTime:     epochSeconds(q.CreationTime),
			keyLastModifiedTime: epochSeconds(q.LastModifiedTime),
		})
	}

	return listResp("ComputeQuotaSummaries", items, next)
}

func (h *Handler) handleUpdateComputeQuota(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ComputeQuotaName string `json:"ComputeQuotaName"`
		ClusterArn       string `json:"ClusterArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.UpdateComputeQuota(ctx, req.ComputeQuotaName, req.ClusterArn); err != nil {
		return nil, err
	}

	q, err := h.Backend.DescribeComputeQuota(ctx, req.ComputeQuotaName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyComputeQuotaArn: q.ComputeQuotaArn,
	})
}

func (h *Handler) handleDeleteComputeQuota(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ComputeQuotaName string `json:"ComputeQuotaName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteComputeQuota(ctx, req.ComputeQuotaName); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}
