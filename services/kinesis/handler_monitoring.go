package kinesis

import (
	"context"
	"encoding/json"
	"net/http"
)

type jsonEnhancedMonitoringReq struct {
	StreamName        string   `json:"StreamName"`
	ShardLevelMetrics []string `json:"ShardLevelMetrics"`
}

type jsonEnhancedMonitoringResp struct {
	StreamName               string   `json:"StreamName"`
	CurrentShardLevelMetrics []string `json:"CurrentShardLevelMetrics"`
	DesiredShardLevelMetrics []string `json:"DesiredShardLevelMetrics"`
}

func (h *Handler) handleEnableEnhancedMonitoring(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonEnhancedMonitoringReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	out, err := h.Backend.EnableEnhancedMonitoring(ctx, &EnableEnhancedMonitoringInput{
		StreamName:        req.StreamName,
		ShardLevelMetrics: req.ShardLevelMetrics,
	})
	if err != nil {
		return nil, err
	}

	return jsonEnhancedMonitoringResp{
		StreamName:               out.StreamName,
		CurrentShardLevelMetrics: out.CurrentShardLevelMetrics,
		DesiredShardLevelMetrics: out.DesiredShardLevelMetrics,
	}, nil
}

func (h *Handler) handleDisableEnhancedMonitoring(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonEnhancedMonitoringReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	out, err := h.Backend.DisableEnhancedMonitoring(ctx, &DisableEnhancedMonitoringInput{
		StreamName:        req.StreamName,
		ShardLevelMetrics: req.ShardLevelMetrics,
	})
	if err != nil {
		return nil, err
	}

	return jsonEnhancedMonitoringResp{
		StreamName:               out.StreamName,
		CurrentShardLevelMetrics: out.CurrentShardLevelMetrics,
		DesiredShardLevelMetrics: out.DesiredShardLevelMetrics,
	}, nil
}
