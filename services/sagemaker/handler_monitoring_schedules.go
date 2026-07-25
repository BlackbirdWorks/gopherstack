package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// MonitoringSchedule handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateMonitoringSchedule(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags                   map[string]string `json:"Tags"`
		MonitoringScheduleName string            `json:"MonitoringScheduleName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MonitoringScheduleName == "" {
		return nil, fmt.Errorf("%w: MonitoringScheduleName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateMonitoringSchedule(ctx, req.MonitoringScheduleName, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyMonitoringScheduleArn: result.MonitoringScheduleArn})
}

func (h *Handler) handleDescribeMonitoringSchedule(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		MonitoringScheduleName string `json:"MonitoringScheduleName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MonitoringScheduleName == "" {
		return nil, fmt.Errorf("%w: MonitoringScheduleName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeMonitoringSchedule(ctx, req.MonitoringScheduleName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteMonitoringSchedule(ctx context.Context, body []byte) error {
	var req struct {
		MonitoringScheduleName string `json:"MonitoringScheduleName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MonitoringScheduleName == "" {
		return fmt.Errorf("%w: MonitoringScheduleName is required", errInvalidRequest)
	}

	return h.Backend.DeleteMonitoringSchedule(ctx, req.MonitoringScheduleName)
}

func (h *Handler) handleStopMonitoringSchedule(ctx context.Context, body []byte) error {
	var req struct {
		MonitoringScheduleName string `json:"MonitoringScheduleName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MonitoringScheduleName == "" {
		return fmt.Errorf("%w: MonitoringScheduleName is required", errInvalidRequest)
	}

	return h.Backend.StopMonitoringSchedule(ctx, req.MonitoringScheduleName)
}

func (h *Handler) handleStartMonitoringSchedule(ctx context.Context, body []byte) error {
	var req struct {
		MonitoringScheduleName string `json:"MonitoringScheduleName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MonitoringScheduleName == "" {
		return fmt.Errorf("%w: MonitoringScheduleName is required", errInvalidRequest)
	}

	return h.Backend.StartMonitoringSchedule(ctx, req.MonitoringScheduleName)
}

func (h *Handler) handleUpdateMonitoringSchedule(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		MonitoringScheduleName string `json:"MonitoringScheduleName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MonitoringScheduleName == "" {
		return nil, fmt.Errorf("%w: MonitoringScheduleName is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateMonitoringSchedule(ctx, req.MonitoringScheduleName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyMonitoringScheduleArn: result.MonitoringScheduleArn})
}

func (h *Handler) handleListMonitoringSchedules(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListMonitoringSchedules(ctx, req.NextToken)

	summaries := make([]map[string]any, 0, len(items))
	for _, ms := range items {
		summaries = append(summaries, map[string]any{
			"MonitoringScheduleName":   ms.MonitoringScheduleName,
			keyMonitoringScheduleArn:   ms.MonitoringScheduleArn,
			"MonitoringScheduleStatus": ms.MonitoringScheduleStatus,
			keyCreationTime:            epochSeconds(ms.CreationTime),
			keyLastModifiedTime:        epochSeconds(ms.LastModifiedTime),
		})
	}

	return json.Marshal(map[string]any{
		"MonitoringScheduleSummaries": summaries,
		keyNextToken:                  next,
	})
}
