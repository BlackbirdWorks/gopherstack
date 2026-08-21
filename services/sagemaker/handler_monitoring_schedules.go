package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// MonitoringSchedule handlers
// ---------------------------------------------------------------------------

// createMonitoringScheduleInput mirrors CreateMonitoringScheduleInput
// (api_op_CreateMonitoringSchedule.go:29-50): MonitoringScheduleConfig and
// MonitoringScheduleName are both required; Tags is optional.
type createMonitoringScheduleInput struct {
	MonitoringScheduleName   string                    `json:"MonitoringScheduleName"`
	MonitoringScheduleConfig *MonitoringScheduleConfig `json:"MonitoringScheduleConfig"`
	Tags                     []tagObject               `json:"Tags"`
}

func (h *Handler) handleCreateMonitoringSchedule(ctx context.Context, body []byte) ([]byte, error) {
	var req createMonitoringScheduleInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MonitoringScheduleName == "" {
		return nil, fmt.Errorf("%w: MonitoringScheduleName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateMonitoringSchedule(
		ctx, req.MonitoringScheduleName, req.MonitoringScheduleConfig, fromTagObjects(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyMonitoringScheduleArn: result.MonitoringScheduleArn})
}

// describeMonitoringScheduleInput mirrors DescribeMonitoringScheduleInput
// (api_op_DescribeMonitoringSchedule.go:27-33): its sole member is required.
type describeMonitoringScheduleInput struct {
	MonitoringScheduleName string `json:"MonitoringScheduleName"`
}

func (h *Handler) handleDescribeMonitoringSchedule(ctx context.Context, body []byte) ([]byte, error) {
	var req describeMonitoringScheduleInput

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

// deleteMonitoringScheduleInput mirrors DeleteMonitoringScheduleInput
// (api_op_DeleteMonitoringSchedule.go:27-33): its sole member is required.
type deleteMonitoringScheduleInput struct {
	MonitoringScheduleName string `json:"MonitoringScheduleName"`
}

func (h *Handler) handleDeleteMonitoringSchedule(ctx context.Context, body []byte) error {
	var req deleteMonitoringScheduleInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MonitoringScheduleName == "" {
		return fmt.Errorf("%w: MonitoringScheduleName is required", errInvalidRequest)
	}

	return h.Backend.DeleteMonitoringSchedule(ctx, req.MonitoringScheduleName)
}

// stopMonitoringScheduleInput mirrors StopMonitoringScheduleInput
// (api_op_StopMonitoringSchedule.go:27-33): its sole member is required.
type stopMonitoringScheduleInput struct {
	MonitoringScheduleName string `json:"MonitoringScheduleName"`
}

func (h *Handler) handleStopMonitoringSchedule(ctx context.Context, body []byte) error {
	var req stopMonitoringScheduleInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MonitoringScheduleName == "" {
		return fmt.Errorf("%w: MonitoringScheduleName is required", errInvalidRequest)
	}

	return h.Backend.StopMonitoringSchedule(ctx, req.MonitoringScheduleName)
}

// startMonitoringScheduleInput mirrors StartMonitoringScheduleInput
// (api_op_StartMonitoringSchedule.go:27-33): its sole member is required.
type startMonitoringScheduleInput struct {
	MonitoringScheduleName string `json:"MonitoringScheduleName"`
}

func (h *Handler) handleStartMonitoringSchedule(ctx context.Context, body []byte) error {
	var req startMonitoringScheduleInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MonitoringScheduleName == "" {
		return fmt.Errorf("%w: MonitoringScheduleName is required", errInvalidRequest)
	}

	return h.Backend.StartMonitoringSchedule(ctx, req.MonitoringScheduleName)
}

// updateMonitoringScheduleInput mirrors UpdateMonitoringScheduleInput
// (api_op_UpdateMonitoringSchedule.go:28-45): both members are required.
type updateMonitoringScheduleInput struct {
	MonitoringScheduleConfig *MonitoringScheduleConfig `json:"MonitoringScheduleConfig"`
	MonitoringScheduleName   string                    `json:"MonitoringScheduleName"`
}

func (h *Handler) handleUpdateMonitoringSchedule(ctx context.Context, body []byte) ([]byte, error) {
	var req updateMonitoringScheduleInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MonitoringScheduleName == "" {
		return nil, fmt.Errorf("%w: MonitoringScheduleName is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateMonitoringSchedule(ctx, req.MonitoringScheduleName, req.MonitoringScheduleConfig)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyMonitoringScheduleArn: result.MonitoringScheduleArn})
}

// listMonitoringSchedulesInput mirrors ListMonitoringSchedulesInput
// (api_op_ListMonitoringSchedules.go:29-72): every member is optional. The
// four time filters are awsjson1.1 epoch-second numbers on the wire
// (confirmed by this campaign's repo-spanning time-decode bug, parity-16)
// — decoded as *float64, never *time.Time.
type listMonitoringSchedulesInput struct {
	CreationTimeAfter           *float64 `json:"CreationTimeAfter"`
	CreationTimeBefore          *float64 `json:"CreationTimeBefore"`
	LastModifiedTimeAfter       *float64 `json:"LastModifiedTimeAfter"`
	LastModifiedTimeBefore      *float64 `json:"LastModifiedTimeBefore"`
	EndpointName                string   `json:"EndpointName"`
	MonitoringJobDefinitionName string   `json:"MonitoringJobDefinitionName"`
	MonitoringTypeEquals        string   `json:"MonitoringTypeEquals"`
	NameContains                string   `json:"NameContains"`
	NextToken                   string   `json:"NextToken"`
	SortBy                      string   `json:"SortBy"`
	SortOrder                   string   `json:"SortOrder"`
	StatusEquals                string   `json:"StatusEquals"`
	MaxResults                  int32    `json:"MaxResults"`
}

func (h *Handler) handleListMonitoringSchedules(ctx context.Context, body []byte) ([]byte, error) {
	var req listMonitoringSchedulesInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListMonitoringSchedules(ctx, ListMonitoringSchedulesParams{
		CreationTimeAfter:           epochPtr(req.CreationTimeAfter),
		CreationTimeBefore:          epochPtr(req.CreationTimeBefore),
		LastModifiedTimeAfter:       epochPtr(req.LastModifiedTimeAfter),
		LastModifiedTimeBefore:      epochPtr(req.LastModifiedTimeBefore),
		EndpointName:                req.EndpointName,
		MonitoringJobDefinitionName: req.MonitoringJobDefinitionName,
		MonitoringTypeEquals:        req.MonitoringTypeEquals,
		NameContains:                req.NameContains,
		SortBy:                      req.SortBy,
		SortOrder:                   req.SortOrder,
		StatusEquals:                req.StatusEquals,
		NextToken:                   req.NextToken,
		MaxResults:                  req.MaxResults,
	})

	summaries := make([]map[string]any, 0, len(items))
	for _, ms := range items {
		summary := map[string]any{
			"MonitoringScheduleName":   ms.MonitoringScheduleName,
			keyMonitoringScheduleArn:   ms.MonitoringScheduleArn,
			"MonitoringScheduleStatus": ms.MonitoringScheduleStatus,
			keyCreationTime:            epochSeconds(ms.CreationTime),
			keyLastModifiedTime:        epochSeconds(ms.LastModifiedTime),
		}

		if ms.EndpointName != "" {
			summary["EndpointName"] = ms.EndpointName
		}

		if ms.MonitoringScheduleConfig != nil {
			if ms.MonitoringScheduleConfig.MonitoringJobDefinitionName != "" {
				summary["MonitoringJobDefinitionName"] = ms.MonitoringScheduleConfig.MonitoringJobDefinitionName
			}

			if ms.MonitoringScheduleConfig.MonitoringType != "" {
				summary["MonitoringType"] = ms.MonitoringScheduleConfig.MonitoringType
			}
		}

		summaries = append(summaries, summary)
	}

	return json.Marshal(map[string]any{
		"MonitoringScheduleSummaries": summaries,
		keyNextToken:                  next,
	})
}
