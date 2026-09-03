package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// MonitoringAlert / MonitoringExecution handlers
// ---------------------------------------------------------------------------

// updateMonitoringAlertRequest is the request body for UpdateMonitoringAlert.
type updateMonitoringAlertRequest struct {
	MonitoringScheduleName string `json:"MonitoringScheduleName"`
	MonitoringAlertName    string `json:"MonitoringAlertName"`
	DatapointsToAlert      int32  `json:"DatapointsToAlert"`
	EvaluationPeriod       int32  `json:"EvaluationPeriod"`
}

func (h *Handler) handleUpdateMonitoringAlert(ctx context.Context, body []byte) ([]byte, error) {
	var req updateMonitoringAlertRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MonitoringScheduleName == "" {
		return nil, fmt.Errorf("%w: MonitoringScheduleName is required", errInvalidRequest)
	}

	if req.MonitoringAlertName == "" {
		return nil, fmt.Errorf("%w: MonitoringAlertName is required", errInvalidRequest)
	}

	if req.DatapointsToAlert == 0 {
		return nil, fmt.Errorf("%w: DatapointsToAlert is required", errInvalidRequest)
	}

	if req.EvaluationPeriod == 0 {
		return nil, fmt.Errorf("%w: EvaluationPeriod is required", errInvalidRequest)
	}

	alert, scheduleArn, err := h.Backend.UpdateMonitoringAlert(
		ctx, req.MonitoringScheduleName, req.MonitoringAlertName, req.DatapointsToAlert, req.EvaluationPeriod,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyMonitoringScheduleArn: scheduleArn,
		keyMonitoringAlertName:   alert.MonitoringAlertName,
	})
}

// listMonitoringAlertsRequest is the request body for ListMonitoringAlerts.
type listMonitoringAlertsRequest struct {
	MonitoringScheduleName string `json:"MonitoringScheduleName"`
	NextToken              string `json:"NextToken"`
	MaxResults             int32  `json:"MaxResults,omitempty"`
}

func (h *Handler) handleListMonitoringAlerts(ctx context.Context, body []byte) ([]byte, error) {
	var req listMonitoringAlertsRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MonitoringScheduleName == "" {
		return nil, fmt.Errorf("%w: MonitoringScheduleName is required", errInvalidRequest)
	}

	items, next, err := h.Backend.ListMonitoringAlerts(ctx, req.MonitoringScheduleName, req.NextToken, req.MaxResults)
	if err != nil {
		return nil, err
	}

	summaries := make([]map[string]any, 0, len(items))
	for _, a := range items {
		summaries = append(summaries, map[string]any{
			keyMonitoringAlertName: a.MonitoringAlertName,
			"AlertStatus":          a.AlertStatus,
			"DatapointsToAlert":    a.DatapointsToAlert,
			"EvaluationPeriod":     a.EvaluationPeriod,
			keyCreationTime:        epochSeconds(a.CreationTime),
			keyLastModifiedTime:    epochSeconds(a.LastModifiedTime),
			"Actions": map[string]any{
				"ModelDashboardIndicator": map[string]any{"Enabled": a.DashboardIndicatorEnabled},
			},
		})
	}

	resp := map[string]any{"MonitoringAlertSummaries": summaries}
	if next != "" {
		resp[keyNextToken] = next
	}

	return json.Marshal(resp)
}

// listMonitoringAlertHistoryRequest is the request body for ListMonitoringAlertHistory.
type listMonitoringAlertHistoryRequest struct {
	CreationTimeAfter      *float64 `json:"CreationTimeAfter,omitempty"`
	CreationTimeBefore     *float64 `json:"CreationTimeBefore,omitempty"`
	MonitoringScheduleName string   `json:"MonitoringScheduleName,omitempty"`
	MonitoringAlertName    string   `json:"MonitoringAlertName,omitempty"`
	StatusEquals           string   `json:"StatusEquals,omitempty"`
	SortBy                 string   `json:"SortBy,omitempty"`
	SortOrder              string   `json:"SortOrder,omitempty"`
	NextToken              string   `json:"NextToken,omitempty"`
	MaxResults             int32    `json:"MaxResults,omitempty"`
}

func (h *Handler) handleListMonitoringAlertHistory(ctx context.Context, body []byte) ([]byte, error) {
	var req listMonitoringAlertHistoryRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	f := MonitoringAlertHistoryFilter{
		CreationTimeAfter:      epochPtr(req.CreationTimeAfter),
		CreationTimeBefore:     epochPtr(req.CreationTimeBefore),
		MonitoringScheduleName: req.MonitoringScheduleName,
		MonitoringAlertName:    req.MonitoringAlertName,
		StatusEquals:           req.StatusEquals,
		SortBy:                 req.SortBy,
		SortOrder:              req.SortOrder,
		MaxResults:             req.MaxResults,
	}

	items, next := h.Backend.ListMonitoringAlertHistory(ctx, req.NextToken, f)

	summaries := make([]map[string]any, 0, len(items))
	for _, e := range items {
		summaries = append(summaries, map[string]any{
			keyMonitoringScheduleName: e.MonitoringScheduleName,
			keyMonitoringAlertName:    e.MonitoringAlertName,
			"AlertStatus":             e.AlertStatus,
			keyCreationTime:           epochSeconds(e.CreationTime),
		})
	}

	resp := map[string]any{"MonitoringAlertHistory": summaries}
	if next != "" {
		resp[keyNextToken] = next
	}

	return json.Marshal(resp)
}

// listMonitoringExecutionsRequest is the parsed representation of a
// ListMonitoringExecutions request body.
type listMonitoringExecutionsRequest struct {
	CreationTimeAfter           *float64 `json:"CreationTimeAfter,omitempty"`
	CreationTimeBefore          *float64 `json:"CreationTimeBefore,omitempty"`
	LastModifiedTimeAfter       *float64 `json:"LastModifiedTimeAfter,omitempty"`
	LastModifiedTimeBefore      *float64 `json:"LastModifiedTimeBefore,omitempty"`
	ScheduledTimeAfter          *float64 `json:"ScheduledTimeAfter,omitempty"`
	ScheduledTimeBefore         *float64 `json:"ScheduledTimeBefore,omitempty"`
	EndpointName                string   `json:"EndpointName,omitempty"`
	MonitoringJobDefinitionName string   `json:"MonitoringJobDefinitionName,omitempty"`
	MonitoringScheduleName      string   `json:"MonitoringScheduleName,omitempty"`
	MonitoringTypeEquals        string   `json:"MonitoringTypeEquals,omitempty"`
	StatusEquals                string   `json:"StatusEquals,omitempty"`
	SortBy                      string   `json:"SortBy,omitempty"`
	SortOrder                   string   `json:"SortOrder,omitempty"`
	NextToken                   string   `json:"NextToken,omitempty"`
	MaxResults                  int32    `json:"MaxResults,omitempty"`
}

func (r listMonitoringExecutionsRequest) toFilter() MonitoringExecutionFilter {
	return MonitoringExecutionFilter{
		CreationTimeAfter:           epochPtr(r.CreationTimeAfter),
		CreationTimeBefore:          epochPtr(r.CreationTimeBefore),
		LastModifiedTimeAfter:       epochPtr(r.LastModifiedTimeAfter),
		LastModifiedTimeBefore:      epochPtr(r.LastModifiedTimeBefore),
		ScheduledTimeAfter:          epochPtr(r.ScheduledTimeAfter),
		ScheduledTimeBefore:         epochPtr(r.ScheduledTimeBefore),
		EndpointName:                r.EndpointName,
		MonitoringJobDefinitionName: r.MonitoringJobDefinitionName,
		MonitoringScheduleName:      r.MonitoringScheduleName,
		MonitoringTypeEquals:        r.MonitoringTypeEquals,
		StatusEquals:                r.StatusEquals,
		SortBy:                      r.SortBy,
		SortOrder:                   r.SortOrder,
		MaxResults:                  r.MaxResults,
	}
}

func (h *Handler) handleListMonitoringExecutions(ctx context.Context, body []byte) ([]byte, error) {
	var req listMonitoringExecutionsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListMonitoringExecutions(ctx, req.NextToken, req.toFilter())

	return json.Marshal(buildMonitoringExecutionListResponse(items, next))
}

func buildMonitoringExecutionListResponse(items []*MonitoringExecution, next string) map[string]any {
	summaries := make([]map[string]any, 0, len(items))
	for _, e := range items {
		summaries = append(summaries, buildMonitoringExecutionSummary(e))
	}

	resp := map[string]any{"MonitoringExecutionSummaries": summaries}
	if next != "" {
		resp[keyNextToken] = next
	}

	return resp
}

func buildMonitoringExecutionSummary(e *MonitoringExecution) map[string]any {
	s := map[string]any{
		keyMonitoringScheduleName:   e.MonitoringScheduleName,
		"MonitoringExecutionStatus": e.MonitoringExecutionStatus,
		keyCreationTime:             epochSeconds(e.CreationTime),
		keyLastModifiedTime:         epochSeconds(e.LastModifiedTime),
		"ScheduledTime":             epochSeconds(e.ScheduledTime),
	}

	if e.EndpointName != "" {
		s[keyEndpointNameField] = e.EndpointName
	}

	if e.MonitoringJobDefinitionName != "" {
		s["MonitoringJobDefinitionName"] = e.MonitoringJobDefinitionName
	}

	if e.MonitoringType != "" {
		s["MonitoringType"] = e.MonitoringType
	}

	if e.ProcessingJobArn != "" {
		s["ProcessingJobArn"] = e.ProcessingJobArn
	}

	if e.FailureReason != "" {
		s["FailureReason"] = e.FailureReason
	}

	return s
}
