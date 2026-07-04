package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// trainingPlanExtOpsSupported returns the real stateful operations implemented
// in this file (Training Plan / Reserved Capacity family).
func trainingPlanExtOpsSupported() []string {
	return []string{
		"ListTrainingPlans",
		"SearchTrainingPlanOfferings",
		"ExtendTrainingPlan",
		"DescribeTrainingPlanExtensionHistory",
		"DescribeReservedCapacity",
		"ListUltraServersByReservedCapacity",
	}
}

// dispatchTrainingPlanExtOps dispatches the Training Plan / Reserved Capacity
// family of real stateful operations.
func (h *Handler) dispatchTrainingPlanExtOps(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	case "ListTrainingPlans":
		r, err := h.handleListTrainingPlans(ctx, body)

		return r, true, err
	case "SearchTrainingPlanOfferings":
		r, err := h.handleSearchTrainingPlanOfferings(ctx, body)

		return r, true, err
	case "ExtendTrainingPlan":
		r, err := h.handleExtendTrainingPlan(ctx, body)

		return r, true, err
	case "DescribeTrainingPlanExtensionHistory":
		r, err := h.handleDescribeTrainingPlanExtensionHistory(ctx, body)

		return r, true, err
	case "DescribeReservedCapacity":
		r, err := h.handleDescribeReservedCapacity(ctx, body)

		return r, true, err
	case "ListUltraServersByReservedCapacity":
		r, err := h.handleListUltraServersByReservedCapacity(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func trainingPlanSummaryJSON(t *TrainingPlan) map[string]any {
	summary := map[string]any{
		"TrainingPlanName": t.TrainingPlanName,
		keyTrainingPlanArn: t.TrainingPlanArn,
		keyStatus:          t.Status,
	}

	if t.DurationHours > 0 {
		summary["DurationHours"] = t.DurationHours
	}

	if t.DurationMinutes > 0 {
		summary["DurationMinutes"] = t.DurationMinutes
	}

	if t.CurrencyCode != "" {
		summary["CurrencyCode"] = t.CurrencyCode
	}

	if t.StartTime != nil {
		summary[trainingPlanSortByStartTime] = t.StartTime
	}

	if t.EndTime != nil {
		summary["EndTime"] = t.EndTime
	}

	if len(t.ReservedCapacitySummaries) > 0 {
		summary["ReservedCapacitySummaries"] = t.ReservedCapacitySummaries
	}

	if t.AvailableInstanceCount > 0 {
		summary["AvailableInstanceCount"] = t.AvailableInstanceCount
	}

	if t.InUseInstanceCount > 0 {
		summary["InUseInstanceCount"] = t.InUseInstanceCount
	}

	return summary
}

func (h *Handler) handleListTrainingPlans(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		SortBy     string                  `json:"SortBy"`
		SortOrder  string                  `json:"SortOrder"`
		NextToken  string                  `json:"NextToken"`
		Filters    []trainingPlanFilterReq `json:"Filters"`
		MaxResults int32                   `json:"MaxResults"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	var statusEquals string

	for _, f := range req.Filters {
		if f.Name == keyStatus {
			statusEquals = f.Value
		}
	}

	plans, next := h.Backend.ListTrainingPlans(ctx, ListTrainingPlansParams{
		StatusEquals: statusEquals,
		SortBy:       req.SortBy,
		SortOrder:    req.SortOrder,
		NextToken:    req.NextToken,
		MaxResults:   req.MaxResults,
	})

	summaries := make([]map[string]any, 0, len(plans))
	for _, t := range plans {
		summaries = append(summaries, trainingPlanSummaryJSON(t))
	}

	return listResp("TrainingPlanSummaries", summaries, next)
}

type trainingPlanFilterReq struct {
	Name  string `json:"Name"`
	Value string `json:"Value"`
}

func (h *Handler) handleSearchTrainingPlanOfferings(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		InstanceType    string   `json:"InstanceType"`
		UltraServerType string   `json:"UltraServerType"`
		TrainingPlanArn string   `json:"TrainingPlanArn"`
		TargetResources []string `json:"TargetResources"`
		DurationHours   int64    `json:"DurationHours"`
		InstanceCount   int32    `json:"InstanceCount"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	offerings, extOfferings, err := h.Backend.SearchTrainingPlanOfferings(ctx, SearchTrainingPlanOfferingsParams{
		InstanceType:    req.InstanceType,
		UltraServerType: req.UltraServerType,
		TrainingPlanArn: req.TrainingPlanArn,
		TargetResources: req.TargetResources,
		DurationHours:   req.DurationHours,
		InstanceCount:   req.InstanceCount,
	})
	if err != nil {
		return nil, err
	}

	items := make([]map[string]any, 0, len(offerings))

	for _, o := range offerings {
		items = append(items, map[string]any{
			"TrainingPlanOfferingId":    o.TrainingPlanOfferingID,
			"TargetResources":           o.TargetResources,
			"CurrencyCode":              o.CurrencyCode,
			"UpfrontFee":                o.UpfrontFee,
			"DurationHours":             o.DurationHours,
			"DurationMinutes":           o.DurationMinutes,
			"ReservedCapacityOfferings": o.ReservedCapacityOfferings,
		})
	}

	return json.Marshal(map[string]any{
		"TrainingPlanOfferings":          items,
		"TrainingPlanExtensionOfferings": extOfferings,
	})
}

func (h *Handler) handleExtendTrainingPlan(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		TrainingPlanExtensionOfferingID string `json:"TrainingPlanExtensionOfferingId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrainingPlanExtensionOfferingID == "" {
		return nil, fmt.Errorf("%w: TrainingPlanExtensionOfferingId is required", errInvalidRequest)
	}

	extensions, err := h.Backend.ExtendTrainingPlan(ctx, req.TrainingPlanExtensionOfferingID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"TrainingPlanExtensions": extensions})
}

func (h *Handler) handleDescribeTrainingPlanExtensionHistory(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		TrainingPlanArn string `json:"TrainingPlanArn"`
		NextToken       string `json:"NextToken"`
		MaxResults      int32  `json:"MaxResults"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrainingPlanArn == "" {
		return nil, fmt.Errorf("%w: TrainingPlanArn is required", errInvalidRequest)
	}

	extensions, next, err := h.Backend.DescribeTrainingPlanExtensionHistory(
		ctx, req.TrainingPlanArn, req.NextToken, req.MaxResults,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"TrainingPlanExtensions": extensions, keyNextToken: next})
}

func (h *Handler) handleDescribeReservedCapacity(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ReservedCapacityArn string `json:"ReservedCapacityArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ReservedCapacityArn == "" {
		return nil, fmt.Errorf("%w: ReservedCapacityArn is required", errInvalidRequest)
	}

	rc, err := h.Backend.DescribeReservedCapacity(ctx, req.ReservedCapacityArn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(rc)
}

func (h *Handler) handleListUltraServersByReservedCapacity(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ReservedCapacityArn string `json:"ReservedCapacityArn"`
		NextToken           string `json:"NextToken"`
		MaxResults          int32  `json:"MaxResults"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ReservedCapacityArn == "" {
		return nil, fmt.Errorf("%w: ReservedCapacityArn is required", errInvalidRequest)
	}

	servers, next, err := h.Backend.ListUltraServersByReservedCapacity(
		ctx, req.ReservedCapacityArn, req.NextToken, req.MaxResults,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"UltraServers": servers, keyNextToken: next})
}
