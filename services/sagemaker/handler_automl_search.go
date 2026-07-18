package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// automlSearchExtOpsSupported returns the real stateful operations
// implemented in this file (AutoML candidates / Search / model metadata family).
func automlSearchExtOpsSupported() []string {
	return []string{
		"ListCandidatesForAutoMLJob",
		"Search",
		"ListModelMetadata",
		"GetSearchSuggestions",
		"GetScalingConfigurationRecommendation",
	}
}

// dispatchAutoMLSearchExtOps dispatches the AutoML candidates / Search /
// model metadata family of real stateful operations.
func (h *Handler) dispatchAutoMLSearchExtOps(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	case "ListCandidatesForAutoMLJob":
		r, err := h.handleListCandidatesForAutoMLJob(ctx, body)

		return r, true, err
	case "Search":
		r, err := h.handleSearch(ctx, body)

		return r, true, err
	case "ListModelMetadata":
		r, err := h.handleListModelMetadata(body)

		return r, true, err
	case "GetSearchSuggestions":
		r, err := h.handleGetSearchSuggestions(ctx, body)

		return r, true, err
	case "GetScalingConfigurationRecommendation":
		r, err := h.handleGetScalingConfigurationRecommendation(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) handleListCandidatesForAutoMLJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		AutoMLJobName       string `json:"AutoMLJobName"`
		CandidateNameEquals string `json:"CandidateNameEquals"`
		StatusEquals        string `json:"StatusEquals"`
		NextToken           string `json:"NextToken"`
		MaxResults          int32  `json:"MaxResults"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AutoMLJobName == "" {
		return nil, fmt.Errorf("%w: AutoMLJobName is required", errInvalidRequest)
	}

	candidates, next, err := h.Backend.ListCandidatesForAutoMLJob(
		ctx, req.AutoMLJobName, ListCandidatesForAutoMLJobParams{
			CandidateNameEquals: req.CandidateNameEquals,
			StatusEquals:        req.StatusEquals,
			NextToken:           req.NextToken,
			MaxResults:          req.MaxResults,
		},
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Candidates": candidates, keyNextToken: next})
}

func (h *Handler) handleSearch(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Resource         string `json:"Resource"`
		SortBy           string `json:"SortBy"`
		SortOrder        string `json:"SortOrder"`
		NextToken        string `json:"NextToken"`
		SearchExpression struct {
			Operator string         `json:"Operator"`
			Filters  []SearchFilter `json:"Filters"`
		} `json:"SearchExpression"`
		MaxResults int32 `json:"MaxResults"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Resource == "" {
		return nil, fmt.Errorf("%w: Resource is required", errInvalidRequest)
	}

	results, next, err := h.Backend.Search(ctx, SearchParams{
		Resource:        req.Resource,
		BooleanOperator: req.SearchExpression.Operator,
		Filters:         req.SearchExpression.Filters,
		NextToken:       req.NextToken,
		MaxResults:      req.MaxResults,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Results": results, keyNextToken: next})
}

func (h *Handler) handleListModelMetadata(body []byte) ([]byte, error) {
	var req struct {
		NextToken        string `json:"NextToken"`
		SearchExpression struct {
			Filters []ModelMetadataFilter `json:"Filters"`
		} `json:"SearchExpression"`
		MaxResults int32 `json:"MaxResults"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	entries, next := h.Backend.ListModelMetadata(req.SearchExpression.Filters, req.NextToken, req.MaxResults)

	summaries := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		summaries = append(summaries, map[string]any{
			"Domain":           e.Domain,
			"Framework":        e.Framework,
			"FrameworkVersion": e.FrameworkVersion,
			resourceModel:      e.Model,
			"Task":             e.Task,
		})
	}

	return json.Marshal(map[string]any{"ModelMetadataSummaries": summaries, keyNextToken: next})
}

func (h *Handler) handleGetSearchSuggestions(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Resource        string `json:"Resource"`
		SuggestionQuery struct {
			PropertyNameQuery struct {
				PropertyNameHint string `json:"PropertyNameHint"`
			} `json:"PropertyNameQuery"`
		} `json:"SuggestionQuery"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Resource == "" {
		return nil, fmt.Errorf("%w: Resource is required", errInvalidRequest)
	}

	names, err := h.Backend.GetSearchSuggestions(req.Resource, req.SuggestionQuery.PropertyNameQuery.PropertyNameHint)
	if err != nil {
		return nil, err
	}

	suggestions := make([]map[string]string, 0, len(names))
	for _, n := range names {
		suggestions = append(suggestions, map[string]string{"PropertyName": n})
	}

	return json.Marshal(map[string]any{"PropertyNameSuggestions": suggestions})
}

func (h *Handler) handleGetScalingConfigurationRecommendation(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		InferenceRecommendationsJobName string `json:"InferenceRecommendationsJobName"`
		EndpointName                    string `json:"EndpointName"`
		RecommendationID                string `json:"RecommendationId"`
		TargetCPUUtilizationPerCore     int32  `json:"TargetCpuUtilizationPerCore"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.InferenceRecommendationsJobName == "" {
		return nil, fmt.Errorf("%w: InferenceRecommendationsJobName is required", errInvalidRequest)
	}

	rec, err := h.Backend.GetScalingConfigurationRecommendation(
		ctx, req.InferenceRecommendationsJobName, req.TargetCPUUtilizationPerCore,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"InferenceRecommendationsJobName": req.InferenceRecommendationsJobName,
		keyEndpointNameField:              req.EndpointName,
		"RecommendationId":                req.RecommendationID,
		"TargetCpuUtilizationPerCore":     rec.TargetCPUUtilizationPerCore,
		"DynamicScalingConfiguration": map[string]any{
			"MinCapacity":      rec.MinCapacity,
			"MaxCapacity":      rec.MaxCapacity,
			"ScaleInCooldown":  rec.ScaleInCooldown,
			"ScaleOutCooldown": rec.ScaleOutCooldown,
		},
	})
}
