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

// listCandidatesForAutoMLJobInput mirrors ListCandidatesForAutoMLJobInput
// (api_op_ListCandidatesForAutoMLJob.go:27-52).
type listCandidatesForAutoMLJobInput struct {
	AutoMLJobName       string `json:"AutoMLJobName"`
	CandidateNameEquals string `json:"CandidateNameEquals"`
	StatusEquals        string `json:"StatusEquals"`
	SortBy              string `json:"SortBy"`
	SortOrder           string `json:"SortOrder"`
	NextToken           string `json:"NextToken"`
	MaxResults          int32  `json:"MaxResults"`
}

func (h *Handler) handleListCandidatesForAutoMLJob(ctx context.Context, body []byte) ([]byte, error) {
	var req listCandidatesForAutoMLJobInput

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
			SortBy:              req.SortBy,
			SortOrder:           req.SortOrder,
			NextToken:           req.NextToken,
			MaxResults:          req.MaxResults,
		},
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Candidates": candidates, keyNextToken: next})
}

// searchInput mirrors SearchInput (api_op_Search.go:27-63). VisibilityConditions
// is not decoded: this backend is single-tenant and models no per-resource
// caller-visibility ACL concept anywhere, the same reasoning already applied
// to CreatedBy/LastModifiedBy (types.UserContext) elsewhere in this service.
type searchInput struct {
	Resource                 string `json:"Resource"`
	CrossAccountFilterOption string `json:"CrossAccountFilterOption"`
	SortBy                   string `json:"SortBy"`
	SortOrder                string `json:"SortOrder"`
	NextToken                string `json:"NextToken"`
	SearchExpression         struct {
		Operator string         `json:"Operator"`
		Filters  []SearchFilter `json:"Filters"`
	} `json:"SearchExpression"`
	MaxResults int32 `json:"MaxResults"`
}

func (h *Handler) handleSearch(ctx context.Context, body []byte) ([]byte, error) {
	var req searchInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Resource == "" {
		return nil, fmt.Errorf("%w: Resource is required", errInvalidRequest)
	}

	results, total, next, err := h.Backend.Search(ctx, SearchParams{
		Resource:                 req.Resource,
		BooleanOperator:          req.SearchExpression.Operator,
		Filters:                  req.SearchExpression.Filters,
		NextToken:                req.NextToken,
		SortBy:                   req.SortBy,
		SortOrder:                req.SortOrder,
		CrossAccountFilterOption: req.CrossAccountFilterOption,
		MaxResults:               req.MaxResults,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"Results":    results,
		"TotalHits":  map[string]any{"Value": total, "Relation": "EqualTo"},
		keyNextToken: next,
	})
}

// listModelMetadataInput mirrors ListModelMetadataInput
// (api_op_ListModelMetadata.go:27-42).
type listModelMetadataInput struct {
	NextToken        string `json:"NextToken"`
	SearchExpression struct {
		Filters []ModelMetadataFilter `json:"Filters"`
	} `json:"SearchExpression"`
	MaxResults int32 `json:"MaxResults"`
}

func (h *Handler) handleListModelMetadata(body []byte) ([]byte, error) {
	var req listModelMetadataInput

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

// getSearchSuggestionsInput mirrors GetSearchSuggestionsInput
// (api_op_GetSearchSuggestions.go:27-38).
type getSearchSuggestionsInput struct {
	Resource        string `json:"Resource"`
	SuggestionQuery struct {
		PropertyNameQuery struct {
			PropertyNameHint string `json:"PropertyNameHint"`
		} `json:"PropertyNameQuery"`
	} `json:"SuggestionQuery"`
}

func (h *Handler) handleGetSearchSuggestions(_ context.Context, body []byte) ([]byte, error) {
	var req getSearchSuggestionsInput

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

// getScalingConfigurationRecommendationInput mirrors
// GetScalingConfigurationRecommendationInput
// (api_op_GetScalingConfigurationRecommendation.go:27-53). ScalingPolicyObjective
// was previously entirely absent from decode -- an accept-and-drop gap, since
// the real response echoes it back verbatim.
type getScalingConfigurationRecommendationInput struct {
	InferenceRecommendationsJobName string                  `json:"InferenceRecommendationsJobName"`
	EndpointName                    string                  `json:"EndpointName"`
	ScalingPolicyObjective          *ScalingPolicyObjective `json:"ScalingPolicyObjective"`
	RecommendationID                string                  `json:"RecommendationId"`
	TargetCPUUtilizationPerCore     int32                   `json:"TargetCpuUtilizationPerCore"`
}

func (h *Handler) handleGetScalingConfigurationRecommendation(ctx context.Context, body []byte) ([]byte, error) {
	var req getScalingConfigurationRecommendationInput

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

	resp := map[string]any{
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
		"Metric": map[string]any{
			"InvocationsPerInstance": rec.InvocationsPerInstance,
			"ModelLatency":           rec.ModelLatency,
		},
	}

	if req.ScalingPolicyObjective != nil {
		resp["ScalingPolicyObjective"] = req.ScalingPolicyObjective
	}

	return json.Marshal(resp)
}
