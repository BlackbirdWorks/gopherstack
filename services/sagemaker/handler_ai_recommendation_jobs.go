package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// aiRecommendationJobOpsSupported returns the operations handled by
// dispatchAIRecommendationJobOps.
func aiRecommendationJobOpsSupported() []string {
	return []string{
		"CreateAIRecommendationJob",
		"DescribeAIRecommendationJob",
		"DeleteAIRecommendationJob",
		"StopAIRecommendationJob",
		"ListAIRecommendationJobs",
	}
}

// dispatchAIRecommendationJobOps handles the AIRecommendationJob operation family.
func (h *Handler) dispatchAIRecommendationJobOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateAIRecommendationJob":
		r, err := h.handleCreateAIRecommendationJob(ctx, body)

		return r, true, err
	case "DescribeAIRecommendationJob":
		r, err := h.handleDescribeAIRecommendationJob(ctx, body)

		return r, true, err
	case "DeleteAIRecommendationJob":
		r, err := h.handleDeleteAIRecommendationJob(ctx, body)

		return r, true, err
	case "StopAIRecommendationJob":
		r, err := h.handleStopAIRecommendationJob(ctx, body)

		return r, true, err
	case "ListAIRecommendationJobs":
		r, err := h.handleListAIRecommendationJobs(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) handleCreateAIRecommendationJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ModelSource                json.RawMessage `json:"ModelSource"`
		OutputConfig               json.RawMessage `json:"OutputConfig"`
		PerformanceTarget          json.RawMessage `json:"PerformanceTarget"`
		ComputeSpec                json.RawMessage `json:"ComputeSpec"`
		InferenceSpecification     json.RawMessage `json:"InferenceSpecification"`
		OptimizeModel              *bool           `json:"OptimizeModel"`
		AIRecommendationJobName    string          `json:"AIRecommendationJobName"`
		AIWorkloadConfigIdentifier string          `json:"AIWorkloadConfigIdentifier"`
		RoleArn                    string          `json:"RoleArn"`
		Tags                       []tagObject     `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	j, err := h.Backend.CreateAIRecommendationJob(ctx, CreateAIRecommendationJobOptions{
		AIRecommendationJobName:    req.AIRecommendationJobName,
		AIWorkloadConfigIdentifier: req.AIWorkloadConfigIdentifier,
		RoleArn:                    req.RoleArn,
		ModelSource:                req.ModelSource,
		OutputConfig:               req.OutputConfig,
		PerformanceTarget:          req.PerformanceTarget,
		ComputeSpec:                req.ComputeSpec,
		InferenceSpecification:     req.InferenceSpecification,
		OptimizeModel:              req.OptimizeModel,
		Tags:                       fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyAIRecommendationJobArn: j.AIRecommendationJobArn})
}

func (h *Handler) handleDescribeAIRecommendationJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		AIRecommendationJobName string `json:"AIRecommendationJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AIRecommendationJobName == "" {
		return nil, fmt.Errorf("%w: AIRecommendationJobName is required", errInvalidRequest)
	}

	j, err := h.Backend.DescribeAIRecommendationJob(ctx, req.AIRecommendationJobName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(j)
}

func (h *Handler) handleDeleteAIRecommendationJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		AIRecommendationJobName string `json:"AIRecommendationJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AIRecommendationJobName == "" {
		return nil, fmt.Errorf("%w: AIRecommendationJobName is required", errInvalidRequest)
	}

	jobARN, err := h.Backend.DeleteAIRecommendationJob(ctx, req.AIRecommendationJobName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyAIRecommendationJobArn: jobARN})
}

func (h *Handler) handleStopAIRecommendationJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		AIRecommendationJobName string `json:"AIRecommendationJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AIRecommendationJobName == "" {
		return nil, fmt.Errorf("%w: AIRecommendationJobName is required", errInvalidRequest)
	}

	jobARN, err := h.Backend.StopAIRecommendationJob(ctx, req.AIRecommendationJobName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyAIRecommendationJobArn: jobARN})
}

func (h *Handler) handleListAIRecommendationJobs(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		CreationTimeAfter  *float64 `json:"CreationTimeAfter"`
		CreationTimeBefore *float64 `json:"CreationTimeBefore"`
		NameContains       string   `json:"NameContains"`
		StatusEquals       string   `json:"StatusEquals"`
		NextToken          string   `json:"NextToken"`
		SortBy             string   `json:"SortBy"`
		SortOrder          string   `json:"SortOrder"`
		MaxResults         int32    `json:"MaxResults"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	jobs, nextToken := h.Backend.ListAIRecommendationJobs(ctx, ListAIRecommendationJobsParams{
		CreationTimeAfter:  epochPtr(req.CreationTimeAfter),
		CreationTimeBefore: epochPtr(req.CreationTimeBefore),
		NameContains:       req.NameContains,
		StatusEquals:       req.StatusEquals,
		SortBy:             req.SortBy,
		SortOrder:          req.SortOrder,
		NextToken:          req.NextToken,
		MaxResults:         req.MaxResults,
	})

	items := make([]map[string]any, 0, len(jobs))
	for _, j := range jobs {
		item := map[string]any{
			keyAIRecommendationJobArn:   j.AIRecommendationJobArn,
			"AIRecommendationJobName":   j.AIRecommendationJobName,
			"AIRecommendationJobStatus": j.AIRecommendationJobStatus,
			keyCreationTime:             epochSeconds(j.CreationTime),
		}

		if j.EndTime != nil {
			item["EndTime"] = epochSeconds(*j.EndTime)
		}

		items = append(items, item)
	}

	return listResp("AIRecommendationJobs", items, nextToken)
}
