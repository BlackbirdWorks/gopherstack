package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// aiBenchmarkJobOpsSupported returns the operations handled by
// dispatchAIBenchmarkJobOps.
func aiBenchmarkJobOpsSupported() []string {
	return []string{
		"CreateAIBenchmarkJob",
		"DescribeAIBenchmarkJob",
		"DeleteAIBenchmarkJob",
		"StopAIBenchmarkJob",
		"ListAIBenchmarkJobs",
	}
}

// dispatchAIBenchmarkJobOps handles the AIBenchmarkJob operation family.
func (h *Handler) dispatchAIBenchmarkJobOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateAIBenchmarkJob":
		r, err := h.handleCreateAIBenchmarkJob(ctx, body)

		return r, true, err
	case "DescribeAIBenchmarkJob":
		r, err := h.handleDescribeAIBenchmarkJob(ctx, body)

		return r, true, err
	case "DeleteAIBenchmarkJob":
		r, err := h.handleDeleteAIBenchmarkJob(ctx, body)

		return r, true, err
	case "StopAIBenchmarkJob":
		r, err := h.handleStopAIBenchmarkJob(ctx, body)

		return r, true, err
	case "ListAIBenchmarkJobs":
		r, err := h.handleListAIBenchmarkJobs(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) handleCreateAIBenchmarkJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		BenchmarkTarget            json.RawMessage `json:"BenchmarkTarget"`
		OutputConfig               json.RawMessage `json:"OutputConfig"`
		NetworkConfig              json.RawMessage `json:"NetworkConfig"`
		AIBenchmarkJobName         string          `json:"AIBenchmarkJobName"`
		AIWorkloadConfigIdentifier string          `json:"AIWorkloadConfigIdentifier"`
		RoleArn                    string          `json:"RoleArn"`
		Tags                       []tagObject     `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	j, err := h.Backend.CreateAIBenchmarkJob(ctx, CreateAIBenchmarkJobOptions{
		AIBenchmarkJobName:         req.AIBenchmarkJobName,
		AIWorkloadConfigIdentifier: req.AIWorkloadConfigIdentifier,
		RoleArn:                    req.RoleArn,
		BenchmarkTarget:            req.BenchmarkTarget,
		OutputConfig:               req.OutputConfig,
		NetworkConfig:              req.NetworkConfig,
		Tags:                       fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyAIBenchmarkJobArn: j.AIBenchmarkJobArn})
}

func (h *Handler) handleDescribeAIBenchmarkJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		AIBenchmarkJobName string `json:"AIBenchmarkJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AIBenchmarkJobName == "" {
		return nil, fmt.Errorf("%w: AIBenchmarkJobName is required", errInvalidRequest)
	}

	j, err := h.Backend.DescribeAIBenchmarkJob(ctx, req.AIBenchmarkJobName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(j)
}

func (h *Handler) handleDeleteAIBenchmarkJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		AIBenchmarkJobName string `json:"AIBenchmarkJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AIBenchmarkJobName == "" {
		return nil, fmt.Errorf("%w: AIBenchmarkJobName is required", errInvalidRequest)
	}

	jobARN, err := h.Backend.DeleteAIBenchmarkJob(ctx, req.AIBenchmarkJobName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyAIBenchmarkJobArn: jobARN})
}

func (h *Handler) handleStopAIBenchmarkJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		AIBenchmarkJobName string `json:"AIBenchmarkJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AIBenchmarkJobName == "" {
		return nil, fmt.Errorf("%w: AIBenchmarkJobName is required", errInvalidRequest)
	}

	jobARN, err := h.Backend.StopAIBenchmarkJob(ctx, req.AIBenchmarkJobName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyAIBenchmarkJobArn: jobARN})
}

func (h *Handler) handleListAIBenchmarkJobs(ctx context.Context, body []byte) ([]byte, error) {
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

	jobs, nextToken := h.Backend.ListAIBenchmarkJobs(ctx, ListAIBenchmarkJobsParams{
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
			keyAIBenchmarkJobArn:   j.AIBenchmarkJobArn,
			"AIBenchmarkJobName":   j.AIBenchmarkJobName,
			"AIBenchmarkJobStatus": j.AIBenchmarkJobStatus,
			keyCreationTime:        epochSeconds(j.CreationTime),
		}

		if j.AIWorkloadConfigIdentifier != "" {
			item["AIWorkloadConfigName"] = aiWorkloadConfigNameFromIdentifier(j.AIWorkloadConfigIdentifier)
		}

		if j.EndTime != nil {
			item["EndTime"] = epochSeconds(*j.EndTime)
		}

		items = append(items, item)
	}

	return listResp("AIBenchmarkJobs", items, nextToken)
}
