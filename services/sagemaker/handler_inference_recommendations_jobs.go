package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// InferenceRecommendationsJob handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateInferenceRecommendationsJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags           map[string]string `json:"Tags,omitempty"`
		JobName        string            `json:"JobName"`
		JobType        string            `json:"JobType,omitempty"`
		JobDescription string            `json:"JobDescription,omitempty"`
		RoleArn        string            `json:"RoleArn,omitempty"`
		InputConfig    json.RawMessage   `json:"InputConfig,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.JobName == "" {
		return nil, fmt.Errorf("%w: JobName is required", errInvalidRequest)
	}

	j, err := h.Backend.CreateInferenceRecommendationsJob(ctx, CreateInferenceRecommendationsJobOptions{
		JobName:        req.JobName,
		JobType:        req.JobType,
		JobDescription: req.JobDescription,
		RoleArn:        req.RoleArn,
		InputConfig:    req.InputConfig,
		Tags:           req.Tags,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyJobArn: j.JobArn})
}

func (h *Handler) handleDescribeInferenceRecommendationsJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		JobName string `json:"JobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.JobName == "" {
		return nil, fmt.Errorf("%w: JobName is required", errInvalidRequest)
	}

	j, err := h.Backend.DescribeInferenceRecommendationsJob(ctx, req.JobName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"JobName":                  j.JobName,
		keyJobArn:                  j.JobArn,
		keyStatus:                  j.Status,
		"InferenceRecommendations": []any{},
		keyCreationTime:            epochSeconds(j.CreationTime),
		keyLastModifiedTime:        epochSeconds(j.LastModifiedTime),
	}

	if j.JobType != "" {
		resp["JobType"] = j.JobType
	}

	if j.JobDescription != "" {
		resp["JobDescription"] = j.JobDescription
	}

	if j.RoleArn != "" {
		resp[keyRoleArn] = j.RoleArn
	}

	if len(j.InputConfig) > 0 {
		resp["InputConfig"] = j.InputConfig
	}

	return json.Marshal(resp)
}

func (h *Handler) handleStopInferenceRecommendationsJob(ctx context.Context, body []byte) error {
	var req struct {
		JobName string `json:"JobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.JobName == "" {
		return fmt.Errorf("%w: JobName is required", errInvalidRequest)
	}

	return h.Backend.StopInferenceRecommendationsJob(ctx, req.JobName)
}

type inferenceRecommendationsJobSummary struct {
	JobName          string  `json:"JobName"`
	JobArn           string  `json:"JobArn"`
	Status           string  `json:"Status"`
	JobType          string  `json:"JobType,omitempty"`
	CreationTime     float64 `json:"CreationTime"`
	LastModifiedTime float64 `json:"LastModifiedTime"`
}

func (h *Handler) handleListInferenceRecommendationsJobs(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	jobs, nextToken := h.Backend.ListInferenceRecommendationsJobs(ctx, req.NextToken)

	summaries := make([]inferenceRecommendationsJobSummary, 0, len(jobs))
	for _, j := range jobs {
		summaries = append(summaries, inferenceRecommendationsJobSummary{
			JobName:          j.JobName,
			JobArn:           j.JobArn,
			Status:           j.Status,
			JobType:          j.JobType,
			CreationTime:     epochSeconds(j.CreationTime),
			LastModifiedTime: epochSeconds(j.LastModifiedTime),
		})
	}

	resp := map[string]any{"InferenceRecommendationsJobs": summaries}
	if nextToken != "" {
		resp[keyNextToken] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleListInferenceRecommendationsJobSteps(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		JobName   string `json:"JobName"`
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.JobName == "" {
		return nil, fmt.Errorf("%w: JobName is required", errInvalidRequest)
	}

	if _, err := h.Backend.DescribeInferenceRecommendationsJob(ctx, req.JobName); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Steps": []any{}})
}
