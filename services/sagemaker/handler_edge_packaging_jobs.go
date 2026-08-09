package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// EdgePackagingJob handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateEdgePackagingJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		EdgePackagingJobName string      `json:"EdgePackagingJobName"`
		ModelName            string      `json:"ModelName,omitempty"`
		ModelVersion         string      `json:"ModelVersion,omitempty"`
		RoleArn              string      `json:"RoleArn,omitempty"`
		CompilationJobName   string      `json:"CompilationJobName,omitempty"`
		Tags                 []tagObject `json:"Tags,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.EdgePackagingJobName == "" {
		return nil, fmt.Errorf("%w: EdgePackagingJobName is required", errInvalidRequest)
	}

	j, err := h.Backend.CreateEdgePackagingJob(ctx, CreateEdgePackagingJobOptions{
		EdgePackagingJobName: req.EdgePackagingJobName,
		ModelName:            req.ModelName,
		ModelVersion:         req.ModelVersion,
		RoleArn:              req.RoleArn,
		CompilationJobName:   req.CompilationJobName,
		Tags:                 fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyEdgePackagingJobArn: j.EdgePackagingJobArn})
}

func (h *Handler) handleDescribeEdgePackagingJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		EdgePackagingJobName string `json:"EdgePackagingJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.EdgePackagingJobName == "" {
		return nil, fmt.Errorf("%w: EdgePackagingJobName is required", errInvalidRequest)
	}

	j, err := h.Backend.DescribeEdgePackagingJob(ctx, req.EdgePackagingJobName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"EdgePackagingJobName":   j.EdgePackagingJobName,
		"EdgePackagingJobArn":    j.EdgePackagingJobArn,
		"EdgePackagingJobStatus": j.EdgePackagingJobStatus,
		keyCreationTime:          epochSeconds(j.CreationTime),
		keyLastModifiedTime:      epochSeconds(j.LastModifiedTime),
	}

	if j.ModelName != "" {
		resp["ModelName"] = j.ModelName
	}

	if j.ModelVersion != "" {
		resp["ModelVersion"] = j.ModelVersion
	}

	if j.RoleArn != "" {
		resp[keyRoleArn] = j.RoleArn
	}

	if j.CompilationJobName != "" {
		resp["CompilationJobName"] = j.CompilationJobName
	}

	if j.FailureReason != "" {
		resp["FailureReason"] = j.FailureReason
	}

	return json.Marshal(resp)
}

func (h *Handler) handleStopEdgePackagingJob(ctx context.Context, body []byte) error {
	var req struct {
		EdgePackagingJobName string `json:"EdgePackagingJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.EdgePackagingJobName == "" {
		return fmt.Errorf("%w: EdgePackagingJobName is required", errInvalidRequest)
	}

	return h.Backend.StopEdgePackagingJob(ctx, req.EdgePackagingJobName)
}

type edgePackagingJobSummary struct {
	EdgePackagingJobName   string  `json:"EdgePackagingJobName"`
	EdgePackagingJobArn    string  `json:"EdgePackagingJobArn"`
	EdgePackagingJobStatus string  `json:"EdgePackagingJobStatus"`
	ModelName              string  `json:"ModelName,omitempty"`
	ModelVersion           string  `json:"ModelVersion,omitempty"`
	CreationTime           float64 `json:"CreationTime"`
	LastModifiedTime       float64 `json:"LastModifiedTime"`
}

func (h *Handler) handleListEdgePackagingJobs(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken    string `json:"NextToken"`
		StatusEquals string `json:"StatusEquals,omitempty"`
		NameContains string `json:"NameContains,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	jobs, nextToken := h.Backend.ListEdgePackagingJobs(ctx, req.NextToken, ListEdgePackagingJobsFilter{
		StatusEquals: req.StatusEquals,
		NameContains: req.NameContains,
	})

	summaries := make([]edgePackagingJobSummary, 0, len(jobs))
	for _, j := range jobs {
		summaries = append(summaries, edgePackagingJobSummary{
			EdgePackagingJobName:   j.EdgePackagingJobName,
			EdgePackagingJobArn:    j.EdgePackagingJobArn,
			EdgePackagingJobStatus: j.EdgePackagingJobStatus,
			ModelName:              j.ModelName,
			ModelVersion:           j.ModelVersion,
			CreationTime:           epochSeconds(j.CreationTime),
			LastModifiedTime:       epochSeconds(j.LastModifiedTime),
		})
	}

	resp := map[string]any{"EdgePackagingJobSummaries": summaries}
	if nextToken != "" {
		resp[keyNextToken] = nextToken
	}

	return json.Marshal(resp)
}
