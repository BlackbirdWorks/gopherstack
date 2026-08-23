package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// ---------------------------------------------------------------------------
// TransformJob handlers (gap #12 partial)
// ---------------------------------------------------------------------------

type createTransformJobRequest struct {
	Environment             map[string]string  `json:"Environment,omitempty"`
	TransformInput          TransformInput     `json:"TransformInput"`
	TransformOutput         TransformOutput    `json:"TransformOutput"`
	TransformJobName        string             `json:"TransformJobName"`
	ModelName               string             `json:"ModelName"`
	BatchStrategy           string             `json:"BatchStrategy,omitempty"`
	TransformResources      TransformResources `json:"TransformResources"`
	Tags                    []tagObject        `json:"Tags,omitempty"`
	MaxConcurrentTransforms int32              `json:"MaxConcurrentTransforms,omitempty"`
	MaxPayloadInMB          int32              `json:"MaxPayloadInMB,omitempty"`
}

func (h *Handler) handleCreateTransformJob(ctx context.Context, body []byte) ([]byte, error) {
	var req createTransformJobRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TransformJobName == "" {
		return nil, fmt.Errorf("%w: TransformJobName is required", errInvalidRequest)
	}

	if req.ModelName == "" {
		return nil, fmt.Errorf("%w: ModelName is required", errInvalidRequest)
	}

	if req.TransformInput.DataSource.S3DataSource.S3Uri == "" {
		return nil, fmt.Errorf("%w: TransformInput.DataSource.S3DataSource.S3Uri is required", errInvalidRequest)
	}

	if req.TransformOutput.S3OutputPath == "" {
		return nil, fmt.Errorf("%w: TransformOutput.S3OutputPath is required", errInvalidRequest)
	}

	if req.TransformResources.InstanceType == "" {
		return nil, fmt.Errorf("%w: TransformResources.InstanceType is required", errInvalidRequest)
	}

	if req.TransformResources.InstanceCount == 0 {
		return nil, fmt.Errorf("%w: TransformResources.InstanceCount is required", errInvalidRequest)
	}

	tj, err := h.Backend.CreateTransformJob(ctx, TransformJobOptions{
		TransformJobName:        req.TransformJobName,
		ModelName:               req.ModelName,
		BatchStrategy:           req.BatchStrategy,
		MaxConcurrentTransforms: req.MaxConcurrentTransforms,
		MaxPayloadInMB:          req.MaxPayloadInMB,
		TransformInput:          req.TransformInput,
		TransformOutput:         req.TransformOutput,
		TransformResources:      req.TransformResources,
		Environment:             req.Environment,
		Tags:                    fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: created transform job", "name", tj.TransformJobName)

	return json.Marshal(map[string]string{"TransformJobArn": tj.TransformJobArn})
}

type describeTransformJobRequest struct {
	TransformJobName string `json:"TransformJobName"`
}

func (h *Handler) handleDescribeTransformJob(ctx context.Context, body []byte) ([]byte, error) {
	var req describeTransformJobRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TransformJobName == "" {
		return nil, fmt.Errorf("%w: TransformJobName is required", errInvalidRequest)
	}

	tj, err := h.Backend.DescribeTransformJob(ctx, req.TransformJobName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"TransformJobName":   tj.TransformJobName,
		"TransformJobArn":    tj.TransformJobArn,
		"TransformJobStatus": tj.TransformJobStatus,
		"ModelName":          tj.ModelName,
		"TransformInput":     tj.TransformInput,
		"TransformOutput":    tj.TransformOutput,
		"TransformResources": tj.TransformResources,
		keyCreationTime:      epochSeconds(tj.CreationTime),
		keyLastModifiedTime:  epochSeconds(tj.LastModifiedTime),
	}
	if tj.BatchStrategy != "" {
		resp["BatchStrategy"] = tj.BatchStrategy
	}
	if tj.FailureReason != "" {
		resp["FailureReason"] = tj.FailureReason
	}
	if tj.TransformStartTime != nil {
		resp["TransformStartTime"] = epochSeconds(*tj.TransformStartTime)
	}
	if tj.TransformEndTime != nil {
		resp["TransformEndTime"] = epochSeconds(*tj.TransformEndTime)
	}
	if len(tj.Environment) > 0 {
		resp["Environment"] = tj.Environment
	}

	return json.Marshal(resp)
}

type stopTransformJobRequest struct {
	TransformJobName string `json:"TransformJobName"`
}

func (h *Handler) handleStopTransformJob(ctx context.Context, body []byte) error {
	var req stopTransformJobRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TransformJobName == "" {
		return fmt.Errorf("%w: TransformJobName is required", errInvalidRequest)
	}

	if err := h.Backend.StopTransformJob(ctx, req.TransformJobName); err != nil {
		return err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: stopping transform job", "name", req.TransformJobName)

	return nil
}

type transformJobSummary struct {
	TransformJobName   string  `json:"TransformJobName"`
	TransformJobArn    string  `json:"TransformJobArn"`
	TransformJobStatus string  `json:"TransformJobStatus"`
	ModelName          string  `json:"ModelName"`
	CreationTime       float64 `json:"CreationTime"`
	LastModifiedTime   float64 `json:"LastModifiedTime"`
}

type listTransformJobsRequest struct {
	CreationTimeAfter      *float64 `json:"CreationTimeAfter,omitempty"`
	CreationTimeBefore     *float64 `json:"CreationTimeBefore,omitempty"`
	LastModifiedTimeAfter  *float64 `json:"LastModifiedTimeAfter,omitempty"`
	LastModifiedTimeBefore *float64 `json:"LastModifiedTimeBefore,omitempty"`
	NextToken              string   `json:"NextToken"`
	StatusEquals           string   `json:"StatusEquals,omitempty"`
	NameContains           string   `json:"NameContains,omitempty"`
	SortBy                 string   `json:"SortBy,omitempty"`
	SortOrder              string   `json:"SortOrder,omitempty"`
	MaxResults             int32    `json:"MaxResults,omitempty"`
}

func (h *Handler) handleListTransformJobs(ctx context.Context, body []byte) ([]byte, error) {
	var req listTransformJobsRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	filter := ListTransformJobsFilter{
		CreationTimeAfter:      epochPtr(req.CreationTimeAfter),
		CreationTimeBefore:     epochPtr(req.CreationTimeBefore),
		LastModifiedTimeAfter:  epochPtr(req.LastModifiedTimeAfter),
		LastModifiedTimeBefore: epochPtr(req.LastModifiedTimeBefore),
		StatusEquals:           req.StatusEquals,
		NameContains:           req.NameContains,
		SortBy:                 req.SortBy,
		SortOrder:              req.SortOrder,
		MaxResults:             req.MaxResults,
	}

	jobs, nextToken := h.Backend.ListTransformJobs(ctx, req.NextToken, filter)
	summaries := make([]transformJobSummary, 0, len(jobs))

	for _, tj := range jobs {
		summaries = append(summaries, transformJobSummary{
			TransformJobName:   tj.TransformJobName,
			TransformJobArn:    tj.TransformJobArn,
			TransformJobStatus: tj.TransformJobStatus,
			ModelName:          tj.ModelName,
			CreationTime:       epochSeconds(tj.CreationTime),
			LastModifiedTime:   epochSeconds(tj.LastModifiedTime),
		})
	}

	resp := map[string]any{"TransformJobSummaries": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}
