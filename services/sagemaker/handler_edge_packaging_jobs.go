package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// EdgePackagingJob handlers
// ---------------------------------------------------------------------------

// edgeOutputConfigRequest mirrors types.EdgeOutputConfig
// (api_op_CreateEdgePackagingJob.go:28-66's OutputConfig, "This member is
// required" — previously absent from decode entirely).
type edgeOutputConfigRequest struct {
	S3OutputLocation       string `json:"S3OutputLocation"`
	KmsKeyID               string `json:"KmsKeyId,omitempty"`
	PresetDeploymentConfig string `json:"PresetDeploymentConfig,omitempty"`
	PresetDeploymentType   string `json:"PresetDeploymentType,omitempty"`
}

type createEdgePackagingJobRequest struct {
	OutputConfig         *edgeOutputConfigRequest `json:"OutputConfig,omitempty"`
	EdgePackagingJobName string                   `json:"EdgePackagingJobName"`
	ModelName            string                   `json:"ModelName,omitempty"`
	ModelVersion         string                   `json:"ModelVersion,omitempty"`
	RoleArn              string                   `json:"RoleArn,omitempty"`
	CompilationJobName   string                   `json:"CompilationJobName,omitempty"`
	ResourceKey          string                   `json:"ResourceKey,omitempty"`
	Tags                 []tagObject              `json:"Tags,omitempty"`
}

func (h *Handler) handleCreateEdgePackagingJob(ctx context.Context, body []byte) ([]byte, error) {
	var req createEdgePackagingJobRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.EdgePackagingJobName == "" {
		return nil, fmt.Errorf("%w: EdgePackagingJobName is required", errInvalidRequest)
	}

	if req.ModelName == "" {
		return nil, fmt.Errorf("%w: ModelName is required", errInvalidRequest)
	}

	if req.ModelVersion == "" {
		return nil, fmt.Errorf("%w: ModelVersion is required", errInvalidRequest)
	}

	if req.RoleArn == "" {
		return nil, fmt.Errorf("%w: RoleArn is required", errInvalidRequest)
	}

	if req.CompilationJobName == "" {
		return nil, fmt.Errorf("%w: CompilationJobName is required", errInvalidRequest)
	}

	if req.OutputConfig == nil || req.OutputConfig.S3OutputLocation == "" {
		return nil, fmt.Errorf("%w: OutputConfig.S3OutputLocation is required", errInvalidRequest)
	}

	j, err := h.Backend.CreateEdgePackagingJob(ctx, CreateEdgePackagingJobOptions{
		EdgePackagingJobName: req.EdgePackagingJobName,
		ModelName:            req.ModelName,
		ModelVersion:         req.ModelVersion,
		RoleArn:              req.RoleArn,
		CompilationJobName:   req.CompilationJobName,
		ResourceKey:          req.ResourceKey,
		OutputConfig: EdgeOutputConfig{
			S3OutputLocation:       req.OutputConfig.S3OutputLocation,
			KmsKeyID:               req.OutputConfig.KmsKeyID,
			PresetDeploymentConfig: req.OutputConfig.PresetDeploymentConfig,
			PresetDeploymentType:   req.OutputConfig.PresetDeploymentType,
		},
		Tags: fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyEdgePackagingJobArn: j.EdgePackagingJobArn})
}

type describeEdgePackagingJobRequest struct {
	EdgePackagingJobName string `json:"EdgePackagingJobName"`
}

func (h *Handler) handleDescribeEdgePackagingJob(ctx context.Context, body []byte) ([]byte, error) {
	var req describeEdgePackagingJobRequest

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
		keyOutputConfig:          j.OutputConfig,
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

	if j.ResourceKey != "" {
		resp["ResourceKey"] = j.ResourceKey
	}

	if j.FailureReason != "" {
		resp["FailureReason"] = j.FailureReason
	}

	return json.Marshal(resp)
}

type stopEdgePackagingJobRequest struct {
	EdgePackagingJobName string `json:"EdgePackagingJobName"`
}

func (h *Handler) handleStopEdgePackagingJob(ctx context.Context, body []byte) error {
	var req stopEdgePackagingJobRequest

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

type listEdgePackagingJobsRequest struct {
	CreationTimeAfter      *float64 `json:"CreationTimeAfter,omitempty"`
	CreationTimeBefore     *float64 `json:"CreationTimeBefore,omitempty"`
	LastModifiedTimeAfter  *float64 `json:"LastModifiedTimeAfter,omitempty"`
	LastModifiedTimeBefore *float64 `json:"LastModifiedTimeBefore,omitempty"`
	NextToken              string   `json:"NextToken"`
	StatusEquals           string   `json:"StatusEquals,omitempty"`
	NameContains           string   `json:"NameContains,omitempty"`
	ModelNameContains      string   `json:"ModelNameContains,omitempty"`
	SortBy                 string   `json:"SortBy,omitempty"`
	SortOrder              string   `json:"SortOrder,omitempty"`
	MaxResults             int32    `json:"MaxResults,omitempty"`
}

func (h *Handler) handleListEdgePackagingJobs(ctx context.Context, body []byte) ([]byte, error) {
	var req listEdgePackagingJobsRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	jobs, nextToken := h.Backend.ListEdgePackagingJobs(ctx, req.NextToken, ListEdgePackagingJobsFilter{
		CreationTimeAfter:      epochPtr(req.CreationTimeAfter),
		CreationTimeBefore:     epochPtr(req.CreationTimeBefore),
		LastModifiedTimeAfter:  epochPtr(req.LastModifiedTimeAfter),
		LastModifiedTimeBefore: epochPtr(req.LastModifiedTimeBefore),
		StatusEquals:           req.StatusEquals,
		NameContains:           req.NameContains,
		ModelNameContains:      req.ModelNameContains,
		SortBy:                 req.SortBy,
		SortOrder:              req.SortOrder,
		MaxResults:             req.MaxResults,
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
