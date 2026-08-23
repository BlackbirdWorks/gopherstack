package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// CompilationJob handlers
// ---------------------------------------------------------------------------

// createCompilationJobInput mirrors CreateCompilationJobInput
// (api_op_CreateCompilationJob.go:57-125).
type createCompilationJobInput struct {
	InputConfig            *CompilationInputConfig  `json:"InputConfig"`
	OutputConfig           *CompilationOutputConfig `json:"OutputConfig"`
	StoppingCondition      *StoppingCondition       `json:"StoppingCondition"`
	CompilationJobName     string                   `json:"CompilationJobName"`
	RoleArn                string                   `json:"RoleArn"`
	ModelPackageVersionArn string                   `json:"ModelPackageVersionArn"`
	VpcConfig              json.RawMessage          `json:"VpcConfig"`
	Tags                   []tagObject              `json:"Tags"`
}

func (h *Handler) handleCreateCompilationJob(ctx context.Context, body []byte) ([]byte, error) {
	var req createCompilationJobInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CompilationJobName == "" {
		return nil, fmt.Errorf("%w: CompilationJobName is required", errInvalidRequest)
	}

	// api_op_CreateCompilationJob.go:60,98,102: RoleArn, OutputConfig, and
	// StoppingCondition are all required -- previously none of the three
	// were enforced, so a request omitting all of them still succeeded.
	if req.RoleArn == "" {
		return nil, fmt.Errorf("%w: RoleArn is required", errInvalidRequest)
	}

	if req.OutputConfig == nil {
		return nil, fmt.Errorf("%w: OutputConfig is required", errInvalidRequest)
	}

	if req.StoppingCondition == nil {
		return nil, fmt.Errorf("%w: StoppingCondition is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateCompilationJob(ctx, req.CompilationJobName, req.RoleArn, fromTagObjects(req.Tags))
	if err != nil {
		return nil, err
	}

	if extErr := h.Backend.SetCompilationJobExtras(
		ctx, req.CompilationJobName, req.InputConfig, req.OutputConfig, req.StoppingCondition,
		req.ModelPackageVersionArn, req.VpcConfig,
	); extErr != nil {
		return nil, extErr
	}

	return json.Marshal(map[string]any{"CompilationJobArn": result.CompilationJobArn})
}

// describeCompilationJobInput mirrors DescribeCompilationJobInput
// (api_op_DescribeCompilationJob.go:29-34).
type describeCompilationJobInput struct {
	CompilationJobName string `json:"CompilationJobName"`
}

func (h *Handler) handleDescribeCompilationJob(ctx context.Context, body []byte) ([]byte, error) {
	var req describeCompilationJobInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CompilationJobName == "" {
		return nil, fmt.Errorf("%w: CompilationJobName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeCompilationJob(ctx, req.CompilationJobName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

// deleteCompilationJobInput mirrors DeleteCompilationJobInput
// (api_op_DeleteCompilationJob.go:27-32).
type deleteCompilationJobInput struct {
	CompilationJobName string `json:"CompilationJobName"`
}

func (h *Handler) handleDeleteCompilationJob(ctx context.Context, body []byte) error {
	var req deleteCompilationJobInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CompilationJobName == "" {
		return fmt.Errorf("%w: CompilationJobName is required", errInvalidRequest)
	}

	return h.Backend.DeleteCompilationJob(ctx, req.CompilationJobName)
}

// stopCompilationJobInput mirrors StopCompilationJobInput
// (api_op_StopCompilationJob.go:27-32).
type stopCompilationJobInput struct {
	CompilationJobName string `json:"CompilationJobName"`
}

func (h *Handler) handleStopCompilationJob(ctx context.Context, body []byte) error {
	var req stopCompilationJobInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CompilationJobName == "" {
		return fmt.Errorf("%w: CompilationJobName is required", errInvalidRequest)
	}

	return h.Backend.StopCompilationJob(ctx, req.CompilationJobName)
}

// listCompilationJobsInput mirrors ListCompilationJobsInput
// (api_op_ListCompilationJobs.go:33-73).
type listCompilationJobsInput struct {
	CreationTimeAfter      *float64 `json:"CreationTimeAfter"`
	CreationTimeBefore     *float64 `json:"CreationTimeBefore"`
	LastModifiedTimeAfter  *float64 `json:"LastModifiedTimeAfter"`
	LastModifiedTimeBefore *float64 `json:"LastModifiedTimeBefore"`
	NameContains           string   `json:"NameContains"`
	StatusEquals           string   `json:"StatusEquals"`
	NextToken              string   `json:"NextToken"`
	SortBy                 string   `json:"SortBy"`
	SortOrder              string   `json:"SortOrder"`
	MaxResults             int32    `json:"MaxResults"`
}

func (h *Handler) handleListCompilationJobs(ctx context.Context, body []byte) ([]byte, error) {
	var req listCompilationJobsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListCompilationJobs(ctx, req.NextToken, ListCompilationJobsFilter{
		CreationTimeAfter:      epochPtr(req.CreationTimeAfter),
		CreationTimeBefore:     epochPtr(req.CreationTimeBefore),
		LastModifiedTimeAfter:  epochPtr(req.LastModifiedTimeAfter),
		LastModifiedTimeBefore: epochPtr(req.LastModifiedTimeBefore),
		NameContains:           req.NameContains,
		StatusEquals:           req.StatusEquals,
		SortBy:                 req.SortBy,
		SortOrder:              req.SortOrder,
		MaxResults:             req.MaxResults,
	})

	summaries := make([]map[string]any, 0, len(items))
	for _, j := range items {
		summary := map[string]any{
			"CompilationJobName":   j.CompilationJobName,
			"CompilationJobArn":    j.CompilationJobArn,
			"CompilationJobStatus": j.CompilationJobStatus,
			keyCreationTime:        epochSeconds(j.CreationTime),
			keyLastModifiedTime:    epochSeconds(j.LastModifiedTime),
		}

		if j.OutputConfig != nil && j.OutputConfig.TargetDevice != "" {
			summary["CompilationTargetDevice"] = j.OutputConfig.TargetDevice
		}

		if j.CompilationStartTime != nil {
			summary["CompilationStartTime"] = epochSeconds(*j.CompilationStartTime)
		}

		if j.CompilationEndTime != nil {
			summary["CompilationEndTime"] = epochSeconds(*j.CompilationEndTime)
		}

		summaries = append(summaries, summary)
	}

	return json.Marshal(map[string]any{
		"CompilationJobSummaries": summaries,
		keyNextToken:              next,
	})
}
