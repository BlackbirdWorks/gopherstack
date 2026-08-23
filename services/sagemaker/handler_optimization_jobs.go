package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// OptimizationJob handlers
// ---------------------------------------------------------------------------

// createOptimizationJobInput mirrors CreateOptimizationJobInput
// (api_op_CreateOptimizationJob.go:48-114). ModelSource/OptimizationConfigs/
// OutputConfig/VpcConfig are decoded as json.RawMessage — see
// OptimizationJob's doc comment in optimization_jobs.go for why.
type createOptimizationJobInput struct {
	ModelSource             json.RawMessage    `json:"ModelSource"`
	OptimizationConfigs     json.RawMessage    `json:"OptimizationConfigs"`
	OutputConfig            json.RawMessage    `json:"OutputConfig"`
	VpcConfig               json.RawMessage    `json:"VpcConfig"`
	StoppingCondition       *StoppingCondition `json:"StoppingCondition"`
	OptimizationEnvironment map[string]string  `json:"OptimizationEnvironment"`
	OptimizationJobName     string             `json:"OptimizationJobName"`
	RoleArn                 string             `json:"RoleArn"`
	DeploymentInstanceType  string             `json:"DeploymentInstanceType"`
	TrainingPlanArns        []string           `json:"TrainingPlanArns"`
	Tags                    []tagObject        `json:"Tags"`
	MaxInstanceCount        int32              `json:"MaxInstanceCount"`
}

func (h *Handler) handleCreateOptimizationJob(ctx context.Context, body []byte) ([]byte, error) {
	var req createOptimizationJobInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.OptimizationJobName == "" {
		return nil, fmt.Errorf("%w: OptimizationJobName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateOptimizationJob(ctx, CreateOptimizationJobOptions{
		Name:                    req.OptimizationJobName,
		RoleArn:                 req.RoleArn,
		DeploymentInstanceType:  req.DeploymentInstanceType,
		ModelSource:             req.ModelSource,
		OptimizationConfigs:     req.OptimizationConfigs,
		OutputConfig:            req.OutputConfig,
		VpcConfig:               req.VpcConfig,
		StoppingCondition:       req.StoppingCondition,
		MaxInstanceCount:        req.MaxInstanceCount,
		OptimizationEnvironment: req.OptimizationEnvironment,
		TrainingPlanArns:        req.TrainingPlanArns,
		Tags:                    fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyOptimizationJobArn: result.OptimizationJobArn})
}

// describeOptimizationJobInput mirrors DescribeOptimizationJobInput
// (api_op_DescribeOptimizationJob.go:29-34).
type describeOptimizationJobInput struct {
	OptimizationJobName string `json:"OptimizationJobName"`
}

// optimizationJobResponseMap builds the AWS wire representation of an
// OptimizationJob's DescribeOptimizationJobOutput. OptimizationOutput/
// OptimizationStartTime/OptimizationEndTime are disclosed not modeled: this
// backend never simulates an actual optimization run (Create completes
// synchronously with no server-derived result to report).
func optimizationJobResponseMap(j *OptimizationJob) map[string]any {
	resp := map[string]any{
		"OptimizationJobName":    j.OptimizationJobName,
		"OptimizationJobArn":     j.OptimizationJobArn,
		"OptimizationJobStatus":  j.OptimizationJobStatus,
		"RoleArn":                j.RoleArn,
		"DeploymentInstanceType": j.DeploymentInstanceType,
		keyCreationTime:          epochSeconds(j.CreationTime),
		keyLastModifiedTime:      epochSeconds(j.LastModifiedTime),
	}

	if len(j.ModelSource) > 0 {
		resp["ModelSource"] = j.ModelSource
	}

	if len(j.OptimizationConfigs) > 0 {
		resp["OptimizationConfigs"] = j.OptimizationConfigs
	}

	if len(j.OutputConfig) > 0 {
		resp["OutputConfig"] = j.OutputConfig
	}

	if len(j.VpcConfig) > 0 {
		resp["VpcConfig"] = j.VpcConfig
	}

	if j.StoppingCondition != nil {
		resp["StoppingCondition"] = j.StoppingCondition
	}

	if j.MaxInstanceCount > 0 {
		resp["MaxInstanceCount"] = j.MaxInstanceCount
	}

	if len(j.OptimizationEnvironment) > 0 {
		resp["OptimizationEnvironment"] = j.OptimizationEnvironment
	}

	if len(j.TrainingPlanArns) > 0 {
		resp["TrainingPlanArns"] = j.TrainingPlanArns
	}

	return resp
}

func (h *Handler) handleDescribeOptimizationJob(ctx context.Context, body []byte) ([]byte, error) {
	var req describeOptimizationJobInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.OptimizationJobName == "" {
		return nil, fmt.Errorf("%w: OptimizationJobName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeOptimizationJob(ctx, req.OptimizationJobName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(optimizationJobResponseMap(result))
}

// deleteOptimizationJobInput mirrors DeleteOptimizationJobInput
// (api_op_DeleteOptimizationJob.go:27-32).
type deleteOptimizationJobInput struct {
	OptimizationJobName string `json:"OptimizationJobName"`
}

func (h *Handler) handleDeleteOptimizationJob(ctx context.Context, body []byte) error {
	var req deleteOptimizationJobInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.OptimizationJobName == "" {
		return fmt.Errorf("%w: OptimizationJobName is required", errInvalidRequest)
	}

	return h.Backend.DeleteOptimizationJob(ctx, req.OptimizationJobName)
}

// stopOptimizationJobInput mirrors StopOptimizationJobInput
// (api_op_StopOptimizationJob.go:27-32).
type stopOptimizationJobInput struct {
	OptimizationJobName string `json:"OptimizationJobName"`
}

func (h *Handler) handleStopOptimizationJob(ctx context.Context, body []byte) error {
	var req stopOptimizationJobInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.OptimizationJobName == "" {
		return fmt.Errorf("%w: OptimizationJobName is required", errInvalidRequest)
	}

	return h.Backend.StopOptimizationJob(ctx, req.OptimizationJobName)
}

// listOptimizationJobsInput mirrors ListOptimizationJobsInput
// (api_op_ListOptimizationJobs.go:30-72).
type listOptimizationJobsInput struct {
	CreationTimeAfter      *float64 `json:"CreationTimeAfter,omitempty"`
	CreationTimeBefore     *float64 `json:"CreationTimeBefore,omitempty"`
	LastModifiedTimeAfter  *float64 `json:"LastModifiedTimeAfter,omitempty"`
	LastModifiedTimeBefore *float64 `json:"LastModifiedTimeBefore,omitempty"`
	NextToken              string   `json:"NextToken"`
	NameContains           string   `json:"NameContains"`
	OptimizationContains   string   `json:"OptimizationContains"`
	StatusEquals           string   `json:"StatusEquals"`
	SortBy                 string   `json:"SortBy"`
	SortOrder              string   `json:"SortOrder"`
	MaxResults             int32    `json:"MaxResults"`
}

func (h *Handler) handleListOptimizationJobs(ctx context.Context, body []byte) ([]byte, error) {
	var req listOptimizationJobsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	jobs, nextToken := h.Backend.ListOptimizationJobs(ctx, req.NextToken, ListOptimizationJobsFilter{
		NameContains:           req.NameContains,
		OptimizationContains:   req.OptimizationContains,
		StatusEquals:           req.StatusEquals,
		SortBy:                 req.SortBy,
		SortOrder:              req.SortOrder,
		MaxResults:             req.MaxResults,
		CreationTimeAfter:      timeFromEpochSecondsPtr(req.CreationTimeAfter),
		CreationTimeBefore:     timeFromEpochSecondsPtr(req.CreationTimeBefore),
		LastModifiedTimeAfter:  timeFromEpochSecondsPtr(req.LastModifiedTimeAfter),
		LastModifiedTimeBefore: timeFromEpochSecondsPtr(req.LastModifiedTimeBefore),
	})

	items := make([]map[string]any, 0, len(jobs))
	for _, j := range jobs {
		items = append(items, map[string]any{
			"OptimizationJobName":    j.OptimizationJobName,
			"OptimizationJobArn":     j.OptimizationJobArn,
			"OptimizationJobStatus":  j.OptimizationJobStatus,
			"DeploymentInstanceType": j.DeploymentInstanceType,
			"OptimizationTypes":      optimizationTypesOf(j.OptimizationConfigs),
			keyCreationTime:          epochSeconds(j.CreationTime),
			keyLastModifiedTime:      epochSeconds(j.LastModifiedTime),
		})
	}

	return listResp("OptimizationJobSummaries", items, nextToken)
}
