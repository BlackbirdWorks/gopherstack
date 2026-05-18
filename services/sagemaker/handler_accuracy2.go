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

func (h *Handler) handleCreateTransformJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Environment             map[string]string  `json:"Environment,omitempty"`
		TransformInput          TransformInput     `json:"TransformInput"`
		TransformOutput         TransformOutput    `json:"TransformOutput"`
		TransformResources      TransformResources `json:"TransformResources"`
		Tags                    []tagObject        `json:"Tags,omitempty"`
		TransformJobName        string             `json:"TransformJobName"`
		ModelName               string             `json:"ModelName"`
		RoleArn                 string             `json:"RoleArn,omitempty"`
		BatchStrategy           string             `json:"BatchStrategy,omitempty"`
		MaxConcurrentTransforms int32              `json:"MaxConcurrentTransforms,omitempty"`
		MaxPayloadInMB          int32              `json:"MaxPayloadInMB,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	tj, err := h.Backend.CreateTransformJob(TransformJobOptions{
		TransformJobName:        req.TransformJobName,
		ModelName:               req.ModelName,
		RoleArn:                 req.RoleArn,
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

func (h *Handler) handleDescribeTransformJob(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		TransformJobName string `json:"TransformJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TransformJobName == "" {
		return nil, fmt.Errorf("%w: TransformJobName is required", errInvalidRequest)
	}

	tj, err := h.Backend.DescribeTransformJob(req.TransformJobName)
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
	if tj.RoleArn != "" {
		resp[keyRoleArn] = tj.RoleArn
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

func (h *Handler) handleStopTransformJob(ctx context.Context, body []byte) error {
	var req struct {
		TransformJobName string `json:"TransformJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TransformJobName == "" {
		return fmt.Errorf("%w: TransformJobName is required", errInvalidRequest)
	}

	if err := h.Backend.StopTransformJob(req.TransformJobName); err != nil {
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

func (h *Handler) handleListTransformJobs(body []byte) ([]byte, error) {
	var req struct {
		NextToken    string `json:"NextToken"`
		StatusEquals string `json:"StatusEquals,omitempty"`
		NameContains string `json:"NameContains,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	filter := ListTransformJobsFilter{
		StatusEquals: req.StatusEquals,
		NameContains: req.NameContains,
	}

	jobs, nextToken := h.Backend.ListTransformJobs(req.NextToken, filter)
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

// ---------------------------------------------------------------------------
// UpdateFeatureGroup handler (gap #19)
// ---------------------------------------------------------------------------

func (h *Handler) handleUpdateFeatureGroup(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		FeatureDefinitions []FeatureDefinition `json:"FeatureAdditions,omitempty"`
		FeatureGroupName   string              `json:"FeatureGroupName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.FeatureGroupName == "" {
		return nil, fmt.Errorf("%w: FeatureGroupName is required", errInvalidRequest)
	}

	fg, err := h.Backend.UpdateFeatureGroup(req.FeatureGroupName, req.FeatureDefinitions)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: updated feature group", "name", fg.FeatureGroupName)

	return json.Marshal(map[string]string{keyFeatureGroupArn: fg.FeatureGroupArn})
}

// ---------------------------------------------------------------------------
// UpdateExperiment, UpdateTrial, UpdateTrialComponent handlers (gap #26)
// ---------------------------------------------------------------------------

func (h *Handler) handleUpdateExperiment(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ExperimentName string `json:"ExperimentName"`
		DisplayName    string `json:"DisplayName,omitempty"`
		Description    string `json:"Description,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ExperimentName == "" {
		return nil, fmt.Errorf("%w: ExperimentName is required", errInvalidRequest)
	}

	e, err := h.Backend.UpdateExperiment(req.ExperimentName, req.DisplayName, req.Description)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: updated experiment", "name", e.ExperimentName)

	return json.Marshal(map[string]string{keyExperimentArn: e.ExperimentArn})
}

func (h *Handler) handleUpdateTrial(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		TrialName   string `json:"TrialName"`
		DisplayName string `json:"DisplayName,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrialName == "" {
		return nil, fmt.Errorf("%w: TrialName is required", errInvalidRequest)
	}

	t, err := h.Backend.UpdateTrial(req.TrialName, req.DisplayName)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: updated trial", "name", t.TrialName)

	return json.Marshal(map[string]string{keyTrialArn: t.TrialArn})
}

func (h *Handler) handleUpdateTrialComponent(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Parameters      map[string]TrialComponentValue    `json:"Parameters,omitempty"`
		InputArtifacts  map[string]TrialComponentArtifact `json:"InputArtifacts,omitempty"`
		OutputArtifacts map[string]TrialComponentArtifact `json:"OutputArtifacts,omitempty"`
		TrialComponentName string                         `json:"TrialComponentName"`
		DisplayName        string                         `json:"DisplayName,omitempty"`
		Status             string                         `json:"Status,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrialComponentName == "" {
		return nil, fmt.Errorf("%w: TrialComponentName is required", errInvalidRequest)
	}

	opts := UpdateTrialComponentOptions{
		DisplayName:     req.DisplayName,
		Status:          req.Status,
		Parameters:      req.Parameters,
		InputArtifacts:  req.InputArtifacts,
		OutputArtifacts: req.OutputArtifacts,
	}

	tc, err := h.Backend.UpdateTrialComponent(req.TrialComponentName, opts)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(
		ctx,
		"sagemaker: updated trial component",
		"name",
		tc.TrialComponentName,
	)

	return json.Marshal(map[string]string{keyTrialComponentArn: tc.TrialComponentArn})
}

// ---------------------------------------------------------------------------
// Extended CreatePipeline/UpdatePipeline/StartPipelineExecution handlers (gaps #23, #25)
// ---------------------------------------------------------------------------

func (h *Handler) handleCreatePipelineFull(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags                     []tagObject               `json:"Tags,omitempty"`
		ParallelismConfiguration *ParallelismConfiguration `json:"ParallelismConfiguration,omitempty"`
		PipelineName             string                    `json:"PipelineName"`
		PipelineDefinition       string                    `json:"PipelineDefinition,omitempty"`
		PipelineDisplayName      string                    `json:"PipelineDisplayName,omitempty"`
		PipelineDescription      string                    `json:"PipelineDescription,omitempty"`
		RoleArn                  string                    `json:"RoleArn,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PipelineName == "" {
		return nil, fmt.Errorf("%w: PipelineName is required", errInvalidRequest)
	}

	p, err := h.Backend.CreatePipelineFull(CreatePipelineOptions{
		PipelineName:             req.PipelineName,
		PipelineDefinition:       req.PipelineDefinition,
		PipelineDisplayName:      req.PipelineDisplayName,
		PipelineDescription:      req.PipelineDescription,
		RoleArn:                  req.RoleArn,
		ParallelismConfiguration: req.ParallelismConfiguration,
		Tags:                     fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: created pipeline (full)", "name", p.PipelineName)

	return json.Marshal(map[string]string{keyPipelineArn: p.PipelineArn})
}

func (h *Handler) handleUpdatePipelineFull(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ParallelismConfiguration *ParallelismConfiguration `json:"ParallelismConfiguration,omitempty"`
		PipelineName             string                    `json:"PipelineName"`
		PipelineDefinition       string                    `json:"PipelineDefinition,omitempty"`
		PipelineDisplayName      string                    `json:"PipelineDisplayName,omitempty"`
		PipelineDescription      string                    `json:"PipelineDescription,omitempty"`
		RoleArn                  string                    `json:"RoleArn,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PipelineName == "" {
		return nil, fmt.Errorf("%w: PipelineName is required", errInvalidRequest)
	}

	p, err := h.Backend.UpdatePipelineFull(
		req.PipelineName,
		req.PipelineDefinition,
		req.PipelineDisplayName,
		req.PipelineDescription,
		req.RoleArn,
		req.ParallelismConfiguration,
	)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: updated pipeline (full)", "name", p.PipelineName)

	return json.Marshal(map[string]string{keyPipelineArn: p.PipelineArn})
}

func (h *Handler) handleStartPipelineExecutionFull(
	ctx context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		PipelineParameters           []PipelineParameter       `json:"PipelineParameters,omitempty"`
		ParallelismConfiguration     *ParallelismConfiguration `json:"ParallelismConfiguration,omitempty"`
		PipelineName                 string                    `json:"PipelineName"`
		PipelineExecutionDisplayName string                    `json:"PipelineExecutionDisplayName,omitempty"`
		PipelineExecutionDescription string                    `json:"PipelineExecutionDescription,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PipelineName == "" {
		return nil, fmt.Errorf("%w: PipelineName is required", errInvalidRequest)
	}

	pe, err := h.Backend.StartPipelineExecutionFull(StartPipelineExecutionOptions{
		PipelineName:                 req.PipelineName,
		PipelineExecutionDisplayName: req.PipelineExecutionDisplayName,
		PipelineExecutionDescription: req.PipelineExecutionDescription,
		PipelineParameters:           req.PipelineParameters,
		ParallelismConfiguration:     req.ParallelismConfiguration,
	})
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(
		ctx,
		"sagemaker: started pipeline execution (full)",
		"arn",
		pe.PipelineExecutionArn,
	)

	return json.Marshal(map[string]string{keyPipelineExecutionArn: pe.PipelineExecutionArn})
}
