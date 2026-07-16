package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// ---------------------------------------------------------------------------
// Pipeline ops handlers (#29) — wire real backend to stub-bypassed ops
// ---------------------------------------------------------------------------

func (h *Handler) handleRetryPipelineExecution(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		PipelineExecutionArn string `json:"PipelineExecutionArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.PipelineExecutionArn == "" {
		return nil, fmt.Errorf("%w: PipelineExecutionArn is required", errInvalidRequest)
	}

	exec, err := h.Backend.RetryPipelineExecution(ctx, req.PipelineExecutionArn)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: retried pipeline execution", "arn", exec.PipelineExecutionArn)

	return json.Marshal(map[string]string{keyPipelineExecutionArn: exec.PipelineExecutionArn})
}

func (h *Handler) handleStopPipelineExecution(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		PipelineExecutionArn string `json:"PipelineExecutionArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.PipelineExecutionArn == "" {
		return nil, fmt.Errorf("%w: PipelineExecutionArn is required", errInvalidRequest)
	}

	exec, err := h.Backend.StopPipelineExecution(ctx, req.PipelineExecutionArn)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: stopped pipeline execution", "arn", exec.PipelineExecutionArn)

	return json.Marshal(map[string]string{keyPipelineExecutionArn: exec.PipelineExecutionArn})
}

// handleSendPipelineExecutionStepSuccess handles the AWS callback token API.
// AWS uses CallbackToken (opaque) rather than PipelineExecutionArn+StepName.
// We use the token as both the step key and (by convention) accept execArn if provided.
func (h *Handler) handleSendPipelineExecutionStepSuccess(
	ctx context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		PipelineExecutionArn string `json:"PipelineExecutionArn"`
		CallbackToken        string `json:"CallbackToken"`
		StepName             string `json:"StepName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	execArn := req.PipelineExecutionArn
	if execArn == "" {
		execArn = req.CallbackToken
	}
	stepName := req.StepName
	if stepName == "" {
		stepName = "callback-step"
	}

	// Propagate error when execArn is known; be lenient when it's empty
	// (callback token may reference executions from before this session).
	if execArn != "" {
		if err := h.Backend.SendPipelineExecutionStepSuccess(ctx, execArn, stepName); err != nil {
			return nil, err
		}
	} else {
		_ = h.Backend.SendPipelineExecutionStepSuccess(ctx, execArn, stepName)
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: sent pipeline step success", "token", execArn)

	return json.Marshal(map[string]string{keyPipelineExecutionArn: execArn})
}

// handleSendPipelineExecutionStepFailure handles the AWS callback token API.
func (h *Handler) handleSendPipelineExecutionStepFailure(
	ctx context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		PipelineExecutionArn string `json:"PipelineExecutionArn"`
		CallbackToken        string `json:"CallbackToken"`
		StepName             string `json:"StepName"`
		FailureReason        string `json:"FailureReason,omitempty"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	execArn := req.PipelineExecutionArn
	if execArn == "" {
		execArn = req.CallbackToken
	}
	stepName := req.StepName
	if stepName == "" {
		stepName = "callback-step"
	}

	// Propagate error when execArn is known; be lenient when it's empty (stale callback token).
	if execArn != "" {
		if err := h.Backend.SendPipelineExecutionStepFailure(ctx, execArn, stepName, req.FailureReason); err != nil {
			return nil, err
		}
	} else {
		_ = h.Backend.SendPipelineExecutionStepFailure(ctx, execArn, stepName, req.FailureReason)
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: sent pipeline step failure", "token", execArn)

	return json.Marshal(map[string]string{keyPipelineExecutionArn: execArn})
}

type pipelineExecStepSummary struct {
	StepName      string  `json:"StepName"`
	StepType      string  `json:"StepType,omitempty"`
	StepStatus    string  `json:"StepStatus"`
	FailureReason string  `json:"FailureReason,omitempty"`
	StartTime     float64 `json:"StartTime,omitempty"`
	EndTime       float64 `json:"EndTime,omitempty"`
}

func (h *Handler) handleListPipelineExecutionSteps(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		PipelineExecutionArn string `json:"PipelineExecutionArn"`
		NextToken            string `json:"NextToken"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.PipelineExecutionArn == "" {
		return nil, fmt.Errorf("%w: PipelineExecutionArn is required", errInvalidRequest)
	}

	// Verify execution exists before listing steps.
	if _, err := h.Backend.DescribePipelineExecution(ctx, req.PipelineExecutionArn); err != nil {
		return nil, err
	}

	steps, nextToken := h.Backend.ListPipelineExecutionSteps(
		ctx,
		req.PipelineExecutionArn,
		req.NextToken,
	)
	summaries := make([]pipelineExecStepSummary, 0, len(steps))
	for _, s := range steps {
		sum := pipelineExecStepSummary{
			StepName:      s.StepName,
			StepType:      s.StepType,
			StepStatus:    s.StepStatus,
			FailureReason: s.FailureReason,
			StartTime:     epochSeconds(s.StartTime),
		}
		if !s.EndTime.IsZero() {
			sum.EndTime = epochSeconds(s.EndTime)
		}
		summaries = append(summaries, sum)
	}

	resp := map[string]any{"PipelineExecutionSteps": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

// ---------------------------------------------------------------------------
// Extended CreatePipeline/UpdatePipeline/StartPipelineExecution handlers (gaps #23, #25)
// ---------------------------------------------------------------------------

func (h *Handler) handleCreatePipelineFull(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ParallelismConfiguration *ParallelismConfiguration `json:"ParallelismConfiguration,omitempty"`
		PipelineName             string                    `json:"PipelineName"`
		PipelineDefinition       string                    `json:"PipelineDefinition,omitempty"`
		PipelineDisplayName      string                    `json:"PipelineDisplayName,omitempty"`
		PipelineDescription      string                    `json:"PipelineDescription,omitempty"`
		RoleArn                  string                    `json:"RoleArn,omitempty"`
		Tags                     []tagObject               `json:"Tags,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PipelineName == "" {
		return nil, fmt.Errorf("%w: PipelineName is required", errInvalidRequest)
	}

	p, err := h.Backend.CreatePipelineFull(ctx, CreatePipelineOptions{
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
		ctx,
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
		ParallelismConfiguration     *ParallelismConfiguration `json:"ParallelismConfiguration,omitempty"`
		PipelineName                 string                    `json:"PipelineName"`
		PipelineExecutionDisplayName string                    `json:"PipelineExecutionDisplayName,omitempty"`
		PipelineExecutionDescription string                    `json:"PipelineExecutionDescription,omitempty"`
		PipelineParameters           []PipelineParameter       `json:"PipelineParameters,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PipelineName == "" {
		return nil, fmt.Errorf("%w: PipelineName is required", errInvalidRequest)
	}

	pe, err := h.Backend.StartPipelineExecutionFull(ctx, StartPipelineExecutionOptions{
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

// ---------------------------------------------------------------------------
// ListPipelineParametersForExecution — gap #25
// ---------------------------------------------------------------------------

func (h *Handler) handleListPipelineParametersForExecution(
	ctx context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		PipelineExecutionArn string `json:"PipelineExecutionArn"`
		NextToken            string `json:"NextToken,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PipelineExecutionArn == "" {
		return nil, fmt.Errorf("%w: PipelineExecutionArn is required", errInvalidRequest)
	}

	pe, err := h.Backend.DescribePipelineExecution(ctx, req.PipelineExecutionArn)
	if err != nil {
		return nil, err
	}

	params := pe.PipelineParameters
	if params == nil {
		params = []PipelineParameter{}
	}

	return json.Marshal(map[string]any{"PipelineParameters": params})
}

// ---------------------------------------------------------------------------
// Pipeline handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleDescribePipeline(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		PipelineName string `json:"PipelineName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PipelineName == "" {
		return nil, fmt.Errorf("%w: PipelineName is required", errInvalidRequest)
	}

	p, err := h.Backend.DescribePipeline(ctx, req.PipelineName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"PipelineName":       p.PipelineName,
		keyPipelineArn:       p.PipelineArn,
		"PipelineStatus":     p.PipelineStatus,
		"PipelineDefinition": p.PipelineDefinition,
		keyRoleArn:           p.RoleArn,
		keyCreationTime:      epochSeconds(p.CreationTime),
		keyLastModifiedTime:  epochSeconds(p.LastModifiedTime),
	}
	if p.PipelineDisplayName != "" {
		resp["PipelineDisplayName"] = p.PipelineDisplayName
	}
	if p.PipelineDescription != "" {
		resp["PipelineDescription"] = p.PipelineDescription
	}
	if p.ParallelismConfiguration != nil {
		resp["ParallelismConfiguration"] = p.ParallelismConfiguration
	}

	return json.Marshal(resp)
}

type pipelineSummary struct {
	PipelineName     string  `json:"PipelineName"`
	PipelineArn      string  `json:"PipelineArn"`
	PipelineStatus   string  `json:"PipelineStatus"`
	CreationTime     float64 `json:"CreationTime"`
	LastModifiedTime float64 `json:"LastModifiedTime"`
}

func (h *Handler) handleListPipelines(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	ps, nextToken := h.Backend.ListPipelines(ctx, req.NextToken)
	summaries := make([]pipelineSummary, 0, len(ps))

	for _, p := range ps {
		summaries = append(summaries, pipelineSummary{
			PipelineName:     p.PipelineName,
			PipelineArn:      p.PipelineArn,
			PipelineStatus:   p.PipelineStatus,
			CreationTime:     epochSeconds(p.CreationTime),
			LastModifiedTime: epochSeconds(p.LastModifiedTime),
		})
	}

	resp := map[string]any{"PipelineSummaries": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleDeletePipeline(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		PipelineName string `json:"PipelineName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PipelineName == "" {
		return nil, fmt.Errorf("%w: PipelineName is required", errInvalidRequest)
	}

	p, err := h.Backend.DeletePipeline(ctx, req.PipelineName)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: deleted pipeline", "name", req.PipelineName)

	return json.Marshal(map[string]string{keyPipelineArn: p.PipelineArn})
}

func (h *Handler) handleDescribePipelineExecution(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		PipelineExecutionArn string `json:"PipelineExecutionArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PipelineExecutionArn == "" {
		return nil, fmt.Errorf("%w: PipelineExecutionArn is required", errInvalidRequest)
	}

	pe, err := h.Backend.DescribePipelineExecution(ctx, req.PipelineExecutionArn)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		keyPipelineArn:             pe.PipelineArn,
		keyPipelineExecutionArn:    pe.PipelineExecutionArn,
		keyPipelineExecutionStatus: pe.PipelineExecutionStatus,
		"StartTime":                epochSeconds(pe.StartTime),
	}
	if pe.PipelineExecutionDisplayName != "" {
		resp["PipelineExecutionDisplayName"] = pe.PipelineExecutionDisplayName
	}
	if pe.PipelineExecutionDescription != "" {
		resp["PipelineExecutionDescription"] = pe.PipelineExecutionDescription
	}
	if len(pe.PipelineParameters) > 0 {
		resp["PipelineParameters"] = pe.PipelineParameters
	}
	if pe.FailureReason != "" {
		resp["FailureReason"] = pe.FailureReason
	}

	return json.Marshal(resp)
}

type pipelineExecutionSummary struct {
	PipelineExecutionArn    string  `json:"PipelineExecutionArn"`
	PipelineExecutionStatus string  `json:"PipelineExecutionStatus"`
	StartTime               float64 `json:"StartTime"`
}

func (h *Handler) handleListPipelineExecutions(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		PipelineName string `json:"PipelineName"`
		NextToken    string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PipelineName == "" {
		return nil, fmt.Errorf("%w: PipelineName is required", errInvalidRequest)
	}

	pes, nextToken := h.Backend.ListPipelineExecutions(ctx, req.PipelineName, req.NextToken)
	summaries := make([]pipelineExecutionSummary, 0, len(pes))

	for _, pe := range pes {
		summaries = append(summaries, pipelineExecutionSummary{
			PipelineExecutionArn:    pe.PipelineExecutionArn,
			PipelineExecutionStatus: pe.PipelineExecutionStatus,
			StartTime:               epochSeconds(pe.StartTime),
		})
	}

	resp := map[string]any{"PipelineExecutionSummaries": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}
