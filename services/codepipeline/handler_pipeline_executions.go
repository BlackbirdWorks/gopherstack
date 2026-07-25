package codepipeline

import (
	"context"
	"fmt"
)

const (
	// maxResultsCapPipelineExecutions is the per-operation pagination cap for ListPipelineExecutions.
	maxResultsCapPipelineExecutions int32 = 100
	// maxResultsCapActionExecutions is the per-operation pagination cap for ListActionExecutions.
	maxResultsCapActionExecutions int32 = 100

	// triggerTypeStartExecution is the default trigger type when no trigger detail is stored.
	triggerTypeStartExecution = "StartPipelineExecution"
)

type startPipelineExecutionInput struct {
	Name string `json:"name"`
}

type pipelineExecutionOutput struct {
	PipelineExecutionID string `json:"pipelineExecutionId"`
}

func (h *Handler) handleStartPipelineExecution(
	ctx context.Context,
	in *startPipelineExecutionInput,
) (*pipelineExecutionOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	exec, err := h.Backend.StartPipelineExecution(ctx, in.Name)
	if err != nil {
		return nil, err
	}

	return &pipelineExecutionOutput{PipelineExecutionID: exec.PipelineExecutionID}, nil
}

type getPipelineExecutionInput struct {
	PipelineName        string `json:"pipelineName"`
	PipelineExecutionID string `json:"pipelineExecutionId"`
}

type getPipelineExecutionOutput struct {
	PipelineExecution map[string]any `json:"pipelineExecution"`
}

func (h *Handler) handleGetPipelineExecution(
	ctx context.Context,
	in *getPipelineExecutionInput,
) (*getPipelineExecutionOutput, error) {
	if in.PipelineName == "" {
		return nil, fmt.Errorf("%w: pipelineName is required", errInvalidRequest)
	}

	exec, err := h.Backend.GetPipelineExecution(ctx, in.PipelineName, in.PipelineExecutionID)
	if err != nil {
		return nil, err
	}

	return &getPipelineExecutionOutput{PipelineExecution: pipelineExecutionDetail(exec)}, nil
}

// triggerObject builds the ExecutionTrigger wire shape shared by both
// PipelineExecution (GetPipelineExecution) and PipelineExecutionSummary
// (ListPipelineExecutions) -- verified identical in both against
// awsAwsjson11_deserializeDocumentExecutionTrigger in the real SDK's
// deserializers.go.
func triggerObject(trigger string) map[string]any {
	triggerType := trigger
	if triggerType == "" {
		triggerType = triggerTypeStartExecution
	}

	return map[string]any{"triggerType": triggerType, "triggerDetail": ""}
}

// rollbackMetadataObject builds the PipelineRollbackMetadata wire shape,
// included only for ROLLBACK-type executions (RollbackStage,
// pipeline_state.go); real AWS omits it entirely for STANDARD runs.
func rollbackMetadataObject(targetExecutionID string) map[string]any {
	return map[string]any{"rollbackTargetPipelineExecutionId": targetExecutionID}
}

// pipelineExecutionDetail builds the PipelineExecution wire shape used by
// GetPipelineExecution's envelope. Real AWS's PipelineExecution shape has NO
// startTime/lastUpdateTime fields at all (verified against
// awsAwsjson11_deserializeDocumentPipelineExecution, which switches only on
// artifactRevisions/executionMode/executionType/pipelineExecutionId/
// pipelineName/pipelineVersion/rollbackMetadata/status/statusSummary/trigger/
// variables) -- those exist only on the DIFFERENT PipelineExecutionSummary
// shape used by ListPipelineExecutions, see pipelineExecutionSummary below.
// An earlier revision of this function incorrectly added them here too.
func pipelineExecutionDetail(exec *PipelineExecution) map[string]any {
	out := map[string]any{
		"pipelineName":        exec.PipelineName,
		"pipelineExecutionId": exec.PipelineExecutionID,
		keyStatus:             exec.Status,
		"pipelineVersion":     exec.PipelineVersion,
		"executionMode":       exec.ExecutionMode,
		"executionType":       exec.ExecutionType,
		"trigger":             triggerObject(exec.Trigger),
	}

	if exec.RollbackTargetExecutionID != "" {
		out["rollbackMetadata"] = rollbackMetadataObject(exec.RollbackTargetExecutionID)
	}

	return out
}

// pipelineExecutionSummary builds the PipelineExecutionSummary wire shape for
// ListPipelineExecutions. Real AWS's PipelineExecutionSummary has NO
// pipelineName/pipelineVersion fields (verified against
// awsAwsjson11_deserializeDocumentPipelineExecutionSummary, which switches
// only on executionMode/executionType/lastUpdateTime/pipelineExecutionId/
// rollbackMetadata/sourceRevisions/startTime/status/statusSummary/
// stopTrigger/trigger) but DOES have startTime/lastUpdateTime as
// epoch-seconds numbers, unlike the detail shape above.
func pipelineExecutionSummary(exec *PipelineExecution) map[string]any {
	out := map[string]any{
		keyPipelineExecutionID: exec.PipelineExecutionID,
		keyStatus:              exec.Status,
		"executionMode":        exec.ExecutionMode,
		"executionType":        exec.ExecutionType,
		"trigger":              triggerObject(exec.Trigger),
	}

	if !exec.StartTime.IsZero() {
		out["startTime"] = float64(exec.StartTime.Unix())
	}

	if !exec.LastUpdateTime.IsZero() {
		out["lastUpdateTime"] = float64(exec.LastUpdateTime.Unix())
	}

	if exec.RollbackTargetExecutionID != "" {
		out["rollbackMetadata"] = rollbackMetadataObject(exec.RollbackTargetExecutionID)
	}

	return out
}

type stopPipelineExecutionInput struct {
	PipelineName        string `json:"pipelineName"`
	PipelineExecutionID string `json:"pipelineExecutionId"`
	Reason              string `json:"reason"`
	Abandon             bool   `json:"abandon"`
}

func (h *Handler) handleStopPipelineExecution(
	ctx context.Context,
	in *stopPipelineExecutionInput,
) (*pipelineExecutionOutput, error) {
	if in.PipelineName == "" {
		return nil, fmt.Errorf("%w: pipelineName is required", errInvalidRequest)
	}

	exec, err := h.Backend.StopPipelineExecution(
		ctx, in.PipelineName, in.PipelineExecutionID, in.Reason, in.Abandon,
	)
	if err != nil {
		return nil, err
	}

	return &pipelineExecutionOutput{PipelineExecutionID: exec.PipelineExecutionID}, nil
}

type listPipelineExecutionsInput struct {
	PipelineName string `json:"pipelineName"`
	NextToken    string `json:"nextToken"`
	MaxResults   int32  `json:"maxResults"`
}

type listPipelineExecutionsOutput struct {
	NextToken                  string           `json:"nextToken,omitempty"`
	PipelineExecutionSummaries []map[string]any `json:"pipelineExecutionSummaries"`
}

func (h *Handler) handleListPipelineExecutions(
	ctx context.Context,
	in *listPipelineExecutionsInput,
) (*listPipelineExecutionsOutput, error) {
	if in.PipelineName == "" {
		return nil, fmt.Errorf("%w: pipelineName is required", errInvalidRequest)
	}

	execs, err := h.Backend.ListPipelineExecutions(ctx, in.PipelineName)
	if err != nil {
		return nil, err
	}

	items := make([]map[string]any, len(execs))
	for i := range execs {
		items[i] = pipelineExecutionSummary(&execs[i])
	}

	page, nextToken, err := cpPaginate(
		items,
		in.NextToken,
		in.MaxResults,
		maxResultsCapPipelineExecutions,
	)
	if err != nil {
		return nil, err
	}

	return &listPipelineExecutionsOutput{
		NextToken:                  nextToken,
		PipelineExecutionSummaries: page,
	}, nil
}

type actionExecutionFilter struct {
	PipelineExecutionID string `json:"pipelineExecutionId"`
}

type listActionExecutionsInput struct {
	Filter       *actionExecutionFilter `json:"filter"`
	PipelineName string                 `json:"pipelineName"`
	NextToken    string                 `json:"nextToken"`
	MaxResults   int32                  `json:"maxResults"`
}

type listActionExecutionsOutput struct {
	NextToken              string           `json:"nextToken,omitempty"`
	ActionExecutionDetails []map[string]any `json:"actionExecutionDetails"`
}

func (h *Handler) handleListActionExecutions(
	ctx context.Context,
	in *listActionExecutionsInput,
) (*listActionExecutionsOutput, error) {
	if in.PipelineName == "" {
		return nil, fmt.Errorf("%w: pipelineName is required", errInvalidRequest)
	}

	var execFilter string
	if in.Filter != nil {
		execFilter = in.Filter.PipelineExecutionID
	}

	items, err := h.Backend.ListActionExecutions(ctx, in.PipelineName, execFilter)
	if err != nil {
		return nil, err
	}

	page, nextToken, pErr := cpPaginate(
		items,
		in.NextToken,
		in.MaxResults,
		maxResultsCapActionExecutions,
	)
	if pErr != nil {
		return nil, pErr
	}

	return &listActionExecutionsOutput{NextToken: nextToken, ActionExecutionDetails: page}, nil
}

type listDeployActionExecutionTargetsInput struct {
	PipelineName      string `json:"pipelineName"`
	ActionExecutionID string `json:"actionExecutionId"`
	NextToken         string `json:"nextToken"`
	MaxResults        int32  `json:"maxResults"`
}

type listDeployActionExecutionTargetsOutput struct {
	Targets []map[string]any `json:"targets"`
}

func (h *Handler) handleListDeployActionExecutionTargets(
	ctx context.Context,
	in *listDeployActionExecutionTargetsInput,
) (*listDeployActionExecutionTargetsOutput, error) {
	if in.PipelineName == "" {
		return nil, fmt.Errorf("%w: pipelineName is required", errInvalidRequest)
	}

	items, err := h.Backend.ListDeployActionExecutionTargets(ctx, in.PipelineName, in.ActionExecutionID)
	if err != nil {
		return nil, err
	}

	return &listDeployActionExecutionTargetsOutput{Targets: items}, nil
}
