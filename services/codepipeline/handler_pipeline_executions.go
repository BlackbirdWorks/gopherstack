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

	return &getPipelineExecutionOutput{
		PipelineExecution: map[string]any{
			"pipelineName":        exec.PipelineName,
			"pipelineExecutionId": exec.PipelineExecutionID,
			keyStatus:             exec.Status,
			"pipelineVersion":     exec.PipelineVersion,
		},
	}, nil
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
	for i, e := range execs {
		triggerType := e.Trigger
		if triggerType == "" {
			triggerType = triggerTypeStartExecution
		}

		items[i] = map[string]any{
			"pipelineExecutionId": e.PipelineExecutionID,
			"status":              e.Status,
			"pipelineVersion":     e.PipelineVersion,
			"trigger": map[string]any{
				"triggerType":   triggerType,
				"triggerDetail": "",
			},
		}
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
