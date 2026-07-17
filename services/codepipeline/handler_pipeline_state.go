package codepipeline

import (
	"context"
	"fmt"
)

// validTransitionType returns true if t is a valid AWS StageTransitionType value.
func validTransitionType(t string) bool {
	return t == transitionTypeInbound || t == transitionTypeOutbound
}

type getPipelineStateInput struct {
	Name string `json:"name"`
}

type getPipelineStateOutput struct {
	PipelineName    string           `json:"pipelineName"`
	StageStates     []map[string]any `json:"stageStates"`
	PipelineVersion int              `json:"pipelineVersion"`
}

func (h *Handler) handleGetPipelineState(
	ctx context.Context,
	in *getPipelineStateInput,
) (*getPipelineStateOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	states, err := h.Backend.GetPipelineState(ctx, in.Name)
	if err != nil {
		return nil, err
	}

	p, err := h.Backend.GetPipeline(ctx, in.Name)
	if err != nil {
		return nil, err
	}

	items := make([]map[string]any, len(states))
	for i, s := range states {
		item := map[string]any{
			"stageName":    s.StageName,
			"actionStates": s.ActionStates,
		}
		if s.InboundTransitionState != nil {
			item["inboundTransitionState"] = map[string]any{
				"disabled": s.InboundTransitionState.Disabled,
				"reason":   s.InboundTransitionState.Reason,
			}
		}

		if s.OutboundTransitionState != nil {
			item["outboundTransitionState"] = map[string]any{
				"disabled": s.OutboundTransitionState.Disabled,
				"reason":   s.OutboundTransitionState.Reason,
			}
		}

		items[i] = item
	}

	return &getPipelineStateOutput{
		PipelineName:    in.Name,
		StageStates:     items,
		PipelineVersion: p.Declaration.Version,
	}, nil
}

type retryStageExecutionInput struct {
	PipelineName        string `json:"pipelineName"`
	StageName           string `json:"stageName"`
	PipelineExecutionID string `json:"pipelineExecutionId"`
	RetryMode           string `json:"retryMode"`
}

func (h *Handler) handleRetryStageExecution(
	ctx context.Context,
	in *retryStageExecutionInput,
) (*pipelineExecutionOutput, error) {
	if in.PipelineName == "" {
		return nil, fmt.Errorf("%w: pipelineName is required", errInvalidRequest)
	}

	exec, err := h.Backend.RetryStageExecution(ctx, in.PipelineName, in.StageName, in.PipelineExecutionID)
	if err != nil {
		return nil, err
	}

	return &pipelineExecutionOutput{PipelineExecutionID: exec.PipelineExecutionID}, nil
}

type rollbackStageInput struct {
	PipelineName              string `json:"pipelineName"`
	StageName                 string `json:"stageName"`
	TargetPipelineExecutionID string `json:"targetPipelineExecutionId"`
}

func (h *Handler) handleRollbackStage(
	ctx context.Context,
	in *rollbackStageInput,
) (*pipelineExecutionOutput, error) {
	if in.PipelineName == "" {
		return nil, fmt.Errorf("%w: pipelineName is required", errInvalidRequest)
	}

	exec, err := h.Backend.RollbackStage(ctx, in.PipelineName, in.StageName, in.TargetPipelineExecutionID)
	if err != nil {
		return nil, err
	}

	return &pipelineExecutionOutput{PipelineExecutionID: exec.PipelineExecutionID}, nil
}

type overrideStageConditionInput struct {
	PipelineName        string `json:"pipelineName"`
	StageName           string `json:"stageName"`
	PipelineExecutionID string `json:"pipelineExecutionId"`
	ConditionType       string `json:"conditionType"`
}

func (h *Handler) handleOverrideStageCondition(
	ctx context.Context,
	in *overrideStageConditionInput,
) (*emptyOut, error) {
	if in.PipelineName == "" {
		return nil, fmt.Errorf("%w: pipelineName is required", errInvalidRequest)
	}

	if err := h.Backend.OverrideStageCondition(ctx, in.PipelineName, in.StageName, in.PipelineExecutionID); err != nil {
		return nil, err
	}

	return &emptyOut{}, nil
}

type disableStageTransitionInput struct {
	PipelineName   string `json:"pipelineName"`
	Reason         string `json:"reason"`
	StageName      string `json:"stageName"`
	TransitionType string `json:"transitionType"`
}

type disableStageTransitionOutput struct{}

func (h *Handler) handleDisableStageTransition(
	ctx context.Context,
	in *disableStageTransitionInput,
) (*disableStageTransitionOutput, error) {
	if in.PipelineName == "" {
		return nil, fmt.Errorf("%w: pipelineName is required", errInvalidRequest)
	}

	if in.StageName == "" {
		return nil, fmt.Errorf("%w: stageName is required", errInvalidRequest)
	}

	if in.TransitionType == "" {
		return nil, fmt.Errorf("%w: transitionType is required", errInvalidRequest)
	}

	if !validTransitionType(in.TransitionType) {
		return nil, fmt.Errorf("%w: invalid transitionType %q, must be %s or %s",
			ErrValidation, in.TransitionType, transitionTypeInbound, transitionTypeOutbound)
	}

	if in.Reason == "" {
		return nil, fmt.Errorf("%w: reason is required", errInvalidRequest)
	}

	if err := h.Backend.DisableStageTransition(
		ctx, in.PipelineName, in.StageName, in.TransitionType, in.Reason,
	); err != nil {
		return nil, err
	}

	return &disableStageTransitionOutput{}, nil
}

type enableStageTransitionInput struct {
	PipelineName   string `json:"pipelineName"`
	StageName      string `json:"stageName"`
	TransitionType string `json:"transitionType"`
}

type enableStageTransitionOutput struct{}

func (h *Handler) handleEnableStageTransition(
	ctx context.Context,
	in *enableStageTransitionInput,
) (*enableStageTransitionOutput, error) {
	if in.PipelineName == "" {
		return nil, fmt.Errorf("%w: pipelineName is required", errInvalidRequest)
	}

	if in.StageName == "" {
		return nil, fmt.Errorf("%w: stageName is required", errInvalidRequest)
	}

	if in.TransitionType == "" {
		return nil, fmt.Errorf("%w: transitionType is required", errInvalidRequest)
	}

	if !validTransitionType(in.TransitionType) {
		return nil, fmt.Errorf("%w: invalid transitionType %q, must be %s or %s",
			ErrValidation, in.TransitionType, transitionTypeInbound, transitionTypeOutbound)
	}

	if err := h.Backend.EnableStageTransition(ctx, in.PipelineName, in.StageName, in.TransitionType); err != nil {
		return nil, err
	}

	return &enableStageTransitionOutput{}, nil
}
