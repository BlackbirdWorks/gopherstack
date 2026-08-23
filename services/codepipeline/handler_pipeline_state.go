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

// getPipelineStateOutput's Created/Updated were previously never emitted at
// all despite being real, always-populated members of
// awsAwsjson11_deserializeOpDocumentGetPipelineStateOutput; fixed
// incidentally while auditing this op's wrapper key.
type getPipelineStateOutput struct {
	PipelineName    string           `json:"pipelineName"`
	StageStates     []map[string]any `json:"stageStates"`
	PipelineVersion int              `json:"pipelineVersion"`
	Created         float64          `json:"created"`
	Updated         float64          `json:"updated"`
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
		// Real types.StageState (deserializers.go awsAwsjson11_deserializeDocumentStageState)
		// has no "outboundTransitionState" member -- only inboundTransitionState is
		// wire-visible, regardless of DisableStageTransition's Outbound transitionType.
		if s.InboundTransitionState != nil {
			item["inboundTransitionState"] = map[string]any{
				// Real types.TransitionState (awsAwsjson11_deserializeDocumentTransitionState)
				// has no "disabled"/"reason" members -- it is enabled (bool, inverse of our
				// stored Disabled) and disabledReason, not disabled/reason.
				"enabled":        !s.InboundTransitionState.Disabled,
				"disabledReason": s.InboundTransitionState.Reason,
			}
		}

		items[i] = item
	}

	return &getPipelineStateOutput{
		PipelineName:    in.Name,
		StageStates:     items,
		PipelineVersion: p.Declaration.Version,
		Created:         p.Metadata.Created,
		Updated:         p.Metadata.Updated,
	}, nil
}

type retryStageExecutionInput struct {
	PipelineName        string `json:"pipelineName"`
	StageName           string `json:"stageName"`
	PipelineExecutionID string `json:"pipelineExecutionId"`
	RetryMode           string `json:"retryMode"`
}

// validRetryMode returns true if m is a valid StageRetryMode value.
func validRetryMode(m string) bool {
	return m == "FAILED_ACTIONS" || m == stageRetryModeAllActions
}

func (h *Handler) handleRetryStageExecution(
	ctx context.Context,
	in *retryStageExecutionInput,
) (*pipelineExecutionOutput, error) {
	if in.PipelineName == "" {
		return nil, fmt.Errorf("%w: pipelineName is required", errInvalidRequest)
	}

	if in.StageName == "" {
		return nil, fmt.Errorf("%w: stageName is required", errInvalidRequest)
	}

	if in.PipelineExecutionID == "" {
		return nil, fmt.Errorf("%w: pipelineExecutionId is required", errInvalidRequest)
	}

	if !validRetryMode(in.RetryMode) {
		return nil, fmt.Errorf("%w: invalid retryMode %q, must be FAILED_ACTIONS or %s",
			ErrValidation, in.RetryMode, stageRetryModeAllActions)
	}

	exec, err := h.Backend.RetryStageExecution(ctx, in.PipelineName, in.StageName, in.PipelineExecutionID, in.RetryMode)
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

	if in.StageName == "" {
		return nil, fmt.Errorf("%w: stageName is required", errInvalidRequest)
	}

	if in.TargetPipelineExecutionID == "" {
		return nil, fmt.Errorf("%w: targetPipelineExecutionId is required", errInvalidRequest)
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

// validConditionType returns true if t is a valid ConditionType value. AWS's
// real enum (aws-sdk-go-v2/service/codepipeline/types.ConditionType) has
// exactly two values: BEFORE_ENTRY and ON_SUCCESS -- there is no ON_FAILURE
// ConditionType (failure conditions are overridden implicitly by retrying or
// rolling back the stage, not via OverrideStageCondition).
func validConditionType(t string) bool {
	return t == "BEFORE_ENTRY" || t == "ON_SUCCESS"
}

func (h *Handler) handleOverrideStageCondition(
	ctx context.Context,
	in *overrideStageConditionInput,
) (*emptyOut, error) {
	if in.PipelineName == "" {
		return nil, fmt.Errorf("%w: pipelineName is required", errInvalidRequest)
	}

	if in.StageName == "" {
		return nil, fmt.Errorf("%w: stageName is required", errInvalidRequest)
	}

	if in.PipelineExecutionID == "" {
		return nil, fmt.Errorf("%w: pipelineExecutionId is required", errInvalidRequest)
	}

	if !validConditionType(in.ConditionType) {
		return nil, fmt.Errorf(
			"%w: invalid conditionType %q, must be BEFORE_ENTRY or ON_SUCCESS", ErrValidation, in.ConditionType,
		)
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
