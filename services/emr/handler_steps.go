package emr

import (
	"context"
)

// --- ListSteps ---

type listStepsInput struct {
	ClusterID  string   `json:"ClusterId"`
	Marker     string   `json:"Marker"`
	StepStates []string `json:"StepStates"`
	StepIDs    []string `json:"StepIds"`
}

type listStepsOutput struct {
	Marker string `json:"Marker,omitempty"`
	Steps  []Step `json:"Steps"`
}

func (h *Handler) handleListSteps(ctx context.Context, in *listStepsInput) (*listStepsOutput, error) {
	steps, nextMarker := h.Backend.ListSteps(ctx, in.ClusterID, in.StepStates, in.StepIDs, in.Marker)

	return &listStepsOutput{Steps: steps, Marker: nextMarker}, nil
}

// --- AddJobFlowSteps ---

type addJobFlowStepsInput struct {
	JobFlowID string     `json:"JobFlowId"`
	Steps     []StepSpec `json:"Steps"`
}

type addJobFlowStepsOutput struct {
	StepIDs []string `json:"StepIds"`
}

func (h *Handler) handleAddJobFlowSteps(
	ctx context.Context,
	in *addJobFlowStepsInput,
) (*addJobFlowStepsOutput, error) {
	ids, err := h.Backend.AddJobFlowSteps(ctx, in.JobFlowID, in.Steps)
	if err != nil {
		return nil, err
	}

	return &addJobFlowStepsOutput{StepIDs: ids}, nil
}

// --- ListBootstrapActions ---

type listBootstrapActionsInput struct {
	ClusterID string `json:"ClusterId"`
	Marker    string `json:"Marker"`
}

type listBootstrapActionsOutput struct {
	Marker           string    `json:"Marker,omitempty"`
	BootstrapActions []Command `json:"BootstrapActions"`
}

func (h *Handler) handleListBootstrapActions(
	ctx context.Context,
	in *listBootstrapActionsInput,
) (*listBootstrapActionsOutput, error) {
	commands, nextMarker, err := h.Backend.ListBootstrapActions(ctx, in.ClusterID, in.Marker)
	if err != nil {
		return nil, err
	}

	if commands == nil {
		commands = []Command{}
	}

	return &listBootstrapActionsOutput{BootstrapActions: commands, Marker: nextMarker}, nil
}

// --- CancelSteps ---

type cancelStepsInput struct {
	ClusterID string   `json:"ClusterId"`
	StepIDs   []string `json:"StepIds"`
}

type cancelStepsOutput struct {
	CancelStepsInfoList []any `json:"CancelStepsInfoList"`
}

func (h *Handler) handleCancelSteps(
	ctx context.Context,
	in *cancelStepsInput,
) (*cancelStepsOutput, error) {
	results, err := h.Backend.CancelSteps(ctx, in.ClusterID, in.StepIDs)
	if err != nil {
		return nil, err
	}

	list := make([]any, 0, len(results))
	for _, r := range results {
		list = append(list, r)
	}

	return &cancelStepsOutput{CancelStepsInfoList: list}, nil
}

// --- DescribeStep ---

type describeStepInput struct {
	ClusterID string `json:"ClusterId"`
	StepID    string `json:"StepId"`
}

type describeStepOutput struct {
	Step *Step `json:"Step"`
}

func (h *Handler) handleDescribeStep(ctx context.Context, in *describeStepInput) (*describeStepOutput, error) {
	step, err := h.Backend.DescribeStep(ctx, in.ClusterID, in.StepID)
	if err != nil {
		return nil, err
	}

	return &describeStepOutput{Step: step}, nil
}
