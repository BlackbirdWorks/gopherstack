package glue

import (
	"context"
	"fmt"
)

type batchGetBlueprintsInput struct {
	Names []string `json:"Names"`
}

type batchGetBlueprintsOutput struct {
	Blueprints        []*Blueprint `json:"Blueprints"`
	MissingBlueprints []string     `json:"MissingBlueprints"`
}

func (h *Handler) handleBatchGetBlueprints(
	_ context.Context,
	in *batchGetBlueprintsInput,
) (*batchGetBlueprintsOutput, error) {
	found, missing := h.Backend.BatchGetBlueprints(in.Names)

	return &batchGetBlueprintsOutput{Blueprints: found, MissingBlueprints: missing}, nil
}

// createBlueprintInput holds input for CreateBlueprint.
type createBlueprintInput struct {
	Tags              map[string]string `json:"Tags,omitempty"`
	Name              string            `json:"Name"`
	BlueprintLocation string            `json:"BlueprintLocation"`
	Description       string            `json:"Description,omitempty"`
}

// createBlueprintOutput holds the result for CreateBlueprint.
type createBlueprintOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleCreateBlueprint(
	_ context.Context,
	in *createBlueprintInput,
) (*createBlueprintOutput, error) {
	if _, err := h.Backend.CreateBlueprint(in.Name, in.BlueprintLocation, in.Description, in.Tags); err != nil {
		return nil, err
	}

	return &createBlueprintOutput{Name: in.Name}, nil
}

// deleteBlueprintInput holds input for DeleteBlueprint.
type deleteBlueprintInput struct {
	Name string `json:"Name"`
}

// deleteBlueprintOutput holds the result for DeleteBlueprint.
type deleteBlueprintOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteBlueprint(
	_ context.Context,
	in *deleteBlueprintInput,
) (*deleteBlueprintOutput, error) {
	if err := h.Backend.DeleteBlueprint(in.Name); err != nil {
		return nil, err
	}

	return &deleteBlueprintOutput{Name: in.Name}, nil
}

// getBlueprintInput holds input for GetBlueprint.
type getBlueprintInput struct {
	Name string `json:"Name"`
}

// getBlueprintOutput holds the result for GetBlueprint.
type getBlueprintOutput struct {
	Blueprint *Blueprint `json:"Blueprint"`
}

func (h *Handler) handleGetBlueprint(
	_ context.Context,
	in *getBlueprintInput,
) (*getBlueprintOutput, error) {
	found, missing := h.Backend.BatchGetBlueprints([]string{in.Name})
	if len(missing) > 0 {
		return nil, fmt.Errorf("blueprint %q not found: %w", in.Name, ErrNotFound)
	}

	return &getBlueprintOutput{Blueprint: found[0]}, nil
}

// getBlueprintRunInput holds input for GetBlueprintRun.
type getBlueprintRunInput struct {
	BlueprintName string `json:"BlueprintName"`
	RunID         string `json:"RunId"`
}

// getBlueprintRunOutput holds the result for GetBlueprintRun.
type getBlueprintRunOutput struct {
	Run any `json:"Run"`
}

func (h *Handler) handleGetBlueprintRun(
	_ context.Context,
	in *getBlueprintRunInput,
) (*getBlueprintRunOutput, error) {
	if in.RunID == "" {
		return nil, fmt.Errorf("%w: RunId is required", ErrValidation)
	}

	run, err := h.Backend.GetBlueprintRun(in.BlueprintName, in.RunID)
	if err != nil {
		return nil, err
	}

	return &getBlueprintRunOutput{Run: run}, nil
}

// getBlueprintRunsInput holds input for GetBlueprintRuns.
type getBlueprintRunsInput struct {
	BlueprintName string `json:"BlueprintName"`
}

// getBlueprintRunsOutput holds the result for GetBlueprintRuns.
type getBlueprintRunsOutput struct {
	Runs []any `json:"Runs"`
}

func (h *Handler) handleGetBlueprintRuns(
	_ context.Context,
	in *getBlueprintRunsInput,
) (*getBlueprintRunsOutput, error) {
	runs := h.Backend.GetBlueprintRuns(in.BlueprintName)
	result := make([]any, 0, len(runs))
	for _, r := range runs {
		result = append(result, r)
	}

	return &getBlueprintRunsOutput{Runs: result}, nil
}

// listBlueprintsInput holds input for ListBlueprints.
type listBlueprintsInput struct{}

// listBlueprintsOutput holds the result for ListBlueprints.
type listBlueprintsOutput struct {
	Blueprints []string `json:"Blueprints"`
}

func (h *Handler) handleListBlueprints(
	_ context.Context,
	_ *listBlueprintsInput,
) (*listBlueprintsOutput, error) {
	return &listBlueprintsOutput{Blueprints: h.Backend.ListBlueprints()}, nil
}

// startBlueprintRunInput holds input for StartBlueprintRun.
type startBlueprintRunInput struct {
	BlueprintName string `json:"BlueprintName"`
}

// startBlueprintRunOutput holds the result for StartBlueprintRun.
type startBlueprintRunOutput struct {
	RunID string `json:"RunId"`
}

func (h *Handler) handleStartBlueprintRun(
	_ context.Context,
	in *startBlueprintRunInput,
) (*startBlueprintRunOutput, error) {
	run, err := h.Backend.StartBlueprintRun(in.BlueprintName)
	if err != nil {
		return nil, err
	}

	return &startBlueprintRunOutput{RunID: run.RunID}, nil
}

// updateBlueprintInput holds input for UpdateBlueprint.
type updateBlueprintInput struct {
	Name              string `json:"Name"`
	BlueprintLocation string `json:"BlueprintLocation"`
	Description       string `json:"Description,omitempty"`
}

// updateBlueprintOutput holds the result for UpdateBlueprint.
type updateBlueprintOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleUpdateBlueprint(
	_ context.Context,
	in *updateBlueprintInput,
) (*updateBlueprintOutput, error) {
	if _, err := h.Backend.UpdateBlueprint(in.Name, in.BlueprintLocation, in.Description); err != nil {
		return nil, err
	}

	return &updateBlueprintOutput{Name: in.Name}, nil
}
