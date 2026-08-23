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

// getBlueprintRunOutput holds the result for GetBlueprintRun. The real
// GetBlueprintRunOutput's member is BlueprintRun (api_op_GetBlueprintRun.go),
// not Run -- a real client's decode silently drops the whole payload since
// no known key matches.
type getBlueprintRunOutput struct {
	BlueprintRun any `json:"BlueprintRun"`
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

	return &getBlueprintRunOutput{BlueprintRun: run}, nil
}

// defaultGetBlueprintRunsLimit is used when GetBlueprintRunsInput.MaxResults is unset.
const defaultGetBlueprintRunsLimit = 100

// getBlueprintRunsInput holds input for GetBlueprintRuns.
type getBlueprintRunsInput struct {
	BlueprintName string `json:"BlueprintName"`
	NextToken     string `json:"NextToken,omitempty"`
	MaxResults    int32  `json:"MaxResults,omitempty"`
}

// getBlueprintRunsOutput holds the result for GetBlueprintRuns. The real
// member is BlueprintRuns (api_op_GetBlueprintRuns.go), not Runs.
type getBlueprintRunsOutput struct {
	NextToken     string `json:"NextToken,omitempty"`
	BlueprintRuns []any  `json:"BlueprintRuns"`
}

func (h *Handler) handleGetBlueprintRuns(
	_ context.Context,
	in *getBlueprintRunsInput,
) (*getBlueprintRunsOutput, error) {
	all := h.Backend.GetBlueprintRuns(in.BlueprintName)

	limit := int(in.MaxResults)
	if limit <= 0 {
		limit = defaultGetBlueprintRunsLimit
	}

	runs, next := paginateSlice(all, in.NextToken, limit)

	result := make([]any, 0, len(runs))
	for _, r := range runs {
		result = append(result, r)
	}

	return &getBlueprintRunsOutput{BlueprintRuns: result, NextToken: next}, nil
}

// defaultListBlueprintsLimit is used when ListBlueprintsInput.MaxResults is unset.
const defaultListBlueprintsLimit = 100

// listBlueprintsInput holds input for ListBlueprints.
type listBlueprintsInput struct {
	Tags       map[string]string `json:"Tags,omitempty"`
	NextToken  string            `json:"NextToken,omitempty"`
	MaxResults int32             `json:"MaxResults,omitempty"`
}

// listBlueprintsOutput holds the result for ListBlueprints.
type listBlueprintsOutput struct {
	NextToken  string   `json:"NextToken,omitempty"`
	Blueprints []string `json:"Blueprints"`
}

func (h *Handler) handleListBlueprints(
	_ context.Context,
	in *listBlueprintsInput,
) (*listBlueprintsOutput, error) {
	names := h.Backend.ListBlueprints()

	blueprints := names
	if len(in.Tags) > 0 {
		full, _ := h.Backend.BatchGetBlueprints(names)
		filtered := make([]string, 0, len(full))

		for _, bp := range full {
			if matchesTagFilter(bp.Tags, in.Tags) {
				filtered = append(filtered, bp.Name)
			}
		}

		blueprints = filtered
	}

	limit := int(in.MaxResults)
	if limit <= 0 {
		limit = defaultListBlueprintsLimit
	}

	page, next := paginateSlice(blueprints, in.NextToken, limit)

	return &listBlueprintsOutput{Blueprints: page, NextToken: next}, nil
}

// startBlueprintRunInput holds input for StartBlueprintRun. RoleArn is
// required on the real op (api_op_StartBlueprintRun.go) but was previously
// dropped entirely.
type startBlueprintRunInput struct {
	BlueprintName string `json:"BlueprintName"`
	RoleArn       string `json:"RoleArn"`
	Parameters    string `json:"Parameters,omitempty"`
}

// startBlueprintRunOutput holds the result for StartBlueprintRun.
type startBlueprintRunOutput struct {
	RunID string `json:"RunId"`
}

func (h *Handler) handleStartBlueprintRun(
	_ context.Context,
	in *startBlueprintRunInput,
) (*startBlueprintRunOutput, error) {
	if in.RoleArn == "" {
		return nil, fmt.Errorf("%w: RoleArn is required", ErrValidation)
	}

	run, err := h.Backend.StartBlueprintRun(in.BlueprintName, in.RoleArn, in.Parameters)
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
