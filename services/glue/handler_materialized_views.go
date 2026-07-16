package glue

import (
	"context"
)

// getMaterializedViewRefreshTaskRunInput holds input for GetMaterializedViewRefreshTaskRun.
type getMaterializedViewRefreshTaskRunInput struct {
	RunID string `json:"RunId"`
}

// getMaterializedViewRefreshTaskRunOutput holds the result for GetMaterializedViewRefreshTaskRun.
type getMaterializedViewRefreshTaskRunOutput struct {
	RunID  string `json:"RunId"`
	Status string `json:"Status"`
}

func (h *Handler) handleGetMaterializedViewRefreshTaskRun(
	_ context.Context,
	in *getMaterializedViewRefreshTaskRunInput,
) (*getMaterializedViewRefreshTaskRunOutput, error) {
	if in.RunID != "" {
		run, err := h.Backend.GetMaterializedViewRefreshTaskRun(in.RunID)
		if err != nil {
			return nil, err
		}

		return &getMaterializedViewRefreshTaskRunOutput{
			RunID:  run.TaskRunID,
			Status: run.Status,
		}, nil
	}

	runs := h.Backend.ListMaterializedViewRefreshTaskRuns()
	if len(runs) == 0 {
		return &getMaterializedViewRefreshTaskRunOutput{Status: stateSucceeded}, nil
	}

	return &getMaterializedViewRefreshTaskRunOutput{
		RunID:  runs[0].TaskRunID,
		Status: runs[0].Status,
	}, nil
}

// listMaterializedViewRefreshTaskRunsInput holds input for ListMaterializedViewRefreshTaskRuns.
type listMaterializedViewRefreshTaskRunsInput struct{}

// listMaterializedViewRefreshTaskRunsOutput holds the result for ListMaterializedViewRefreshTaskRuns.
type listMaterializedViewRefreshTaskRunsOutput struct {
	Runs []any `json:"Runs"`
}

func (h *Handler) handleListMaterializedViewRefreshTaskRuns(
	_ context.Context,
	_ *listMaterializedViewRefreshTaskRunsInput,
) (*listMaterializedViewRefreshTaskRunsOutput, error) {
	runs := h.Backend.ListMaterializedViewRefreshTaskRuns()
	result := make([]any, 0, len(runs))
	for _, r := range runs {
		result = append(result, r)
	}

	return &listMaterializedViewRefreshTaskRunsOutput{Runs: result}, nil
}

// startMaterializedViewRefreshTaskRunInput holds input for StartMaterializedViewRefreshTaskRun.
type startMaterializedViewRefreshTaskRunInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
}

// startMaterializedViewRefreshTaskRunOutput holds the result for StartMaterializedViewRefreshTaskRun.
type startMaterializedViewRefreshTaskRunOutput struct {
	RunID string `json:"RunId"`
}

func (h *Handler) handleStartMaterializedViewRefreshTaskRun(
	_ context.Context,
	in *startMaterializedViewRefreshTaskRunInput,
) (*startMaterializedViewRefreshTaskRunOutput, error) {
	run, err := h.Backend.StartMaterializedViewRefreshTaskRun(in.DatabaseName, in.TableName)
	if err != nil {
		return nil, err
	}

	return &startMaterializedViewRefreshTaskRunOutput{RunID: run.TaskRunID}, nil
}

// stopMaterializedViewRefreshTaskRunInput holds input for StopMaterializedViewRefreshTaskRun.
type stopMaterializedViewRefreshTaskRunInput struct {
	RunID string `json:"RunId"`
}

func (h *Handler) handleStopMaterializedViewRefreshTaskRun(
	_ context.Context,
	in *stopMaterializedViewRefreshTaskRunInput,
) (*emptyOutput, error) {
	if in.RunID == "" {
		return &emptyOutput{}, nil
	}

	return &emptyOutput{}, h.Backend.StopMaterializedViewRefreshTaskRun(in.RunID)
}
