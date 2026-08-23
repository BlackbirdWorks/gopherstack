package glue

import (
	"context"
)

// getMaterializedViewRefreshTaskRunInput holds input for
// GetMaterializedViewRefreshTaskRun. The real member name is
// MaterializedViewRefreshTaskRunId, not RunId (glue@v1.152.0
// api_op_GetMaterializedViewRefreshTaskRun.go), and CatalogId is a required
// member not modeled: this backend keeps one flat namespace of refresh runs
// with no per-catalog scoping -- accepted on the wire and otherwise inert.
type getMaterializedViewRefreshTaskRunInput struct {
	MaterializedViewRefreshTaskRunID string `json:"MaterializedViewRefreshTaskRunId"`
}

// getMaterializedViewRefreshTaskRunOutput holds the result for
// GetMaterializedViewRefreshTaskRun. The real output wraps the run under a
// MaterializedViewRefreshTaskRun member (types.MaterializedViewRefreshTaskRun),
// not a flat RunId/Status pair.
type getMaterializedViewRefreshTaskRunOutput struct {
	MaterializedViewRefreshTaskRun *MaterializedViewRefreshRun `json:"MaterializedViewRefreshTaskRun,omitempty"`
}

func (h *Handler) handleGetMaterializedViewRefreshTaskRun(
	_ context.Context,
	in *getMaterializedViewRefreshTaskRunInput,
) (*getMaterializedViewRefreshTaskRunOutput, error) {
	run, err := h.Backend.GetMaterializedViewRefreshTaskRun(in.MaterializedViewRefreshTaskRunID)
	if err != nil {
		return nil, err
	}

	return &getMaterializedViewRefreshTaskRunOutput{MaterializedViewRefreshTaskRun: run}, nil
}

// defaultListMaterializedViewRefreshTaskRunsLimit is used when
// ListMaterializedViewRefreshTaskRunsInput.MaxResults is unset.
const defaultListMaterializedViewRefreshTaskRunsLimit = 100

// listMaterializedViewRefreshTaskRunsInput holds input for
// ListMaterializedViewRefreshTaskRuns.
//
// CatalogId (required on the real op) is not modeled: this backend keeps one
// flat namespace of materialized-view refresh runs with no per-catalog
// scoping, matching how the rest of this service treats the account's
// implicit single catalog -- accepted on the wire and otherwise inert.
// DatabaseName/TableName are real: MaterializedViewRefreshRun (models.go)
// stores both.
type listMaterializedViewRefreshTaskRunsInput struct {
	CatalogID    string `json:"CatalogId"`
	DatabaseName string `json:"DatabaseName,omitempty"`
	TableName    string `json:"TableName,omitempty"`
	NextToken    string `json:"NextToken,omitempty"`
	MaxResults   int32  `json:"MaxResults,omitempty"`
}

// listMaterializedViewRefreshTaskRunsOutput holds the result for
// ListMaterializedViewRefreshTaskRuns. The real member name is
// MaterializedViewRefreshTaskRuns, not Runs (glue@v1.152.0
// api_op_ListMaterializedViewRefreshTaskRuns.go) -- found while wiring this
// op's Filter/MaxResults/NextToken (gopherstack-awzv).
type listMaterializedViewRefreshTaskRunsOutput struct {
	NextToken                       string `json:"NextToken,omitempty"`
	MaterializedViewRefreshTaskRuns []any  `json:"MaterializedViewRefreshTaskRuns"`
}

func (h *Handler) handleListMaterializedViewRefreshTaskRuns(
	_ context.Context,
	in *listMaterializedViewRefreshTaskRunsInput,
) (*listMaterializedViewRefreshTaskRunsOutput, error) {
	all := h.Backend.ListMaterializedViewRefreshTaskRuns()

	matching := make([]*MaterializedViewRefreshRun, 0, len(all))

	for _, r := range all {
		if in.DatabaseName != "" && r.DatabaseName != in.DatabaseName {
			continue
		}

		if in.TableName != "" && r.TableName != in.TableName {
			continue
		}

		matching = append(matching, r)
	}

	limit := int(in.MaxResults)
	if limit <= 0 {
		limit = defaultListMaterializedViewRefreshTaskRunsLimit
	}

	page, next := paginateSlice(matching, in.NextToken, limit)

	result := make([]any, 0, len(page))
	for _, r := range page {
		result = append(result, r)
	}

	return &listMaterializedViewRefreshTaskRunsOutput{MaterializedViewRefreshTaskRuns: result, NextToken: next}, nil
}

// startMaterializedViewRefreshTaskRunInput holds input for StartMaterializedViewRefreshTaskRun.
type startMaterializedViewRefreshTaskRunInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
}

// startMaterializedViewRefreshTaskRunOutput holds the result for
// StartMaterializedViewRefreshTaskRun. The real member name is
// MaterializedViewRefreshTaskRunId, not RunId (glue@v1.152.0
// api_op_StartMaterializedViewRefreshTaskRun.go).
type startMaterializedViewRefreshTaskRunOutput struct {
	MaterializedViewRefreshTaskRunID string `json:"MaterializedViewRefreshTaskRunId"`
}

func (h *Handler) handleStartMaterializedViewRefreshTaskRun(
	_ context.Context,
	in *startMaterializedViewRefreshTaskRunInput,
) (*startMaterializedViewRefreshTaskRunOutput, error) {
	run, err := h.Backend.StartMaterializedViewRefreshTaskRun(in.DatabaseName, in.TableName)
	if err != nil {
		return nil, err
	}

	return &startMaterializedViewRefreshTaskRunOutput{MaterializedViewRefreshTaskRunID: run.TaskRunID}, nil
}

// stopMaterializedViewRefreshTaskRunInput holds input for
// StopMaterializedViewRefreshTaskRun. The real
// StopMaterializedViewRefreshTaskRunInput (glue@v1.152.0
// api_op_StopMaterializedViewRefreshTaskRun.go) identifies the run by
// DatabaseName+TableName, not a run ID -- there is no RunId member on this
// op at all. Previously a real client's Stop call always sent an empty
// RunId, hitting this handler's early-return and silently no-op'ing instead
// of stopping anything.
type stopMaterializedViewRefreshTaskRunInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
}

func (h *Handler) handleStopMaterializedViewRefreshTaskRun(
	_ context.Context,
	in *stopMaterializedViewRefreshTaskRunInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.StopMaterializedViewRefreshTaskRun(in.DatabaseName, in.TableName)
}
