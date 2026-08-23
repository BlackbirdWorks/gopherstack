package glue

import (
	"context"
	"fmt"
)

// createColumnStatisticsTaskSettingsInput holds input for CreateColumnStatisticsTaskSettings.
type createColumnStatisticsTaskSettingsInput struct {
	DatabaseName   string   `json:"DatabaseName"`
	TableName      string   `json:"TableName"`
	RoleArn        string   `json:"Role,omitempty"`
	ColumnNameList []string `json:"ColumnNameList,omitempty"`
}

func (h *Handler) handleCreateColumnStatisticsTaskSettings(
	_ context.Context,
	in *createColumnStatisticsTaskSettingsInput,
) (*emptyOutput, error) {
	_, err := h.Backend.CreateColumnStatisticsTaskSettings(
		in.DatabaseName, in.TableName, in.RoleArn, in.ColumnNameList,
	)

	return &emptyOutput{}, err
}

// deleteColumnStatisticsForPartitionInput holds input for DeleteColumnStatisticsForPartition.
type deleteColumnStatisticsForPartitionInput struct {
	DatabaseName    string   `json:"DatabaseName"`
	TableName       string   `json:"TableName"`
	ColumnName      string   `json:"ColumnName"`
	PartitionValues []string `json:"PartitionValues"`
}

func (h *Handler) handleDeleteColumnStatisticsForPartition(
	_ context.Context,
	in *deleteColumnStatisticsForPartitionInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteColumnStatisticsForPartition(
		in.DatabaseName,
		in.TableName,
		in.PartitionValues,
		in.ColumnName,
	)
}

// deleteColumnStatisticsForTableInput holds input for DeleteColumnStatisticsForTable.
type deleteColumnStatisticsForTableInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
	ColumnName   string `json:"ColumnName"`
}

func (h *Handler) handleDeleteColumnStatisticsForTable(
	_ context.Context,
	in *deleteColumnStatisticsForTableInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteColumnStatisticsForTable(
		in.DatabaseName,
		in.TableName,
		in.ColumnName,
	)
}

// deleteColumnStatisticsTaskSettingsInput holds input for DeleteColumnStatisticsTaskSettings.
type deleteColumnStatisticsTaskSettingsInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
}

func (h *Handler) handleDeleteColumnStatisticsTaskSettings(
	_ context.Context,
	in *deleteColumnStatisticsTaskSettingsInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteColumnStatisticsTaskSettings(
		in.DatabaseName,
		in.TableName,
	)
}

// getColumnStatisticsForPartitionInput holds input for GetColumnStatisticsForPartition.
type getColumnStatisticsForPartitionInput struct {
	DatabaseName    string   `json:"DatabaseName"`
	TableName       string   `json:"TableName"`
	PartitionValues []string `json:"PartitionValues"`
	ColumnNames     []string `json:"ColumnNames,omitempty"`
}

// getColumnStatisticsForPartitionOutput holds the result for GetColumnStatisticsForPartition.
type getColumnStatisticsForPartitionOutput struct {
	ColumnStatisticsList []*ColumnStatistics `json:"ColumnStatisticsList"`
	Errors               []any               `json:"Errors"`
}

func (h *Handler) handleGetColumnStatisticsForPartition(
	_ context.Context,
	in *getColumnStatisticsForPartitionInput,
) (*getColumnStatisticsForPartitionOutput, error) {
	stats, err := h.Backend.GetColumnStatisticsForPartition(
		in.DatabaseName,
		in.TableName,
		in.PartitionValues,
		in.ColumnNames,
	)
	if err != nil {
		return nil, err
	}
	if stats == nil {
		stats = []*ColumnStatistics{}
	}

	return &getColumnStatisticsForPartitionOutput{ColumnStatisticsList: stats, Errors: []any{}}, nil
}

// getColumnStatisticsForTableInput holds input for GetColumnStatisticsForTable.
type getColumnStatisticsForTableInput struct {
	DatabaseName string   `json:"DatabaseName"`
	TableName    string   `json:"TableName"`
	ColumnNames  []string `json:"ColumnNames,omitempty"`
}

// getColumnStatisticsForTableOutput holds the result for GetColumnStatisticsForTable.
type getColumnStatisticsForTableOutput struct {
	ColumnStatisticsList []*ColumnStatistics `json:"ColumnStatisticsList"`
	Errors               []any               `json:"Errors"`
}

func (h *Handler) handleGetColumnStatisticsForTable(
	_ context.Context,
	in *getColumnStatisticsForTableInput,
) (*getColumnStatisticsForTableOutput, error) {
	stats, err := h.Backend.GetColumnStatisticsForTable(
		in.DatabaseName,
		in.TableName,
		in.ColumnNames,
	)
	if err != nil {
		return nil, err
	}
	if stats == nil {
		stats = []*ColumnStatistics{}
	}

	return &getColumnStatisticsForTableOutput{ColumnStatisticsList: stats, Errors: []any{}}, nil
}

// getColumnStatisticsTaskRunInput holds input for GetColumnStatisticsTaskRun.
type getColumnStatisticsTaskRunInput struct {
	ColumnStatisticsTaskRunID string `json:"ColumnStatisticsTaskRunId"`
}

// getColumnStatisticsTaskRunOutput holds the result for GetColumnStatisticsTaskRun.
type getColumnStatisticsTaskRunOutput struct {
	ColumnStatisticsTaskRun any `json:"ColumnStatisticsTaskRun"`
}

func (h *Handler) handleGetColumnStatisticsTaskRun(
	_ context.Context,
	in *getColumnStatisticsTaskRunInput,
) (*getColumnStatisticsTaskRunOutput, error) {
	if in.ColumnStatisticsTaskRunID == "" {
		return nil, fmt.Errorf("%w: ColumnStatisticsTaskRunId is required", ErrValidation)
	}

	run, err := h.Backend.GetColumnStatisticsTaskRun(in.ColumnStatisticsTaskRunID)
	if err != nil {
		return nil, err
	}

	return &getColumnStatisticsTaskRunOutput{ColumnStatisticsTaskRun: run}, nil
}

// defaultGetColumnStatisticsTaskRunsLimit is used when
// GetColumnStatisticsTaskRunsInput.MaxResults is unset.
const defaultGetColumnStatisticsTaskRunsLimit = 100

// getColumnStatisticsTaskRunsInput holds input for GetColumnStatisticsTaskRuns.
type getColumnStatisticsTaskRunsInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
	NextToken    string `json:"NextToken,omitempty"`
	MaxResults   int32  `json:"MaxResults,omitempty"`
}

// getColumnStatisticsTaskRunsOutput holds the result for GetColumnStatisticsTaskRuns.
type getColumnStatisticsTaskRunsOutput struct {
	NextToken                string `json:"NextToken,omitempty"`
	ColumnStatisticsTaskRuns []any  `json:"ColumnStatisticsTaskRuns"`
}

func (h *Handler) handleGetColumnStatisticsTaskRuns(
	_ context.Context,
	in *getColumnStatisticsTaskRunsInput,
) (*getColumnStatisticsTaskRunsOutput, error) {
	all := h.Backend.GetColumnStatisticsTaskRuns()

	matching := make([]*ColumnStatisticsTaskRun, 0, len(all))

	for _, r := range all {
		if r.DatabaseName == in.DatabaseName && r.TableName == in.TableName {
			matching = append(matching, r)
		}
	}

	limit := int(in.MaxResults)
	if limit <= 0 {
		limit = defaultGetColumnStatisticsTaskRunsLimit
	}

	page, next := paginateSlice(matching, in.NextToken, limit)

	result := make([]any, 0, len(page))
	for _, r := range page {
		result = append(result, r)
	}

	return &getColumnStatisticsTaskRunsOutput{ColumnStatisticsTaskRuns: result, NextToken: next}, nil
}

// getColumnStatisticsTaskSettingsInput holds input for GetColumnStatisticsTaskSettings.
type getColumnStatisticsTaskSettingsInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
}

// getColumnStatisticsTaskSettingsOutput holds the result for GetColumnStatisticsTaskSettings.
type getColumnStatisticsTaskSettingsOutput struct {
	ColumnStatisticsTaskSettings any `json:"ColumnStatisticsTaskSettings"`
}

func (h *Handler) handleGetColumnStatisticsTaskSettings(
	_ context.Context,
	in *getColumnStatisticsTaskSettingsInput,
) (*getColumnStatisticsTaskSettingsOutput, error) {
	s, _ := h.Backend.GetColumnStatisticsTaskSettings(in.DatabaseName, in.TableName)

	return &getColumnStatisticsTaskSettingsOutput{ColumnStatisticsTaskSettings: s}, nil
}

// defaultListColumnStatisticsTaskRunsLimit is used when
// ListColumnStatisticsTaskRunsInput.MaxResults is unset.
const defaultListColumnStatisticsTaskRunsLimit = 100

// listColumnStatisticsTaskRunsInput holds input for ListColumnStatisticsTaskRuns.
type listColumnStatisticsTaskRunsInput struct {
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int32  `json:"MaxResults,omitempty"`
}

// listColumnStatisticsTaskRunsOutput holds the result for ListColumnStatisticsTaskRuns.
type listColumnStatisticsTaskRunsOutput struct {
	NextToken                  string   `json:"NextToken,omitempty"`
	ColumnStatisticsTaskRunIDs []string `json:"ColumnStatisticsTaskRunIds"`
}

func (h *Handler) handleListColumnStatisticsTaskRuns(
	_ context.Context,
	in *listColumnStatisticsTaskRunsInput,
) (*listColumnStatisticsTaskRunsOutput, error) {
	all := h.Backend.ListColumnStatisticsTaskRuns()

	limit := int(in.MaxResults)
	if limit <= 0 {
		limit = defaultListColumnStatisticsTaskRunsLimit
	}

	runs, next := paginateSlice(all, in.NextToken, limit)

	ids := make([]string, 0, len(runs))
	for _, r := range runs {
		ids = append(ids, r.ColumnStatisticsTaskRunID)
	}

	return &listColumnStatisticsTaskRunsOutput{ColumnStatisticsTaskRunIDs: ids, NextToken: next}, nil
}

// startColumnStatisticsTaskRunInput holds input for StartColumnStatisticsTaskRun.
//
// CatalogID (real member name is "CatalogID", not "CatalogId" --
// glue@v1.152.0 api_op_StartColumnStatisticsTaskRun.go), ColumnNameList,
// SampleSize, and SecurityConfiguration are not modeled: this backend has no
// per-column sampling/encryption-scoped state to honor them against --
// accepted on the wire and otherwise inert. Role is real and required
// (ColumnStatisticsTaskRun.Role, models.go).
type startColumnStatisticsTaskRunInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
	Role         string `json:"Role"`
}

// startColumnStatisticsTaskRunOutput holds the result for StartColumnStatisticsTaskRun.
type startColumnStatisticsTaskRunOutput struct {
	ColumnStatisticsTaskRunID string `json:"ColumnStatisticsTaskRunId"`
}

func (h *Handler) handleStartColumnStatisticsTaskRun(
	_ context.Context,
	in *startColumnStatisticsTaskRunInput,
) (*startColumnStatisticsTaskRunOutput, error) {
	if in.Role == "" {
		return nil, fmt.Errorf("%w: Role is required", ErrValidation)
	}

	run, err := h.Backend.StartColumnStatisticsTaskRun(in.DatabaseName, in.TableName, in.Role)
	if err != nil {
		return nil, err
	}

	return &startColumnStatisticsTaskRunOutput{
		ColumnStatisticsTaskRunID: run.ColumnStatisticsTaskRunID,
	}, nil
}

// startColumnStatisticsTaskRunScheduleInput holds input for StartColumnStatisticsTaskRunSchedule.
type startColumnStatisticsTaskRunScheduleInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
}

func (h *Handler) handleStartColumnStatisticsTaskRunSchedule(
	_ context.Context,
	in *startColumnStatisticsTaskRunScheduleInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.StartColumnStatisticsTaskRunSchedule(in.DatabaseName, in.TableName)
}

// stopColumnStatisticsTaskRunInput holds input for StopColumnStatisticsTaskRun.
// The real StopColumnStatisticsTaskRunInput (glue@v1.152.0
// api_op_StopColumnStatisticsTaskRun.go) identifies the run by
// DatabaseName+TableName, not a run ID -- there is no
// ColumnStatisticsTaskRunId member on this op at all.
type stopColumnStatisticsTaskRunInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
}

func (h *Handler) handleStopColumnStatisticsTaskRun(
	_ context.Context,
	in *stopColumnStatisticsTaskRunInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.StopColumnStatisticsTaskRun(in.DatabaseName, in.TableName)
}

// stopColumnStatisticsTaskRunScheduleInput holds input for StopColumnStatisticsTaskRunSchedule.
type stopColumnStatisticsTaskRunScheduleInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
}

func (h *Handler) handleStopColumnStatisticsTaskRunSchedule(
	_ context.Context,
	in *stopColumnStatisticsTaskRunScheduleInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.StopColumnStatisticsTaskRunSchedule(in.DatabaseName, in.TableName)
}

// updateColumnStatisticsForPartitionInput holds input for UpdateColumnStatisticsForPartition.
type updateColumnStatisticsForPartitionInput struct {
	DatabaseName         string              `json:"DatabaseName"`
	TableName            string              `json:"TableName"`
	PartitionValues      []string            `json:"PartitionValues"`
	ColumnStatisticsList []*ColumnStatistics `json:"ColumnStatisticsList"`
}

// updateColumnStatisticsForPartitionOutput holds the result for UpdateColumnStatisticsForPartition.
type updateColumnStatisticsForPartitionOutput struct {
	Errors []any `json:"Errors"`
}

func (h *Handler) handleUpdateColumnStatisticsForPartition(
	_ context.Context,
	in *updateColumnStatisticsForPartitionInput,
) (*updateColumnStatisticsForPartitionOutput, error) {
	err := h.Backend.UpdateColumnStatisticsForPartition(
		in.DatabaseName,
		in.TableName,
		in.PartitionValues,
		in.ColumnStatisticsList,
	)
	if err != nil {
		return nil, err
	}

	return &updateColumnStatisticsForPartitionOutput{Errors: []any{}}, nil
}

// updateColumnStatisticsForTableInput holds input for UpdateColumnStatisticsForTable.
type updateColumnStatisticsForTableInput struct {
	DatabaseName         string              `json:"DatabaseName"`
	TableName            string              `json:"TableName"`
	ColumnStatisticsList []*ColumnStatistics `json:"ColumnStatisticsList"`
}

// updateColumnStatisticsForTableOutput holds the result for UpdateColumnStatisticsForTable.
type updateColumnStatisticsForTableOutput struct {
	Errors []any `json:"Errors"`
}

func (h *Handler) handleUpdateColumnStatisticsForTable(
	_ context.Context,
	in *updateColumnStatisticsForTableInput,
) (*updateColumnStatisticsForTableOutput, error) {
	err := h.Backend.UpdateColumnStatisticsForTable(
		in.DatabaseName,
		in.TableName,
		in.ColumnStatisticsList,
	)
	if err != nil {
		return nil, err
	}

	return &updateColumnStatisticsForTableOutput{Errors: []any{}}, nil
}

// updateColumnStatisticsTaskSettingsInput holds input for UpdateColumnStatisticsTaskSettings.
type updateColumnStatisticsTaskSettingsInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
	RoleArn      string `json:"Role,omitempty"`
}

func (h *Handler) handleUpdateColumnStatisticsTaskSettings(
	_ context.Context,
	in *updateColumnStatisticsTaskSettingsInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.UpdateColumnStatisticsTaskSettings(
		in.DatabaseName, in.TableName, in.RoleArn,
	)
}
