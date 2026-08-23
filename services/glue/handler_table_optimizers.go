package glue

import (
	"context"
	"fmt"
)

// batchGetTableOptimizerInput holds input for BatchGetTableOptimizer.
type batchGetTableOptimizerInput struct {
	Entries []BatchGetTableOptimizerEntry `json:"Entries"`
}

// batchGetTableOptimizerOutput holds the result for BatchGetTableOptimizer.
type batchGetTableOptimizerOutput struct {
	TableOptimizers []*BatchTableOptimizer        `json:"TableOptimizers"`
	Failures        []BatchGetTableOptimizerError `json:"Failures"`
}

func (h *Handler) handleBatchGetTableOptimizer(
	_ context.Context,
	in *batchGetTableOptimizerInput,
) (*batchGetTableOptimizerOutput, error) {
	found, errs := h.Backend.BatchGetTableOptimizer(in.Entries)
	if found == nil {
		found = []*BatchTableOptimizer{}
	}
	if errs == nil {
		errs = []BatchGetTableOptimizerError{}
	}

	return &batchGetTableOptimizerOutput{TableOptimizers: found, Failures: errs}, nil
}

// createTableOptimizerInput holds input for CreateTableOptimizer.
type createTableOptimizerInput struct {
	CatalogID                   string                      `json:"CatalogId,omitempty"`
	DatabaseName                string                      `json:"DatabaseName"`
	TableName                   string                      `json:"TableName"`
	Type                        string                      `json:"Type"`
	TableOptimizerConfiguration TableOptimizerConfiguration `json:"TableOptimizerConfiguration"`
}

func (h *Handler) handleCreateTableOptimizer(
	_ context.Context,
	in *createTableOptimizerInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.CreateTableOptimizer(
		in.CatalogID,
		in.DatabaseName,
		in.TableName,
		in.Type,
		in.TableOptimizerConfiguration,
	)
}

// deleteTableOptimizerInput holds input for DeleteTableOptimizer.
type deleteTableOptimizerInput struct {
	CatalogID    string `json:"CatalogId,omitempty"`
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
	Type         string `json:"Type"`
}

func (h *Handler) handleDeleteTableOptimizer(
	_ context.Context,
	in *deleteTableOptimizerInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteTableOptimizer(in.DatabaseName, in.TableName, in.Type)
}

// getTableOptimizerInput holds input for GetTableOptimizer.
type getTableOptimizerInput struct {
	CatalogID    string `json:"CatalogId,omitempty"`
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
	Type         string `json:"Type"`
}

// getTableOptimizerOutput holds the result for GetTableOptimizer.
type getTableOptimizerOutput struct {
	TableOptimizer *TableOptimizer `json:"TableOptimizer"`
	CatalogID      string          `json:"CatalogId,omitempty"`
	DatabaseName   string          `json:"DatabaseName,omitempty"`
	TableName      string          `json:"TableName,omitempty"`
}

func (h *Handler) handleGetTableOptimizer(
	_ context.Context,
	in *getTableOptimizerInput,
) (*getTableOptimizerOutput, error) {
	to, err := h.Backend.GetTableOptimizer(in.DatabaseName, in.TableName, in.Type)
	if err != nil {
		return nil, err
	}

	return &getTableOptimizerOutput{
		TableOptimizer: to,
		CatalogID:      in.CatalogID,
		DatabaseName:   in.DatabaseName,
		TableName:      in.TableName,
	}, nil
}

// listTableOptimizerRunsInput holds input for ListTableOptimizerRuns.
type listTableOptimizerRunsInput struct {
	CatalogID    string `json:"CatalogId,omitempty"`
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
	Type         string `json:"Type"`
	NextToken    string `json:"NextToken,omitempty"`
	MaxResults   int32  `json:"MaxResults,omitempty"`
}

// listTableOptimizerRunsOutput holds the result for ListTableOptimizerRuns.
type listTableOptimizerRunsOutput struct {
	CatalogID          string               `json:"CatalogId,omitempty"`
	DatabaseName       string               `json:"DatabaseName,omitempty"`
	TableName          string               `json:"TableName,omitempty"`
	NextToken          string               `json:"NextToken,omitempty"`
	TableOptimizerRuns []*TableOptimizerRun `json:"TableOptimizerRuns"`
}

func (h *Handler) handleListTableOptimizerRuns(
	_ context.Context,
	in *listTableOptimizerRunsInput,
) (*listTableOptimizerRunsOutput, error) {
	if in.DatabaseName == "" || in.TableName == "" || in.Type == "" {
		return nil, fmt.Errorf("%w: DatabaseName, TableName, and Type are required", ErrValidation)
	}

	to, err := h.Backend.GetTableOptimizer(in.DatabaseName, in.TableName, in.Type)
	if err != nil {
		return nil, err
	}

	runs := []*TableOptimizerRun{}
	if to.LastRun != nil {
		runs = append(runs, to.LastRun)
	}

	return &listTableOptimizerRunsOutput{
		CatalogID:          in.CatalogID,
		DatabaseName:       in.DatabaseName,
		TableName:          in.TableName,
		TableOptimizerRuns: runs,
	}, nil
}

// updateTableOptimizerInput holds input for UpdateTableOptimizer.
type updateTableOptimizerInput struct {
	CatalogID                   string                      `json:"CatalogId,omitempty"`
	DatabaseName                string                      `json:"DatabaseName"`
	TableName                   string                      `json:"TableName"`
	Type                        string                      `json:"Type"`
	TableOptimizerConfiguration TableOptimizerConfiguration `json:"TableOptimizerConfiguration"`
}

func (h *Handler) handleUpdateTableOptimizer(
	_ context.Context,
	in *updateTableOptimizerInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.UpdateTableOptimizer(
		in.DatabaseName,
		in.TableName,
		in.Type,
		in.TableOptimizerConfiguration,
	)
}
