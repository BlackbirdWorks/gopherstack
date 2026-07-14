package glue

import (
	"context"
	"fmt"
	"regexp"
)

type createTableInput struct {
	DatabaseName string     `json:"DatabaseName"`
	TableInput   TableInput `json:"TableInput"`
}

func (h *Handler) handleCreateTable(_ context.Context, in *createTableInput) (*emptyOutput, error) {
	if _, err := h.Backend.CreateTable(in.DatabaseName, in.TableInput); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type getTableInput struct {
	DatabaseName string `json:"DatabaseName"`
	Name         string `json:"Name"`
}

type getTableOutput struct {
	Table *Table `json:"Table"`
}

func (h *Handler) handleGetTable(_ context.Context, in *getTableInput) (*getTableOutput, error) {
	t, err := h.Backend.GetTable(in.DatabaseName, in.Name)
	if err != nil {
		return nil, err
	}

	return &getTableOutput{Table: t}, nil
}

// maxGetTablesResults is the AWS-enforced upper bound for GetTables MaxResults.
const maxGetTablesResults = 100

type getTablesInput struct {
	DatabaseName string `json:"DatabaseName"`
	Expression   string `json:"Expression,omitempty"`
	MaxResults   *int32 `json:"MaxResults,omitempty"`
	NextToken    string `json:"NextToken,omitempty"`
}

type getTablesOutput struct {
	NextToken string   `json:"NextToken,omitempty"`
	TableList []*Table `json:"TableList"`
}

func (h *Handler) handleGetTables(_ context.Context, in *getTablesInput) (*getTablesOutput, error) {
	if in.MaxResults != nil && (*in.MaxResults < 1 || *in.MaxResults > maxGetTablesResults) {
		return nil, fmt.Errorf("%w: MaxResults must be between 1 and %d", ErrValidation, maxGetTablesResults)
	}

	tables, err := h.Backend.GetTables(in.DatabaseName)
	if err != nil {
		return nil, err
	}

	if in.Expression != "" {
		var re *regexp.Regexp

		re, err = tableNameRegexp(in.Expression)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid Expression: %w", ErrValidation, err)
		}

		filtered := tables[:0]
		for _, tbl := range tables {
			if re.MatchString(tbl.Name) {
				filtered = append(filtered, tbl)
			}
		}

		tables = filtered
	}

	limit := maxGetTablesResults
	if in.MaxResults != nil {
		limit = int(*in.MaxResults)
	}

	page, next := paginateSlice(tables, in.NextToken, limit)

	return &getTablesOutput{TableList: page, NextToken: next}, nil
}

type updateTableInput struct {
	DatabaseName string     `json:"DatabaseName"`
	TableInput   TableInput `json:"TableInput"`
}

func (h *Handler) handleUpdateTable(_ context.Context, in *updateTableInput) (*emptyOutput, error) {
	if err := h.Backend.UpdateTable(in.DatabaseName, in.TableInput); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type deleteTableInput struct {
	DatabaseName string `json:"DatabaseName"`
	Name         string `json:"Name"`
}

func (h *Handler) handleDeleteTable(_ context.Context, in *deleteTableInput) (*emptyOutput, error) {
	if err := h.Backend.DeleteTable(in.DatabaseName, in.Name); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type batchDeleteTableInput struct {
	DatabaseName   string   `json:"DatabaseName"`
	TablesToDelete []string `json:"TablesToDelete"`
}

type batchDeleteTableOutput struct {
	Errors []TableError `json:"Errors"`
}

func (h *Handler) handleBatchDeleteTable(
	_ context.Context,
	in *batchDeleteTableInput,
) (*batchDeleteTableOutput, error) {
	errs := h.Backend.BatchDeleteTable(in.DatabaseName, in.TablesToDelete)

	return &batchDeleteTableOutput{Errors: errs}, nil
}

type batchDeleteTableVersionInput struct {
	DatabaseName string   `json:"DatabaseName"`
	TableName    string   `json:"TableName"`
	VersionIDs   []string `json:"VersionIds"`
}

type batchDeleteTableVersionOutput struct {
	Errors []TableVersionError `json:"Errors"`
}

func (h *Handler) handleBatchDeleteTableVersion(
	_ context.Context,
	in *batchDeleteTableVersionInput,
) (*batchDeleteTableVersionOutput, error) {
	errs := h.Backend.BatchDeleteTableVersion(in.DatabaseName, in.TableName, in.VersionIDs)

	return &batchDeleteTableVersionOutput{Errors: errs}, nil
}

// deleteTableVersionInput holds input for DeleteTableVersion.
type deleteTableVersionInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
	VersionID    string `json:"VersionId"`
}

func (h *Handler) handleDeleteTableVersion(
	_ context.Context,
	in *deleteTableVersionInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteTableVersion(in.DatabaseName, in.TableName, in.VersionID)
}

// getTableVersionInput holds input for GetTableVersion.
type getTableVersionInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
	VersionID    string `json:"VersionId"`
}

// getTableVersionOutput holds the result for GetTableVersion.
type getTableVersionOutput struct {
	TableVersion *TableVersion `json:"TableVersion"`
}

func (h *Handler) handleGetTableVersion(
	_ context.Context,
	in *getTableVersionInput,
) (*getTableVersionOutput, error) {
	tv, err := h.Backend.GetTableVersion(in.DatabaseName, in.TableName, in.VersionID)
	if err != nil {
		return nil, err
	}

	return &getTableVersionOutput{TableVersion: tv}, nil
}

// getTableVersionsInput holds input for GetTableVersions.
type getTableVersionsInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
}

// getTableVersionsOutput holds the result for GetTableVersions.
type getTableVersionsOutput struct {
	TableVersions []*TableVersion `json:"TableVersions"`
}

func (h *Handler) handleGetTableVersions(
	_ context.Context,
	in *getTableVersionsInput,
) (*getTableVersionsOutput, error) {
	versions := h.Backend.GetTableVersions(in.DatabaseName, in.TableName)

	return &getTableVersionsOutput{TableVersions: versions}, nil
}

// getUnfilteredTableMetadataInput holds input for GetUnfilteredTableMetadata.
type getUnfilteredTableMetadataInput struct {
	DatabaseName             string   `json:"DatabaseName"`
	Name                     string   `json:"Name"`
	SupportedPermissionTypes []string `json:"SupportedPermissionTypes,omitempty"`
}

// getUnfilteredTableMetadataOutput holds the result for GetUnfilteredTableMetadata.
type getUnfilteredTableMetadataOutput struct {
	Table                         *Table   `json:"Table"`
	AuthorizedColumns             []string `json:"AuthorizedColumns"`
	IsRegisteredWithLakeFormation bool     `json:"IsRegisteredWithLakeFormation"`
}

func (h *Handler) handleGetUnfilteredTableMetadata(
	_ context.Context,
	in *getUnfilteredTableMetadataInput,
) (*getUnfilteredTableMetadataOutput, error) {
	if in.DatabaseName == "" || in.Name == "" {
		return &getUnfilteredTableMetadataOutput{AuthorizedColumns: []string{}}, nil
	}

	tbl, err := h.Backend.GetTable(in.DatabaseName, in.Name)
	if err != nil {
		return nil, err
	}

	return &getUnfilteredTableMetadataOutput{
		Table:             tbl,
		AuthorizedColumns: []string{},
	}, nil
}

// searchTablesInput holds input for SearchTables.
type searchTablesInput struct {
	SearchText string `json:"SearchText,omitempty"`
}

// searchTablesOutput holds the result for SearchTables.
type searchTablesOutput struct {
	TableList []*Table `json:"TableList"`
}

func (h *Handler) handleSearchTables(
	_ context.Context,
	in *searchTablesInput,
) (*searchTablesOutput, error) {
	tables := h.Backend.SearchTables(in.SearchText)

	return &searchTablesOutput{TableList: tables}, nil
}
