package glue

import (
	"context"
	"fmt"
	"regexp"
)

type createTableInput struct {
	DatabaseName string     `json:"DatabaseName"`
	CatalogID    string     `json:"CatalogId,omitempty"`
	TableInput   TableInput `json:"TableInput"`
}

func (h *Handler) handleCreateTable(_ context.Context, in *createTableInput) (*emptyOutput, error) {
	tableInput := in.TableInput
	tableInput.CatalogID = in.CatalogID

	if _, err := h.Backend.CreateTable(in.DatabaseName, tableInput); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// getTableInput holds input for GetTable.
//
// AttributesToGet (DEFAULT/LATEST_ICEBERG_METADATA) is not modeled: this
// backend has no Iceberg table metadata state to return, so there is
// nothing for the filter to select between -- accepted on the wire and
// otherwise inert (see PARITY.md).
type getTableInput struct {
	DatabaseName    string   `json:"DatabaseName"`
	Name            string   `json:"Name"`
	CatalogID       string   `json:"CatalogId,omitempty"`
	AttributesToGet []string `json:"AttributesToGet,omitempty"`
}

type getTableOutput struct {
	Table *Table `json:"Table"`
}

func (h *Handler) handleGetTable(_ context.Context, in *getTableInput) (*getTableOutput, error) {
	t, err := h.Backend.GetTable(in.DatabaseName, in.Name)
	if err != nil {
		return nil, err
	}

	if catalogIDMismatch(in.CatalogID, t.CatalogID) {
		return nil, ErrNotFound
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
	CatalogID    string `json:"CatalogId,omitempty"`
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

	if in.CatalogID != "" {
		filtered := tables[:0]

		for _, tbl := range tables {
			if tbl.CatalogID == in.CatalogID {
				filtered = append(filtered, tbl)
			}
		}

		tables = filtered
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
	CatalogID    string     `json:"CatalogId,omitempty"`
	TableInput   TableInput `json:"TableInput"`
	SkipArchive  bool       `json:"SkipArchive,omitempty"`
}

func (h *Handler) handleUpdateTable(_ context.Context, in *updateTableInput) (*emptyOutput, error) {
	if in.CatalogID != "" {
		existing, err := h.Backend.GetTable(in.DatabaseName, in.TableInput.Name)
		if err != nil {
			return nil, err
		}

		if catalogIDMismatch(in.CatalogID, existing.CatalogID) {
			return nil, ErrNotFound
		}
	}

	tableInput := in.TableInput
	tableInput.SkipArchive = in.SkipArchive

	if err := h.Backend.UpdateTable(in.DatabaseName, tableInput); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type deleteTableInput struct {
	DatabaseName string `json:"DatabaseName"`
	Name         string `json:"Name"`
	CatalogID    string `json:"CatalogId,omitempty"`
}

func (h *Handler) handleDeleteTable(_ context.Context, in *deleteTableInput) (*emptyOutput, error) {
	if in.CatalogID != "" {
		existing, err := h.Backend.GetTable(in.DatabaseName, in.Name)
		if err != nil {
			return nil, err
		}

		if catalogIDMismatch(in.CatalogID, existing.CatalogID) {
			return nil, ErrNotFound
		}
	}

	if err := h.Backend.DeleteTable(in.DatabaseName, in.Name); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type batchDeleteTableInput struct {
	DatabaseName   string   `json:"DatabaseName"`
	CatalogID      string   `json:"CatalogId,omitempty"`
	TablesToDelete []string `json:"TablesToDelete"`
}

type batchDeleteTableOutput struct {
	Errors []TableError `json:"Errors"`
}

func (h *Handler) handleBatchDeleteTable(
	_ context.Context,
	in *batchDeleteTableInput,
) (*batchDeleteTableOutput, error) {
	toDelete := in.TablesToDelete
	errs := make([]TableError, 0, len(toDelete))

	if in.CatalogID != "" {
		var scoped []string

		for _, name := range toDelete {
			t, err := h.Backend.GetTable(in.DatabaseName, name)
			if err == nil && catalogIDMismatch(in.CatalogID, t.CatalogID) {
				errs = append(errs, TableError{
					TableName:   name,
					ErrorDetail: ErrorDetail{ErrorCode: errEntityNotFoundCode, ErrorMessage: "table not found"},
				})

				continue
			}

			scoped = append(scoped, name)
		}

		toDelete = scoped
	}

	errs = append(errs, h.Backend.BatchDeleteTable(in.DatabaseName, toDelete)...)

	return &batchDeleteTableOutput{Errors: errs}, nil
}

type batchDeleteTableVersionInput struct {
	DatabaseName string   `json:"DatabaseName"`
	TableName    string   `json:"TableName"`
	CatalogID    string   `json:"CatalogId,omitempty"`
	VersionIDs   []string `json:"VersionIds"`
}

type batchDeleteTableVersionOutput struct {
	Errors []TableVersionError `json:"Errors"`
}

func (h *Handler) handleBatchDeleteTableVersion(
	_ context.Context,
	in *batchDeleteTableVersionInput,
) (*batchDeleteTableVersionOutput, error) {
	if in.CatalogID != "" {
		existing, err := h.Backend.GetTable(in.DatabaseName, in.TableName)
		if err != nil {
			return nil, err
		}

		if catalogIDMismatch(in.CatalogID, existing.CatalogID) {
			return nil, ErrNotFound
		}
	}

	errs := h.Backend.BatchDeleteTableVersion(in.DatabaseName, in.TableName, in.VersionIDs)

	return &batchDeleteTableVersionOutput{Errors: errs}, nil
}

// deleteTableVersionInput holds input for DeleteTableVersion.
type deleteTableVersionInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
	VersionID    string `json:"VersionId"`
	CatalogID    string `json:"CatalogId,omitempty"`
}

func (h *Handler) handleDeleteTableVersion(
	_ context.Context,
	in *deleteTableVersionInput,
) (*emptyOutput, error) {
	if in.CatalogID != "" {
		existing, err := h.Backend.GetTable(in.DatabaseName, in.TableName)
		if err != nil {
			return nil, err
		}

		if catalogIDMismatch(in.CatalogID, existing.CatalogID) {
			return nil, ErrNotFound
		}
	}

	return &emptyOutput{}, h.Backend.DeleteTableVersion(in.DatabaseName, in.TableName, in.VersionID)
}

// getTableVersionInput holds input for GetTableVersion.
type getTableVersionInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
	VersionID    string `json:"VersionId"`
	CatalogID    string `json:"CatalogId,omitempty"`
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

	if tv.Table != nil && catalogIDMismatch(in.CatalogID, tv.Table.CatalogID) {
		return nil, ErrNotFound
	}

	return &getTableVersionOutput{TableVersion: tv}, nil
}

// defaultGetTableVersionsLimit is used when GetTableVersionsInput.MaxResults is unset.
const defaultGetTableVersionsLimit = 100

// getTableVersionsInput holds input for GetTableVersions.
//
// MaxResults/NextToken are real GetTableVersionsInput members
// (glue@v1.157.0 api_op_GetTableVersions.go) previously declared nowhere on
// this wire struct, so every call returned every stored version in one
// unbounded response regardless of what a real client requested.
type getTableVersionsInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
	NextToken    string `json:"NextToken,omitempty"`
	CatalogID    string `json:"CatalogId,omitempty"`
	MaxResults   int32  `json:"MaxResults,omitempty"`
}

// getTableVersionsOutput holds the result for GetTableVersions.
type getTableVersionsOutput struct {
	NextToken     string          `json:"NextToken,omitempty"`
	TableVersions []*TableVersion `json:"TableVersions"`
}

func (h *Handler) handleGetTableVersions(
	_ context.Context,
	in *getTableVersionsInput,
) (*getTableVersionsOutput, error) {
	versions := h.Backend.GetTableVersions(in.DatabaseName, in.TableName)

	if in.CatalogID != "" {
		filtered := versions[:0]

		for _, v := range versions {
			if v.Table == nil || v.Table.CatalogID == in.CatalogID {
				filtered = append(filtered, v)
			}
		}

		versions = filtered
	}

	limit := int(in.MaxResults)
	if limit <= 0 {
		limit = defaultGetTableVersionsLimit
	}

	page, next := paginateSlice(versions, in.NextToken, limit)

	return &getTableVersionsOutput{TableVersions: page, NextToken: next}, nil
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
