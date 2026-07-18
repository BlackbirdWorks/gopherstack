package glue

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

type batchCreatePartitionInput struct {
	DatabaseName       string           `json:"DatabaseName"`
	TableName          string           `json:"TableName"`
	PartitionInputList []PartitionInput `json:"PartitionInputList"`
}

type batchCreatePartitionOutput struct {
	Errors     []PartitionError `json:"Errors"`
	Partitions []*Partition     `json:"Partitions"`
}

func (h *Handler) handleBatchCreatePartition(
	_ context.Context,
	in *batchCreatePartitionInput,
) (*batchCreatePartitionOutput, error) {
	if len(in.PartitionInputList) > maxBatchCreatePartitions {
		return nil, fmt.Errorf("%w: too many partitions: maximum is %d", ErrValidation, maxBatchCreatePartitions)
	}

	created, errs := h.Backend.BatchCreatePartition(in.DatabaseName, in.TableName, in.PartitionInputList)

	return &batchCreatePartitionOutput{Partitions: created, Errors: errs}, nil
}

type batchDeletePartitionInput struct {
	DatabaseName       string               `json:"DatabaseName"`
	TableName          string               `json:"TableName"`
	PartitionsToDelete []PartitionValueList `json:"PartitionsToDelete"`
}

type batchDeletePartitionOutput struct {
	Errors []PartitionError `json:"Errors"`
}

func (h *Handler) handleBatchDeletePartition(
	_ context.Context,
	in *batchDeletePartitionInput,
) (*batchDeletePartitionOutput, error) {
	errs := h.Backend.BatchDeletePartition(in.DatabaseName, in.TableName, in.PartitionsToDelete)

	return &batchDeletePartitionOutput{Errors: errs}, nil
}

// awserrFromDetail converts a batch-operation ErrorDetail into the matching
// sentinel error so handleError reports the correct wire exception type.
//
// Batch partition/table ops (BatchCreatePartition, BatchDeletePartition, ...)
// return per-item ErrorDetail values with an AWS exception-name string in
// ErrorCode, but no Go error. The single-item AWS ops (CreatePartition,
// DeletePartition, ...) are implemented by calling the batch backend method
// with a one-element slice and surfacing errs[0] as a real error. Previously
// this always wrapped awserr.ErrNotFound regardless of ErrorCode, so an
// AlreadyExistsException detail (e.g. CreatePartition on a duplicate) was
// reported to the client as EntityNotFoundException instead.
func awserrFromDetail(d ErrorDetail) error {
	msg := d.ErrorCode + ": " + d.ErrorMessage

	switch d.ErrorCode {
	case "AlreadyExistsException":
		return awserr.New(msg, awserr.ErrAlreadyExists)
	case errEntityNotFoundCode:
		return awserr.New(msg, awserr.ErrNotFound)
	default:
		return awserr.New(msg, awserr.ErrInvalidParameter)
	}
}

// batchGetPartitionInput holds input for BatchGetPartition.
type batchGetPartitionInput struct {
	DatabaseName    string               `json:"DatabaseName"`
	TableName       string               `json:"TableName"`
	PartitionsToGet []PartitionValueList `json:"PartitionsToGet"`
}

// batchGetPartitionOutput holds the result for BatchGetPartition.
type batchGetPartitionOutput struct {
	Partitions      []*Partition         `json:"Partitions"`
	UnprocessedKeys []PartitionValueList `json:"UnprocessedKeys"`
}

// handleBatchGetPartition validates that the referenced database/table exist
// before returning, then looks up each requested partition value list against
// real backend state. AWS Glue returns an EntityNotFoundException when the
// table is missing rather than silently returning an empty list; partitions
// that don't exist are reported back in UnprocessedKeys (per the
// BatchGetPartitionResponse shape) rather than causing the whole call to fail.
//
// Previously this always returned an empty Partitions list regardless of what
// partitions actually existed in the backend — a disguised stub masked by a
// stale comment claiming "the mock backend has no partition storage" (it does;
// see InMemoryBackend.GetPartition/GetPartitions in partitions.go).
func (h *Handler) handleBatchGetPartition(
	_ context.Context,
	in *batchGetPartitionInput,
) (*batchGetPartitionOutput, error) {
	if in.DatabaseName == "" || in.TableName == "" {
		return nil, fmt.Errorf("%w: DatabaseName and TableName are required", ErrValidation)
	}

	if _, err := h.Backend.GetTable(in.DatabaseName, in.TableName); err != nil {
		return nil, err
	}

	found := make([]*Partition, 0, len(in.PartitionsToGet))
	unprocessed := make([]PartitionValueList, 0)

	for _, pvl := range in.PartitionsToGet {
		p, err := h.Backend.GetPartition(in.DatabaseName, in.TableName, pvl.Values)
		if err != nil {
			unprocessed = append(unprocessed, pvl)

			continue
		}

		found = append(found, p)
	}

	return &batchGetPartitionOutput{Partitions: found, UnprocessedKeys: unprocessed}, nil
}

// batchUpdatePartitionEntry is a single entry in a BatchUpdatePartition request.
type batchUpdatePartitionEntry struct {
	PartitionValueList []string       `json:"PartitionValueList"`
	PartitionInput     PartitionInput `json:"PartitionInput"`
}

// batchUpdatePartitionInput holds input for BatchUpdatePartition.
type batchUpdatePartitionInput struct {
	DatabaseName string                      `json:"DatabaseName"`
	TableName    string                      `json:"TableName"`
	Entries      []batchUpdatePartitionEntry `json:"Entries"`
}

// batchUpdatePartitionError holds per-entry error detail for BatchUpdatePartition.
type batchUpdatePartitionError struct {
	ErrorDetail        ErrorDetail `json:"ErrorDetail"`
	PartitionValueList []string    `json:"PartitionValueList"`
}

// batchUpdatePartitionOutput holds the result for BatchUpdatePartition.
type batchUpdatePartitionOutput struct {
	Errors []batchUpdatePartitionError `json:"Errors"`
}

func (h *Handler) handleBatchUpdatePartition(
	_ context.Context,
	in *batchUpdatePartitionInput,
) (*batchUpdatePartitionOutput, error) {
	errs := make([]batchUpdatePartitionError, 0, len(in.Entries))

	for _, entry := range in.Entries {
		if err := h.Backend.UpdatePartition(
			in.DatabaseName, in.TableName,
			entry.PartitionValueList, entry.PartitionInput,
		); err != nil {
			errs = append(errs, batchUpdatePartitionError{
				PartitionValueList: entry.PartitionValueList,
				ErrorDetail: ErrorDetail{
					ErrorCode:    errEntityNotFoundCode,
					ErrorMessage: err.Error(),
				},
			})
		}
	}

	return &batchUpdatePartitionOutput{Errors: errs}, nil
}

// createPartitionInput holds input for CreatePartition.
type createPartitionInput struct {
	DatabaseName   string         `json:"DatabaseName"`
	TableName      string         `json:"TableName"`
	PartitionInput PartitionInput `json:"PartitionInput"`
}

func (h *Handler) handleCreatePartition(
	_ context.Context,
	in *createPartitionInput,
) (*emptyOutput, error) {
	_, errs := h.Backend.BatchCreatePartition(
		in.DatabaseName,
		in.TableName,
		[]PartitionInput{in.PartitionInput},
	)
	if len(errs) > 0 {
		return nil, awserrFromDetail(errs[0].ErrorDetail)
	}

	return &emptyOutput{}, nil
}

// deletePartitionInput holds input for DeletePartition.
type deletePartitionInput struct {
	DatabaseName    string   `json:"DatabaseName"`
	TableName       string   `json:"TableName"`
	PartitionValues []string `json:"PartitionValues"`
}

func (h *Handler) handleDeletePartition(
	_ context.Context,
	in *deletePartitionInput,
) (*emptyOutput, error) {
	errs := h.Backend.BatchDeletePartition(
		in.DatabaseName,
		in.TableName,
		[]PartitionValueList{{Values: in.PartitionValues}},
	)
	if len(errs) > 0 {
		return nil, awserrFromDetail(errs[0].ErrorDetail)
	}

	return &emptyOutput{}, nil
}

// getPartitionInput holds input for GetPartition.
type getPartitionInput struct {
	DatabaseName    string   `json:"DatabaseName"`
	TableName       string   `json:"TableName"`
	PartitionValues []string `json:"PartitionValues"`
}

// getPartitionOutput holds the result for GetPartition.
type getPartitionOutput struct {
	Partition *Partition `json:"Partition"`
}

func (h *Handler) handleGetPartition(
	_ context.Context,
	in *getPartitionInput,
) (*getPartitionOutput, error) {
	p, err := h.Backend.GetPartition(in.DatabaseName, in.TableName, in.PartitionValues)
	if err != nil {
		return nil, err
	}

	return &getPartitionOutput{Partition: p}, nil
}

// maxGetPartitionsResults is the AWS-enforced upper bound for GetPartitions MaxResults.
const maxGetPartitionsResults = 1000

// getPartitionsInput holds input for GetPartitions.
type getPartitionsInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
	Expression   string `json:"Expression,omitempty"`
	MaxResults   *int32 `json:"MaxResults,omitempty"`
	NextToken    string `json:"NextToken,omitempty"`
}

// getPartitionsOutput holds the result for GetPartitions.
type getPartitionsOutput struct {
	NextToken  string       `json:"NextToken,omitempty"`
	Partitions []*Partition `json:"Partitions"`
}

func (h *Handler) handleGetPartitions(
	_ context.Context,
	in *getPartitionsInput,
) (*getPartitionsOutput, error) {
	if in.MaxResults != nil && (*in.MaxResults < 1 || *in.MaxResults > maxGetPartitionsResults) {
		return nil, fmt.Errorf(
			"%w: MaxResults must be between 1 and %d",
			ErrValidation,
			maxGetPartitionsResults,
		)
	}

	partitions, err := h.Backend.GetPartitions(in.DatabaseName, in.TableName)
	if err != nil {
		return nil, err
	}

	if in.Expression != "" {
		var tbl *Table

		tbl, err = h.Backend.GetTable(in.DatabaseName, in.TableName)
		if err != nil {
			return nil, err
		}

		keyNames := make([]string, len(tbl.PartitionKeys))
		for i, col := range tbl.PartitionKeys {
			keyNames[i] = col.Name
		}

		var pred partitionExpr

		pred, err = parsePartitionExpr(in.Expression)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid Expression: %w", ErrValidation, err)
		}

		filtered := partitions[:0]
		for _, p := range partitions {
			if pred.eval(keyNames, p.Values) {
				filtered = append(filtered, p)
			}
		}

		partitions = filtered
	}

	limit := maxGetPartitionsResults
	if in.MaxResults != nil {
		limit = int(*in.MaxResults)
	}

	page, next := paginateSlice(partitions, in.NextToken, limit)

	return &getPartitionsOutput{Partitions: page, NextToken: next}, nil
}

// getUnfilteredPartitionMetadataInput holds input for GetUnfilteredPartitionMetadata.
type getUnfilteredPartitionMetadataInput struct {
	DatabaseName             string   `json:"DatabaseName"`
	TableName                string   `json:"TableName"`
	PartitionValues          []string `json:"PartitionValues"`
	SupportedPermissionTypes []string `json:"SupportedPermissionTypes,omitempty"`
}

// getUnfilteredPartitionMetadataOutput holds the result for GetUnfilteredPartitionMetadata.
type getUnfilteredPartitionMetadataOutput struct {
	Partition                     *Partition `json:"Partition"`
	AuthorizedColumns             []string   `json:"AuthorizedColumns"`
	IsRegisteredWithLakeFormation bool       `json:"IsRegisteredWithLakeFormation"`
}

func (h *Handler) handleGetUnfilteredPartitionMetadata(
	_ context.Context,
	in *getUnfilteredPartitionMetadataInput,
) (*getUnfilteredPartitionMetadataOutput, error) {
	if in.DatabaseName == "" || in.TableName == "" {
		return &getUnfilteredPartitionMetadataOutput{AuthorizedColumns: []string{}}, nil
	}

	p, err := h.Backend.GetPartition(in.DatabaseName, in.TableName, in.PartitionValues)
	if err != nil {
		return nil, err
	}

	return &getUnfilteredPartitionMetadataOutput{
		Partition:         p,
		AuthorizedColumns: []string{},
	}, nil
}

// getUnfilteredPartitionsMetadataInput holds input for GetUnfilteredPartitionsMetadata.
type getUnfilteredPartitionsMetadataInput struct {
	DatabaseName             string   `json:"DatabaseName"`
	TableName                string   `json:"TableName"`
	SupportedPermissionTypes []string `json:"SupportedPermissionTypes,omitempty"`
}

// unfilteredPartitionEntry wraps a Partition for the unfiltered metadata response.
type unfilteredPartitionEntry struct {
	Partition                     *Partition `json:"Partition"`
	AuthorizedColumns             []string   `json:"AuthorizedColumns"`
	IsRegisteredWithLakeFormation bool       `json:"IsRegisteredWithLakeFormation"`
}

// getUnfilteredPartitionsMetadataOutput holds the result for GetUnfilteredPartitionsMetadata.
type getUnfilteredPartitionsMetadataOutput struct {
	UnfilteredPartitions []any `json:"UnfilteredPartitions"`
}

func (h *Handler) handleGetUnfilteredPartitionsMetadata(
	_ context.Context,
	in *getUnfilteredPartitionsMetadataInput,
) (*getUnfilteredPartitionsMetadataOutput, error) {
	if in.DatabaseName == "" || in.TableName == "" {
		return &getUnfilteredPartitionsMetadataOutput{UnfilteredPartitions: []any{}}, nil
	}

	partitions, err := h.Backend.GetPartitions(in.DatabaseName, in.TableName)
	if err != nil {
		return nil, err
	}

	result := make([]any, 0, len(partitions))
	for _, p := range partitions {
		result = append(result, unfilteredPartitionEntry{
			Partition:         p,
			AuthorizedColumns: []string{},
		})
	}

	return &getUnfilteredPartitionsMetadataOutput{UnfilteredPartitions: result}, nil
}

// updatePartitionInput holds input for UpdatePartition.
type updatePartitionInput struct {
	DatabaseName       string         `json:"DatabaseName"`
	TableName          string         `json:"TableName"`
	PartitionValueList []string       `json:"PartitionValueList"`
	PartitionInput     PartitionInput `json:"PartitionInput"`
}

func (h *Handler) handleUpdatePartition(
	_ context.Context,
	in *updatePartitionInput,
) (*emptyOutput, error) {
	if err := h.Backend.UpdatePartition(
		in.DatabaseName, in.TableName,
		in.PartitionValueList, in.PartitionInput,
	); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}
