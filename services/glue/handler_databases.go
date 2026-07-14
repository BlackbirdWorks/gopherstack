package glue

import (
	"context"
	"fmt"
)

type createDatabaseInput struct {
	Tags          map[string]string `json:"Tags,omitempty"`
	DatabaseInput DatabaseInput     `json:"DatabaseInput"`
}

func (h *Handler) handleCreateDatabase(_ context.Context, in *createDatabaseInput) (*emptyOutput, error) {
	if _, err := h.Backend.CreateDatabase(in.DatabaseInput, in.Tags); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type getDatabaseInput struct {
	Name string `json:"Name"`
}

type getDatabaseOutput struct {
	Database *Database `json:"Database"`
}

func (h *Handler) handleGetDatabase(_ context.Context, in *getDatabaseInput) (*getDatabaseOutput, error) {
	db, err := h.Backend.GetDatabase(in.Name)
	if err != nil {
		return nil, err
	}

	return &getDatabaseOutput{Database: db}, nil
}

// maxGetDatabasesResults is the AWS-enforced upper bound for GetDatabases MaxResults.
const maxGetDatabasesResults = 100

type getDatabasesInput struct {
	MaxResults *int32 `json:"MaxResults,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

type getDatabasesOutput struct {
	NextToken    string      `json:"NextToken,omitempty"`
	DatabaseList []*Database `json:"DatabaseList"`
}

func (h *Handler) handleGetDatabases(_ context.Context, in *getDatabasesInput) (*getDatabasesOutput, error) {
	if in.MaxResults != nil && (*in.MaxResults < 1 || *in.MaxResults > maxGetDatabasesResults) {
		return nil, fmt.Errorf("%w: MaxResults must be between 1 and %d", ErrValidation, maxGetDatabasesResults)
	}

	dbs := h.Backend.GetDatabases()

	limit := maxGetDatabasesResults
	if in.MaxResults != nil {
		limit = int(*in.MaxResults)
	}

	page, next := paginateSlice(dbs, in.NextToken, limit)

	return &getDatabasesOutput{DatabaseList: page, NextToken: next}, nil
}

type updateDatabaseInput struct {
	Name          string        `json:"Name"`
	DatabaseInput DatabaseInput `json:"DatabaseInput"`
}

func (h *Handler) handleUpdateDatabase(_ context.Context, in *updateDatabaseInput) (*emptyOutput, error) {
	if err := h.Backend.UpdateDatabase(in.Name, in.DatabaseInput); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type deleteDatabaseInput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteDatabase(_ context.Context, in *deleteDatabaseInput) (*emptyOutput, error) {
	if err := h.Backend.DeleteDatabase(in.Name); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}
