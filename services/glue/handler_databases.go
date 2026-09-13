package glue

import (
	"context"
	"fmt"
)

type createDatabaseInput struct {
	Tags          map[string]string `json:"Tags,omitempty"`
	CatalogID     string            `json:"CatalogId,omitempty"`
	DatabaseInput DatabaseInput     `json:"DatabaseInput"`
}

func (h *Handler) handleCreateDatabase(_ context.Context, in *createDatabaseInput) (*emptyOutput, error) {
	dbInput := in.DatabaseInput
	dbInput.CatalogID = in.CatalogID

	if _, err := h.Backend.CreateDatabase(dbInput, in.Tags); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type getDatabaseInput struct {
	Name      string `json:"Name"`
	CatalogID string `json:"CatalogId,omitempty"`
}

type getDatabaseOutput struct {
	Database *databaseWire `json:"Database"`
}

func (h *Handler) handleGetDatabase(_ context.Context, in *getDatabaseInput) (*getDatabaseOutput, error) {
	db, err := h.Backend.GetDatabase(in.Name)
	if err != nil {
		return nil, err
	}

	if catalogIDMismatch(in.CatalogID, db.CatalogID) {
		return nil, ErrNotFound
	}

	return &getDatabaseOutput{Database: toDatabaseWire(db)}, nil
}

// maxGetDatabasesResults is the AWS-enforced upper bound for GetDatabases MaxResults.
const maxGetDatabasesResults = 100

type getDatabasesInput struct {
	MaxResults *int32 `json:"MaxResults,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
	CatalogID  string `json:"CatalogId,omitempty"`
}

type getDatabasesOutput struct {
	NextToken    string          `json:"NextToken,omitempty"`
	DatabaseList []*databaseWire `json:"DatabaseList"`
}

func (h *Handler) handleGetDatabases(_ context.Context, in *getDatabasesInput) (*getDatabasesOutput, error) {
	if in.MaxResults != nil && (*in.MaxResults < 1 || *in.MaxResults > maxGetDatabasesResults) {
		return nil, fmt.Errorf("%w: MaxResults must be between 1 and %d", ErrValidation, maxGetDatabasesResults)
	}

	dbs := h.Backend.GetDatabases()

	if in.CatalogID != "" {
		filtered := dbs[:0]

		for _, db := range dbs {
			if db.CatalogID == in.CatalogID {
				filtered = append(filtered, db)
			}
		}

		dbs = filtered
	}

	limit := maxGetDatabasesResults
	if in.MaxResults != nil {
		limit = int(*in.MaxResults)
	}

	page, next := paginateSlice(dbs, in.NextToken, limit)

	return &getDatabasesOutput{DatabaseList: toDatabaseWireList(page), NextToken: next}, nil
}

type updateDatabaseInput struct {
	Name          string        `json:"Name"`
	CatalogID     string        `json:"CatalogId,omitempty"`
	DatabaseInput DatabaseInput `json:"DatabaseInput"`
}

func (h *Handler) handleUpdateDatabase(_ context.Context, in *updateDatabaseInput) (*emptyOutput, error) {
	if in.CatalogID != "" {
		existing, err := h.Backend.GetDatabase(in.Name)
		if err != nil {
			return nil, err
		}

		if catalogIDMismatch(in.CatalogID, existing.CatalogID) {
			return nil, ErrNotFound
		}
	}

	if err := h.Backend.UpdateDatabase(in.Name, in.DatabaseInput); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type deleteDatabaseInput struct {
	Name      string `json:"Name"`
	CatalogID string `json:"CatalogId,omitempty"`
}

func (h *Handler) handleDeleteDatabase(_ context.Context, in *deleteDatabaseInput) (*emptyOutput, error) {
	if in.CatalogID != "" {
		existing, err := h.Backend.GetDatabase(in.Name)
		if err != nil {
			return nil, err
		}

		if catalogIDMismatch(in.CatalogID, existing.CatalogID) {
			return nil, ErrNotFound
		}
	}

	if err := h.Backend.DeleteDatabase(in.Name); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}
