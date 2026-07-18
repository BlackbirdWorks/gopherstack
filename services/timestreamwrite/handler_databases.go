package timestreamwrite

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type createDatabaseInput struct {
	DatabaseName string     `json:"DatabaseName"`
	KmsKeyID     string     `json:"KmsKeyId,omitempty"`
	Tags         []tagInput `json:"Tags"`
}

type databaseOutput struct {
	Database databaseView `json:"Database"`
}

type databaseView struct {
	Arn             string  `json:"Arn"`
	DatabaseName    string  `json:"DatabaseName"`
	KmsKeyID        string  `json:"KmsKeyId,omitempty"`
	CreationTime    float64 `json:"CreationTime"`
	LastUpdatedTime float64 `json:"LastUpdatedTime"`
	TableCount      int     `json:"TableCount"`
}

type listDatabasesInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listDatabasesOutput struct {
	NextToken string         `json:"NextToken,omitempty"`
	Databases []databaseView `json:"Databases"`
}

type databaseNameInput struct {
	DatabaseName string `json:"DatabaseName"`
}

type updateDatabaseInput struct {
	DatabaseName string `json:"DatabaseName"`
	KmsKeyID     string `json:"KmsKeyId"`
}

func toDatabaseView(db *Database) databaseView {
	return databaseView{
		Arn:             db.ARN,
		CreationTime:    float64(db.CreationTime.Unix()),
		DatabaseName:    db.DatabaseName,
		KmsKeyID:        db.KmsKeyID,
		LastUpdatedTime: float64(db.LastUpdatedTime.Unix()),
		TableCount:      db.TableCount,
	}
}

func (h *Handler) handleCreateDatabase(
	_ context.Context,
	in *createDatabaseInput,
) (*databaseOutput, error) {
	if in.DatabaseName == "" {
		return nil, fmt.Errorf("%w: DatabaseName is required", errInvalidRequest)
	}

	// Validate database name format and length per AWS API constraints.
	if err := validateDatabaseName(in.DatabaseName); err != nil {
		return nil, err
	}

	// Validate tags per AWS API constraints.
	if err := validateTagInputs(in.Tags); err != nil {
		return nil, err
	}

	tags := tagsFromInput(in.Tags)

	db, err := h.Backend.CreateDatabase(in.DatabaseName, in.KmsKeyID, tags)
	if err != nil {
		return nil, err
	}

	return &databaseOutput{Database: toDatabaseView(db)}, nil
}

func (h *Handler) handleDescribeDatabase(
	_ context.Context,
	in *databaseNameInput,
) (*databaseOutput, error) {
	if in.DatabaseName == "" {
		return nil, fmt.Errorf("%w: DatabaseName is required", errInvalidRequest)
	}

	db, err := h.Backend.DescribeDatabase(in.DatabaseName)
	if err != nil {
		return nil, err
	}

	return &databaseOutput{Database: toDatabaseView(db)}, nil
}

func (h *Handler) handleListDatabases(
	_ context.Context,
	in *listDatabasesInput,
) (*listDatabasesOutput, error) {
	dbs := h.Backend.ListDatabases()
	pg := page.New(dbs, in.NextToken, in.MaxResults, defaultTimestreamMaxResults)
	views := make([]databaseView, 0, len(pg.Data))

	for i := range pg.Data {
		views = append(views, toDatabaseView(&pg.Data[i]))
	}

	return &listDatabasesOutput{Databases: views, NextToken: pg.Next}, nil
}

func (h *Handler) handleDeleteDatabase(
	_ context.Context,
	in *databaseNameInput,
) (*emptyOutput, error) {
	if in.DatabaseName == "" {
		return nil, fmt.Errorf("%w: DatabaseName is required", errInvalidRequest)
	}

	// AWS API requires all tables to be deleted before a database can be deleted.
	tbls, err := h.Backend.ListTables(in.DatabaseName)
	if err != nil {
		// Propagate not-found and other backend errors.
		return nil, err
	}

	if len(tbls) > 0 {
		return nil, fmt.Errorf(
			"%w: database %q cannot be deleted: it still contains %d table(s); "+
				"delete all tables before deleting the database",
			errInvalidRequest, in.DatabaseName, len(tbls),
		)
	}

	err = h.Backend.DeleteDatabase(in.DatabaseName)
	if err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

func (h *Handler) handleUpdateDatabase(
	_ context.Context,
	in *updateDatabaseInput,
) (*databaseOutput, error) {
	if in.DatabaseName == "" {
		return nil, fmt.Errorf("%w: DatabaseName is required", errInvalidRequest)
	}

	db, err := h.Backend.UpdateDatabase(in.DatabaseName, in.KmsKeyID)
	if err != nil {
		return nil, err
	}

	return &databaseOutput{Database: toDatabaseView(db)}, nil
}
