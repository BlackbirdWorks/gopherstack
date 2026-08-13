package dms

import (
	"context"
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type createDataMigrationInput struct {
	DataMigrationName          *string    `json:"DataMigrationName"`
	MigrationProjectIdentifier *string    `json:"MigrationProjectIdentifier"`
	DataMigrationType          *string    `json:"DataMigrationType"`
	ServiceAccessRoleArn       *string    `json:"ServiceAccessRoleArn"`
	SelectionRules             *string    `json:"SelectionRules"`
	NumberOfJobs               *int32     `json:"NumberOfJobs"`
	EnableCloudwatchLogs       *bool      `json:"EnableCloudwatchLogs"`
	Tags                       []tagEntry `json:"Tags"`
}

type dataMigrationJSON struct {
	DataMigrationName    string `json:"DataMigrationName"`
	DataMigrationArn     string `json:"DataMigrationArn"`
	MigrationProjectArn  string `json:"MigrationProjectArn"`
	DataMigrationType    string `json:"DataMigrationType"`
	ServiceAccessRoleArn string `json:"ServiceAccessRoleArn"`
	DataMigrationStatus  string `json:"DataMigrationStatus"`
	NumberOfJobs         int32  `json:"NumberOfJobs"`
	EnableCloudwatchLogs bool   `json:"EnableCloudwatchLogs"`
}

type createDataMigrationOutput struct {
	DataMigration dataMigrationJSON `json:"DataMigration"`
}

func (h *Handler) handleCreateDataMigration(
	ctx context.Context, in *createDataMigrationInput,
) (*createDataMigrationOutput, error) {
	name := ptrconv.String(in.DataMigrationName)
	if name == "" {
		return nil, fmt.Errorf("%w: DataMigrationName is required", ErrValidation)
	}

	migrationType := ptrconv.String(in.DataMigrationType)
	if migrationType == "" {
		return nil, fmt.Errorf("%w: DataMigrationType is required", ErrValidation)
	}

	kv := tagsToMap(in.Tags)
	dm, err := h.Backend.CreateDataMigration(
		ctx,
		name,
		ptrconv.String(in.MigrationProjectIdentifier),
		migrationType,
		ptrconv.String(in.ServiceAccessRoleArn),
		ptrconv.String(in.SelectionRules),
		ptrInt32(in.NumberOfJobs),
		ptrconv.Bool(in.EnableCloudwatchLogs),
		kv,
	)
	if err != nil {
		return nil, err
	}

	return &createDataMigrationOutput{DataMigration: dmToJSON(dm)}, nil
}

func dmToJSON(dm *DataMigration) dataMigrationJSON {
	return dataMigrationJSON{
		DataMigrationName:    dm.DataMigrationName,
		DataMigrationArn:     dm.DataMigrationArn,
		MigrationProjectArn:  dm.MigrationProjectArn,
		DataMigrationType:    dm.DataMigrationType,
		ServiceAccessRoleArn: dm.ServiceAccessRoleArn,
		DataMigrationStatus:  dm.DataMigrationStatus,
		NumberOfJobs:         dm.NumberOfJobs,
		EnableCloudwatchLogs: dm.EnableCloudwatchLogs,
	}
}

type deleteDataMigrationInput struct {
	DataMigrationIdentifier *string `json:"DataMigrationIdentifier"`
}

type deleteDataMigrationOutput struct {
	DataMigration dataMigrationJSON `json:"DataMigration"`
}

func (h *Handler) handleDeleteDataMigration(
	ctx context.Context, in *deleteDataMigrationInput,
) (*deleteDataMigrationOutput, error) {
	dm, err := h.Backend.DeleteDataMigration(ctx, ptrconv.String(in.DataMigrationIdentifier))
	if err != nil {
		return nil, err
	}

	return &deleteDataMigrationOutput{DataMigration: dmToJSON(dm)}, nil
}

type describeDataMigrationsInput struct {
	DataMigrationIdentifier *string       `json:"DataMigrationIdentifier"`
	Marker                  *string       `json:"Marker"`
	MaxRecords              *int32        `json:"MaxRecords"`
	Filters                 []filterEntry `json:"Filters"`
}

type describeDataMigrationsOutput struct {
	Marker         *string             `json:"Marker,omitempty"`
	DataMigrations []dataMigrationJSON `json:"DataMigrations"`
}

func (h *Handler) handleDescribeDataMigrations(
	ctx context.Context, in *describeDataMigrationsInput,
) (*describeDataMigrationsOutput, error) {
	identifier := ptrconv.String(in.DataMigrationIdentifier)
	if identifier == "" {
		identifier = extractFilterValue(in.Filters, "data-migration-identifier")
	}

	list, err := h.Backend.DescribeDataMigrations(ctx, identifier)
	if err != nil {
		return nil, err
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].DataMigrationName < list[j].DataMigrationName
	})

	all := make([]dataMigrationJSON, 0, len(list))
	for _, dm := range list {
		all = append(all, dmToJSON(dm))
	}

	data, nextMarker := dmsPaginate(all, in.Marker, in.MaxRecords)

	return &describeDataMigrationsOutput{DataMigrations: data, Marker: nextMarker}, nil
}

type modifyDataMigrationInput struct {
	DataMigrationIdentifier *string `json:"DataMigrationIdentifier"`
	DataMigrationType       *string `json:"DataMigrationType"`
	ServiceAccessRoleArn    *string `json:"ServiceAccessRoleArn"`
	NumberOfJobs            *int32  `json:"NumberOfJobs"`
}

type modifyDataMigrationOutput struct {
	DataMigration dataMigrationJSON `json:"DataMigration"`
}

func (h *Handler) handleModifyDataMigration(
	ctx context.Context, in *modifyDataMigrationInput,
) (*modifyDataMigrationOutput, error) {
	dm, err := h.Backend.ModifyDataMigration(
		ctx,
		ptrconv.String(in.DataMigrationIdentifier),
		ptrconv.String(in.DataMigrationType),
		ptrconv.String(in.ServiceAccessRoleArn),
		in.NumberOfJobs,
	)
	if err != nil {
		return nil, err
	}

	return &modifyDataMigrationOutput{DataMigration: dmToJSON(dm)}, nil
}

type startDataMigrationInput struct {
	DataMigrationIdentifier *string `json:"DataMigrationIdentifier"`
	StartType               *string `json:"StartType"`
}

type startDataMigrationOutput struct {
	DataMigration dataMigrationJSON `json:"DataMigration"`
}

func (h *Handler) handleStartDataMigration(
	ctx context.Context, in *startDataMigrationInput,
) (*startDataMigrationOutput, error) {
	dm, err := h.Backend.StartDataMigration(ctx, ptrconv.String(in.DataMigrationIdentifier))
	if err != nil {
		return nil, err
	}

	return &startDataMigrationOutput{DataMigration: dmToJSON(dm)}, nil
}

type stopDataMigrationInput struct {
	DataMigrationIdentifier *string `json:"DataMigrationIdentifier"`
}

type stopDataMigrationOutput struct {
	DataMigration dataMigrationJSON `json:"DataMigration"`
}

func (h *Handler) handleStopDataMigration(
	ctx context.Context, in *stopDataMigrationInput,
) (*stopDataMigrationOutput, error) {
	dm, err := h.Backend.StopDataMigration(ctx, ptrconv.String(in.DataMigrationIdentifier))
	if err != nil {
		return nil, err
	}

	return &stopDataMigrationOutput{DataMigration: dmToJSON(dm)}, nil
}

// opsDataMigrations returns the dispatch-table entries for the data_migrations operation family.
func (h *Handler) opsDataMigrations() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opCreateDataMigration: service.WrapOp(h.handleCreateDataMigration),
		opDeleteDataMigration: service.WrapOp(h.handleDeleteDataMigration),
		opDescribeDataMigrations: service.WrapOp(
			h.handleDescribeDataMigrations,
		),
		opModifyDataMigration: service.WrapOp(h.handleModifyDataMigration),
		opStartDataMigration:  service.WrapOp(h.handleStartDataMigration),
		opStopDataMigration:   service.WrapOp(h.handleStopDataMigration),
	}
}
