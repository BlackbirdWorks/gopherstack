package dms

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type createDataMigrationInput struct {
	DataMigrationName          *string                 `json:"DataMigrationName"`
	MigrationProjectIdentifier *string                 `json:"MigrationProjectIdentifier"`
	DataMigrationType          *string                 `json:"DataMigrationType"`
	ServiceAccessRoleArn       *string                 `json:"ServiceAccessRoleArn"`
	SelectionRules             *string                 `json:"SelectionRules"`
	NumberOfJobs               *int32                  `json:"NumberOfJobs"`
	EnableCloudwatchLogs       *bool                   `json:"EnableCloudwatchLogs"`
	SourceDataSettings         []sourceDataSettingJSON `json:"SourceDataSettings"`
	TargetDataSettings         []targetDataSettingJSON `json:"TargetDataSettings"`
	Tags                       []tagEntry              `json:"Tags"`
}

type sourceDataSettingJSON struct {
	CDCStartPosition string `json:"CDCStartPosition,omitempty"`
	SlotName         string `json:"SlotName,omitempty"`
	CDCStartTime     string `json:"CDCStartTime,omitempty"`
	CDCStopTime      string `json:"CDCStopTime,omitempty"`
}

type targetDataSettingJSON struct {
	TablePreparationMode string `json:"TablePreparationMode,omitempty"`
}

func sourceDataSettingsFromJSON(in []sourceDataSettingJSON) ([]SourceDataSetting, error) {
	if in == nil {
		return nil, nil
	}

	out := make([]SourceDataSetting, 0, len(in))
	for _, s := range in {
		for _, ts := range []string{s.CDCStartTime, s.CDCStopTime} {
			if _, err := time.Parse(time.RFC3339Nano, ts); ts != "" && err != nil {
				return nil, fmt.Errorf("%w: invalid CDC timestamp %q", ErrValidation, ts)
			}
		}

		out = append(out, SourceDataSetting(s))
	}

	return out, nil
}

func targetDataSettingsFromJSON(in []targetDataSettingJSON) ([]TargetDataSetting, error) {
	if in == nil {
		return nil, nil
	}

	out := make([]TargetDataSetting, 0, len(in))
	for _, s := range in {
		switch s.TablePreparationMode {
		case "", "do-nothing", "truncate", "drop-tables-on-target":
		default:
			return nil, fmt.Errorf("%w: invalid TablePreparationMode %q", ErrValidation, s.TablePreparationMode)
		}

		out = append(out, TargetDataSetting(s))
	}

	return out, nil
}

// dataMigrationSettingsJSON mirrors real AWS's DataMigrationSettings, the
// nested object DataMigration.DataMigrationSettings deserializes into
// (databasemigrationservice@v1.66.4 deserializers.go:16546); NumberOfJobs and
// EnableCloudwatchLogs are flat request-input fields but the response nests
// them under this object, and renames the latter to CloudwatchLogsEnabled.
type dataMigrationSettingsJSON struct {
	SelectionRules        string `json:"SelectionRules,omitempty"`
	NumberOfJobs          int32  `json:"NumberOfJobs"`
	CloudwatchLogsEnabled bool   `json:"CloudwatchLogsEnabled"`
}

type dataMigrationJSON struct {
	DataMigrationSettings *dataMigrationSettingsJSON `json:"DataMigrationSettings,omitempty"`
	DataMigrationName     string                     `json:"DataMigrationName"`
	DataMigrationArn      string                     `json:"DataMigrationArn"`
	MigrationProjectArn   string                     `json:"MigrationProjectArn"`
	DataMigrationType     string                     `json:"DataMigrationType"`
	ServiceAccessRoleArn  string                     `json:"ServiceAccessRoleArn"`
	DataMigrationStatus   string                     `json:"DataMigrationStatus"`
	SourceDataSettings    []sourceDataSettingJSON    `json:"SourceDataSettings,omitempty"`
	TargetDataSettings    []targetDataSettingJSON    `json:"TargetDataSettings,omitempty"`
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

	targets, err := targetDataSettingsFromJSON(in.TargetDataSettings)
	if err != nil {
		return nil, err
	}

	sources, err := sourceDataSettingsFromJSON(in.SourceDataSettings)
	if err != nil {
		return nil, err
	}

	dm, err := h.Backend.CreateDataMigration(ctx, CreateDataMigrationParams{
		Name:                       name,
		MigrationProjectIdentifier: ptrconv.String(in.MigrationProjectIdentifier),
		DataMigrationType:          migrationType,
		ServiceAccessRoleArn:       ptrconv.String(in.ServiceAccessRoleArn),
		SelectionRules:             ptrconv.String(in.SelectionRules),
		NumberOfJobs:               ptrInt32(in.NumberOfJobs),
		EnableCloudwatchLogs:       ptrconv.Bool(in.EnableCloudwatchLogs),
		SourceDataSettings:         sources,
		TargetDataSettings:         targets,
		Tags:                       tagsToMap(in.Tags),
	})
	if err != nil {
		return nil, err
	}

	return &createDataMigrationOutput{DataMigration: dmToJSON(dm)}, nil
}

func dmToJSON(dm *DataMigration) dataMigrationJSON {
	out := dataMigrationJSON{
		DataMigrationName:    dm.DataMigrationName,
		DataMigrationArn:     dm.DataMigrationArn,
		MigrationProjectArn:  dm.MigrationProjectArn,
		DataMigrationType:    dm.DataMigrationType,
		ServiceAccessRoleArn: dm.ServiceAccessRoleArn,
		DataMigrationStatus:  dm.DataMigrationStatus,
		DataMigrationSettings: &dataMigrationSettingsJSON{
			NumberOfJobs:          dm.NumberOfJobs,
			CloudwatchLogsEnabled: dm.EnableCloudwatchLogs,
			SelectionRules:        dm.SelectionRules,
		},
	}

	for _, s := range dm.SourceDataSettings {
		out.SourceDataSettings = append(out.SourceDataSettings, sourceDataSettingJSON(s))
	}

	for _, s := range dm.TargetDataSettings {
		out.TargetDataSettings = append(out.TargetDataSettings, targetDataSettingJSON(s))
	}

	return out
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
	WithoutSettings         *bool         `json:"WithoutSettings"`
	Filters                 []filterEntry `json:"Filters"`
}

type describeDataMigrationsOutput struct {
	Marker         *string             `json:"Marker,omitempty"`
	DataMigrations []dataMigrationJSON `json:"DataMigrations"`
}

func (h *Handler) handleDescribeDataMigrations(
	ctx context.Context, in *describeDataMigrationsInput,
) (*describeDataMigrationsOutput, error) {
	df := newDescribeFilters(in.Filters)
	if identifier := ptrconv.String(in.DataMigrationIdentifier); identifier != "" {
		df = NewIdentifierFilter("data-migration-identifier", identifier)
	}

	list, err := h.Backend.DescribeDataMigrations(ctx, df)
	if err != nil {
		return nil, err
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].DataMigrationName < list[j].DataMigrationName
	})

	withoutSettings := ptrconv.Bool(in.WithoutSettings)

	all := make([]dataMigrationJSON, 0, len(list))
	for _, dm := range list {
		item := dmToJSON(dm)
		if withoutSettings {
			item.DataMigrationSettings = nil
		}

		all = append(all, item)
	}

	data, nextMarker := dmsPaginate(all, in.Marker, in.MaxRecords)

	return &describeDataMigrationsOutput{DataMigrations: data, Marker: nextMarker}, nil
}

type modifyDataMigrationInput struct {
	DataMigrationIdentifier *string                 `json:"DataMigrationIdentifier"`
	DataMigrationName       *string                 `json:"DataMigrationName"`
	DataMigrationType       *string                 `json:"DataMigrationType"`
	ServiceAccessRoleArn    *string                 `json:"ServiceAccessRoleArn"`
	NumberOfJobs            *int32                  `json:"NumberOfJobs"`
	EnableCloudwatchLogs    *bool                   `json:"EnableCloudwatchLogs"`
	SelectionRules          *string                 `json:"SelectionRules"`
	SourceDataSettings      []sourceDataSettingJSON `json:"SourceDataSettings"`
	TargetDataSettings      []targetDataSettingJSON `json:"TargetDataSettings"`
}

type modifyDataMigrationOutput struct {
	DataMigration dataMigrationJSON `json:"DataMigration"`
}

func (h *Handler) handleModifyDataMigration(
	ctx context.Context, in *modifyDataMigrationInput,
) (*modifyDataMigrationOutput, error) {
	targets, err := targetDataSettingsFromJSON(in.TargetDataSettings)
	if err != nil {
		return nil, err
	}

	sources, err := sourceDataSettingsFromJSON(in.SourceDataSettings)
	if err != nil {
		return nil, err
	}

	dm, err := h.Backend.ModifyDataMigration(ctx, ModifyDataMigrationParams{
		NameOrArn:            ptrconv.String(in.DataMigrationIdentifier),
		NewName:              ptrconv.String(in.DataMigrationName),
		DataMigrationType:    ptrconv.String(in.DataMigrationType),
		ServiceAccessRoleArn: ptrconv.String(in.ServiceAccessRoleArn),
		NumberOfJobs:         in.NumberOfJobs,
		EnableCloudwatchLogs: in.EnableCloudwatchLogs,
		SelectionRules:       in.SelectionRules,
		SourceDataSettings:   sources,
		TargetDataSettings:   targets,
	})
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
