package dms

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type createDataProviderInput struct {
	DataProviderName *string         `json:"DataProviderName"`
	Engine           *string         `json:"Engine"`
	Description      *string         `json:"Description"`
	Settings         json.RawMessage `json:"Settings"`
	Virtual          *bool           `json:"Virtual"`
	Tags             []tagEntry      `json:"Tags"`
}

type dataProviderJSON struct {
	DataProviderName string          `json:"DataProviderName"`
	DataProviderArn  string          `json:"DataProviderArn"`
	Engine           string          `json:"Engine"`
	Description      string          `json:"Description,omitempty"`
	Settings         json.RawMessage `json:"Settings,omitempty"`
	Virtual          bool            `json:"Virtual,omitempty"`
}

type createDataProviderOutput struct {
	DataProvider dataProviderJSON `json:"DataProvider"`
}

func (h *Handler) handleCreateDataProvider(
	ctx context.Context, in *createDataProviderInput,
) (*createDataProviderOutput, error) {
	name := ptrconv.String(in.DataProviderName)
	if name == "" {
		return nil, fmt.Errorf("%w: DataProviderName is required", ErrValidation)
	}

	engine := ptrconv.String(in.Engine)
	if engine == "" {
		return nil, fmt.Errorf("%w: Engine is required", ErrValidation)
	}

	settings, err := validateDataProviderSettings(in.Settings)
	if err != nil {
		return nil, err
	}

	dp, err := h.Backend.CreateDataProvider(ctx, CreateDataProviderParams{
		Name:        name,
		Engine:      engine,
		Description: ptrconv.String(in.Description),
		Settings:    settings,
		Virtual:     ptrconv.Bool(in.Virtual),
		Tags:        tagsToMap(in.Tags),
	})
	if err != nil {
		return nil, err
	}

	return &createDataProviderOutput{DataProvider: dpToJSON(dp)}, nil
}

// isDataProviderSettingsArm reports whether arm is a DataProviderSettings
// union member (types.DataProviderSettingsMember*).
func isDataProviderSettingsArm(arm string) bool {
	switch arm {
	case "DocDbSettings", "IbmDb2LuwSettings", "IbmDb2zOsSettings", "MariaDbSettings",
		"MicrosoftSqlServerSettings", "MongoDbSettings", "MySqlSettings", "OracleSettings",
		"PostgreSqlSettings", "RedshiftSettings", "SybaseAseSettings":
		return true
	default:
		return false
	}
}

// validateDataProviderSettings checks that a supplied Settings document is a
// union with exactly one known member and returns it as a string.
func validateDataProviderSettings(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", nil
	}

	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil || len(doc) != 1 {
		return "", fmt.Errorf("%w: Settings must be a union with exactly one member", ErrValidation)
	}

	for arm := range doc {
		if !isDataProviderSettingsArm(arm) {
			return "", fmt.Errorf("%w: unknown Settings member %q", ErrValidation, arm)
		}
	}

	return string(raw), nil
}

func dpToJSON(dp *DataProvider) dataProviderJSON {
	out := dataProviderJSON{
		DataProviderName: dp.DataProviderName,
		DataProviderArn:  dp.DataProviderArn,
		Engine:           dp.Engine,
		Description:      dp.Description,
		Virtual:          dp.Virtual,
	}
	if dp.Settings != "" {
		out.Settings = json.RawMessage(dp.Settings)
	}

	return out
}

type deleteDataProviderInput struct {
	DataProviderIdentifier *string `json:"DataProviderIdentifier"`
}

type deleteDataProviderOutput struct {
	DataProvider dataProviderJSON `json:"DataProvider"`
}

func (h *Handler) handleDeleteDataProvider(
	ctx context.Context, in *deleteDataProviderInput,
) (*deleteDataProviderOutput, error) {
	dp, err := h.Backend.DeleteDataProvider(ctx, ptrconv.String(in.DataProviderIdentifier))
	if err != nil {
		return nil, err
	}

	return &deleteDataProviderOutput{DataProvider: dpToJSON(dp)}, nil
}

type describeDataProvidersInput struct {
	DataProviderIdentifier *string       `json:"DataProviderIdentifier"`
	Marker                 *string       `json:"Marker"`
	MaxRecords             *int32        `json:"MaxRecords"`
	Filters                []filterEntry `json:"Filters"`
}

type describeDataProvidersOutput struct {
	Marker        *string            `json:"Marker,omitempty"`
	DataProviders []dataProviderJSON `json:"DataProviders"`
}

func (h *Handler) handleDescribeDataProviders(
	ctx context.Context, in *describeDataProvidersInput,
) (*describeDataProvidersOutput, error) {
	df := newDescribeFilters(in.Filters)
	if identifier := ptrconv.String(in.DataProviderIdentifier); identifier != "" {
		df = NewIdentifierFilter("data-provider-identifier", identifier)
	}

	list, err := h.Backend.DescribeDataProviders(ctx, df)
	if err != nil {
		return nil, err
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].DataProviderName < list[j].DataProviderName
	})

	all := make([]dataProviderJSON, 0, len(list))
	for _, dp := range list {
		all = append(all, dpToJSON(dp))
	}

	data, nextMarker := dmsPaginate(all, in.Marker, in.MaxRecords)

	return &describeDataProvidersOutput{DataProviders: data, Marker: nextMarker}, nil
}

type modifyDataProviderInput struct {
	DataProviderIdentifier *string         `json:"DataProviderIdentifier"`
	DataProviderName       *string         `json:"DataProviderName"`
	Engine                 *string         `json:"Engine"`
	Description            *string         `json:"Description"`
	Virtual                *bool           `json:"Virtual"`
	ExactSettings          *bool           `json:"ExactSettings"`
	Settings               json.RawMessage `json:"Settings"`
}

type modifyDataProviderOutput struct {
	DataProvider dataProviderJSON `json:"DataProvider"`
}

func (h *Handler) handleModifyDataProvider(
	ctx context.Context, in *modifyDataProviderInput,
) (*modifyDataProviderOutput, error) {
	settings, err := validateDataProviderSettings(in.Settings)
	if err != nil {
		return nil, err
	}

	dp, err := h.Backend.ModifyDataProvider(ctx, ModifyDataProviderParams{
		NameOrArn:     ptrconv.String(in.DataProviderIdentifier),
		NewName:       ptrconv.String(in.DataProviderName),
		Engine:        ptrconv.String(in.Engine),
		Description:   ptrconv.String(in.Description),
		Settings:      settings,
		Virtual:       in.Virtual,
		ExactSettings: in.ExactSettings,
	})
	if err != nil {
		return nil, err
	}

	return &modifyDataProviderOutput{DataProvider: dpToJSON(dp)}, nil
}

// opsDataProviders returns the dispatch-table entries for the data_providers operation family.
func (h *Handler) opsDataProviders() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opCreateDataProvider: service.WrapOp(h.handleCreateDataProvider),
		opDeleteDataProvider: service.WrapOp(h.handleDeleteDataProvider),
		opDescribeDataProviders: service.WrapOp(
			h.handleDescribeDataProviders,
		),
		opModifyDataProvider: service.WrapOp(h.handleModifyDataProvider),
	}
}
