package dms

import (
	"context"
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type createDataProviderInput struct {
	DataProviderName *string    `json:"DataProviderName"`
	Engine           *string    `json:"Engine"`
	Description      *string    `json:"Description"`
	Tags             []tagEntry `json:"Tags"`
}

type dataProviderJSON struct {
	DataProviderName string `json:"DataProviderName"`
	DataProviderArn  string `json:"DataProviderArn"`
	Engine           string `json:"Engine"`
	Description      string `json:"Description,omitempty"`
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

	kv := tagsToMap(in.Tags)
	dp, err := h.Backend.CreateDataProvider(ctx, name, engine, ptrconv.String(in.Description), kv)
	if err != nil {
		return nil, err
	}

	return &createDataProviderOutput{DataProvider: dpToJSON(dp)}, nil
}

func dpToJSON(dp *DataProvider) dataProviderJSON {
	return dataProviderJSON{
		DataProviderName: dp.DataProviderName,
		DataProviderArn:  dp.DataProviderArn,
		Engine:           dp.Engine,
		Description:      dp.Description,
	}
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
	DataProviderIdentifier *string `json:"DataProviderIdentifier"`
	Marker                 *string `json:"Marker"`
	MaxRecords             *int32  `json:"MaxRecords"`
}

type describeDataProvidersOutput struct {
	Marker        *string            `json:"Marker,omitempty"`
	DataProviders []dataProviderJSON `json:"DataProviders"`
}

func (h *Handler) handleDescribeDataProviders(
	ctx context.Context, in *describeDataProvidersInput,
) (*describeDataProvidersOutput, error) {
	list, err := h.Backend.DescribeDataProviders(ctx, ptrconv.String(in.DataProviderIdentifier))
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
	DataProviderIdentifier *string `json:"DataProviderIdentifier"`
	Engine                 *string `json:"Engine"`
	Description            *string `json:"Description"`
}

type modifyDataProviderOutput struct {
	DataProvider dataProviderJSON `json:"DataProvider"`
}

func (h *Handler) handleModifyDataProvider(
	ctx context.Context, in *modifyDataProviderInput,
) (*modifyDataProviderOutput, error) {
	dp, err := h.Backend.ModifyDataProvider(
		ctx,
		ptrconv.String(in.DataProviderIdentifier),
		ptrconv.String(in.Engine),
		ptrconv.String(in.Description),
	)
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
