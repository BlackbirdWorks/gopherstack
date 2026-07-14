package dms

import (
	"context"
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type createEndpointInput struct {
	EndpointIdentifier *string    `json:"EndpointIdentifier"`
	EndpointType       *string    `json:"EndpointType"`
	EngineName         *string    `json:"EngineName"`
	ServerName         *string    `json:"ServerName"`
	DatabaseName       *string    `json:"DatabaseName"`
	Username           *string    `json:"Username"`
	Port               *int32     `json:"Port"`
	Tags               []tagEntry `json:"Tags"`
}

type createEndpointOutput struct {
	Endpoint endpointJSON `json:"Endpoint"`
}

func (h *Handler) handleCreateEndpoint(
	ctx context.Context, in *createEndpointInput,
) (*createEndpointOutput, error) {
	identifier := ptrconv.String(in.EndpointIdentifier)
	endpointType := ptrconv.String(in.EndpointType)
	engineName := ptrconv.String(in.EngineName)

	if identifier == "" {
		return nil, fmt.Errorf("%w: EndpointIdentifier is required", ErrValidation)
	}

	if endpointType == "" {
		return nil, fmt.Errorf("%w: EndpointType is required", ErrValidation)
	}

	if engineName == "" {
		return nil, fmt.Errorf("%w: EngineName is required", ErrValidation)
	}

	kv := tagsToMap(in.Tags)
	ep, err := h.Backend.CreateEndpoint(
		ctx,
		identifier,
		endpointType,
		engineName,
		ptrconv.String(in.ServerName),
		ptrconv.String(in.DatabaseName),
		ptrconv.String(in.Username),
		ptrInt32(in.Port),
		kv,
	)
	if err != nil {
		return nil, err
	}

	return &createEndpointOutput{Endpoint: epToJSON(ep)}, nil
}

type describeEndpointsInput struct {
	Marker     *string       `json:"Marker"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeEndpointsOutput struct {
	Marker    *string        `json:"Marker,omitempty"`
	Endpoints []endpointJSON `json:"Endpoints"`
}

//nolint:dupl // same HMAC-paginate pattern as handleDescribeReplicationInstances
func (h *Handler) handleDescribeEndpoints(
	ctx context.Context,
	in *describeEndpointsInput,
) (*describeEndpointsOutput, error) {
	identifier := extractFilterValue(in.Filters, "endpoint-id")
	arnFilter := extractFilterValue(in.Filters, "endpoint-arn")

	lookup := identifier
	if arnFilter != "" {
		lookup = arnFilter
	}

	list, err := h.Backend.DescribeEndpoints(ctx, lookup)
	if err != nil {
		return nil, err
	}

	// Sort for stable pagination.
	sort.Slice(list, func(i, j int) bool {
		return list[i].EndpointIdentifier < list[j].EndpointIdentifier
	})

	all := make([]endpointJSON, 0, len(list))
	for _, ep := range list {
		all = append(all, epToJSON(ep))
	}

	data, nextMarker := dmsHMACPaginate(all, in.Marker, in.MaxRecords, h.Backend.PaginationSecret())

	return &describeEndpointsOutput{Endpoints: data, Marker: nextMarker}, nil
}

type deleteEndpointInput struct {
	EndpointArn *string `json:"EndpointArn"`
}

type deleteEndpointOutput struct {
	Endpoint endpointJSON `json:"Endpoint"`
}

func (h *Handler) handleDeleteEndpoint(
	ctx context.Context, in *deleteEndpointInput,
) (*deleteEndpointOutput, error) {
	ep, err := h.Backend.DeleteEndpoint(ctx, ptrconv.String(in.EndpointArn))
	if err != nil {
		return nil, err
	}

	return &deleteEndpointOutput{Endpoint: epToJSON(ep)}, nil
}

type endpointJSON struct {
	EndpointIdentifier string `json:"EndpointIdentifier"`
	EndpointArn        string `json:"EndpointArn"`
	EndpointType       string `json:"EndpointType"`
	EngineName         string `json:"EngineName"`
	ServerName         string `json:"ServerName,omitempty"`
	DatabaseName       string `json:"DatabaseName,omitempty"`
	Username           string `json:"Username,omitempty"`
	Status             string `json:"Status"`
	Port               int32  `json:"Port,omitempty"`
}

func epToJSON(ep *Endpoint) endpointJSON {
	return endpointJSON{
		EndpointIdentifier: ep.EndpointIdentifier,
		EndpointArn:        ep.EndpointArn,
		EndpointType:       ep.EndpointType,
		EngineName:         ep.EngineName,
		ServerName:         ep.ServerName,
		DatabaseName:       ep.DatabaseName,
		Username:           ep.Username,
		Status:             ep.Status,
		Port:               ep.Port,
	}
}

type describeEndpointSettingsInput struct {
	EngineName *string `json:"EngineName"`
	Marker     *string `json:"Marker"`
	MaxRecords *int32  `json:"MaxRecords"`
}

type describeEndpointSettingsOutput struct {
	Marker           *string          `json:"Marker,omitempty"`
	EndpointSettings []map[string]any `json:"EndpointSettings"`
}

func (h *Handler) handleDescribeEndpointSettings(
	_ context.Context, _ *describeEndpointSettingsInput,
) (*describeEndpointSettingsOutput, error) {
	return &describeEndpointSettingsOutput{EndpointSettings: []map[string]any{}}, nil
}

type describeEndpointTypesInput struct {
	Marker     *string       `json:"Marker"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type supportedEndpointTypeJSON struct {
	EngineName        string `json:"EngineName"`
	EndpointType      string `json:"EndpointType"`
	EngineDisplayName string `json:"EngineDisplayName"`
	SupportsCDC       bool   `json:"SupportsCDC"`
}

type describeEndpointTypesOutput struct {
	Marker                 *string                     `json:"Marker,omitempty"`
	SupportedEndpointTypes []supportedEndpointTypeJSON `json:"SupportedEndpointTypes"`
}

func (h *Handler) handleDescribeEndpointTypes(
	_ context.Context, _ *describeEndpointTypesInput,
) (*describeEndpointTypesOutput, error) {
	engines := []string{
		"mysql",
		"postgres",
		"oracle",
		"sqlserver",
		"mongodb",
		"s3",
		"kinesis",
		"kafka",
		"aurora",
		"aurora-postgresql",
		"mariadb",
		"redshift",
		"dynamodb",
	}
	const endpointDirections = 2 // SOURCE and TARGET
	types := make([]supportedEndpointTypeJSON, 0, len(engines)*endpointDirections)

	for _, e := range engines {
		types = append(
			types,
			supportedEndpointTypeJSON{
				EngineName:        e,
				SupportsCDC:       true,
				EndpointType:      "SOURCE",
				EngineDisplayName: e,
			},
			supportedEndpointTypeJSON{
				EngineName:        e,
				SupportsCDC:       true,
				EndpointType:      "TARGET",
				EngineDisplayName: e,
			},
		)
	}

	return &describeEndpointTypesOutput{SupportedEndpointTypes: types}, nil
}

type describeEngineVersionsInput struct {
	Marker     *string `json:"Marker"`
	MaxRecords *int32  `json:"MaxRecords"`
}

type engineVersionJSON struct {
	Version          string `json:"Version"`
	Lifecycle        string `json:"Lifecycle"`
	ReleaseNotes     string `json:"ReleaseNotes,omitempty"`
	LaunchDate       string `json:"LaunchDate,omitempty"`
	AutoUpgradeDate  string `json:"AutoUpgradeDate,omitempty"`
	DeprecationDate  string `json:"DeprecationDate,omitempty"`
	ForceUpgradeDate string `json:"ForceUpgradeDate,omitempty"`
}

type describeEngineVersionsOutput struct {
	Marker         *string             `json:"Marker,omitempty"`
	EngineVersions []engineVersionJSON `json:"EngineVersions"`
}

func dmsEngineVersionList() []engineVersionJSON {
	return []engineVersionJSON{
		{Version: defaultEngineVersion, Lifecycle: statusAvailable, LaunchDate: "2023-11-01"},
		{Version: "3.5.2", Lifecycle: statusAvailable, LaunchDate: "2023-07-01"},
		{Version: "3.5.1", Lifecycle: statusAvailable, LaunchDate: "2023-03-01"},
		{Version: "3.4.7", Lifecycle: statusAvailable, LaunchDate: "2022-11-01"},
		{Version: "3.4.6", Lifecycle: statusAvailable, LaunchDate: "2022-07-01"},
		{Version: "3.4.5", Lifecycle: "deprecated", LaunchDate: "2022-03-01", DeprecationDate: "2023-06-01"},
	}
}

func (h *Handler) handleDescribeEngineVersions(
	_ context.Context, in *describeEngineVersionsInput,
) (*describeEngineVersionsOutput, error) {
	data, nextMarker := dmsPaginate(dmsEngineVersionList(), in.Marker, in.MaxRecords)

	return &describeEngineVersionsOutput{EngineVersions: data, Marker: nextMarker}, nil
}

type describeRefreshSchemasStatusInput struct {
	EndpointArn *string `json:"EndpointArn"`
}

type refreshSchemasStatusJSON struct {
	Status string `json:"Status"`
}

type describeRefreshSchemasStatusOutput struct {
	RefreshSchemasStatus refreshSchemasStatusJSON `json:"RefreshSchemasStatus"`
}

func (h *Handler) handleDescribeRefreshSchemasStatus(
	_ context.Context, _ *describeRefreshSchemasStatusInput,
) (*describeRefreshSchemasStatusOutput, error) {
	return &describeRefreshSchemasStatusOutput{
		RefreshSchemasStatus: refreshSchemasStatusJSON{Status: statusSuccessful},
	}, nil
}

type describeSchemasInput struct {
	EndpointArn            *string `json:"EndpointArn"`
	ReplicationInstanceArn *string `json:"ReplicationInstanceArn"`
	Marker                 *string `json:"Marker"`
	MaxRecords             *int32  `json:"MaxRecords"`
}

type describeSchemasOutput struct {
	Marker  *string  `json:"Marker,omitempty"`
	Schemas []string `json:"Schemas"`
}

func (h *Handler) handleDescribeSchemas(
	ctx context.Context, in *describeSchemasInput,
) (*describeSchemasOutput, error) {
	schemas, err := h.Backend.DescribeSchemas(ctx, ptrconv.String(in.EndpointArn))
	if err != nil {
		return nil, err
	}

	data, nextMarker := dmsPaginate(schemas, in.Marker, in.MaxRecords)

	return &describeSchemasOutput{Schemas: data, Marker: nextMarker}, nil
}

type modifyEndpointInput struct {
	EndpointArn  *string `json:"EndpointArn"`
	ServerName   *string `json:"ServerName"`
	DatabaseName *string `json:"DatabaseName"`
	Username     *string `json:"Username"`
	Port         *int32  `json:"Port"`
}

type modifyEndpointOutput struct {
	Endpoint endpointJSON `json:"Endpoint"`
}

func (h *Handler) handleModifyEndpoint(
	ctx context.Context, in *modifyEndpointInput,
) (*modifyEndpointOutput, error) {
	ep, err := h.Backend.ModifyEndpoint(
		ctx,
		ptrconv.String(in.EndpointArn),
		ptrconv.String(in.ServerName),
		ptrconv.String(in.DatabaseName),
		ptrconv.String(in.Username),
		ptrInt32(in.Port),
	)
	if err != nil {
		return nil, err
	}

	return &modifyEndpointOutput{Endpoint: epToJSON(ep)}, nil
}

type refreshSchemasInput struct {
	EndpointArn            *string `json:"EndpointArn"`
	ReplicationInstanceArn *string `json:"ReplicationInstanceArn"`
}

type refreshSchemasOutput struct {
	RefreshSchemasStatus refreshSchemasStatusJSON `json:"RefreshSchemasStatus"`
}

func (h *Handler) handleRefreshSchemas(
	ctx context.Context, in *refreshSchemasInput,
) (*refreshSchemasOutput, error) {
	if err := h.Backend.RefreshSchemas(ctx, ptrconv.String(in.EndpointArn)); err != nil {
		return nil, err
	}

	return &refreshSchemasOutput{
		RefreshSchemasStatus: refreshSchemasStatusJSON{Status: statusSuccessful},
	}, nil
}

// opsEndpoints returns the dispatch-table entries for the endpoints operation family.
func (h *Handler) opsEndpoints() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opCreateEndpoint:    service.WrapOp(h.handleCreateEndpoint),
		opDescribeEndpoints: service.WrapOp(h.handleDescribeEndpoints),
		opDeleteEndpoint:    service.WrapOp(h.handleDeleteEndpoint),
		opDescribeEndpointSettings: service.WrapOp(
			h.handleDescribeEndpointSettings,
		),
		opDescribeEndpointTypes: service.WrapOp(
			h.handleDescribeEndpointTypes,
		),
		opDescribeEngineVersions: service.WrapOp(
			h.handleDescribeEngineVersions,
		),
		opDescribeRefreshSchemasStatus: service.WrapOp(
			h.handleDescribeRefreshSchemasStatus,
		),
		opDescribeSchemas: service.WrapOp(h.handleDescribeSchemas),
		opModifyEndpoint:  service.WrapOp(h.handleModifyEndpoint),
		opRefreshSchemas:  service.WrapOp(h.handleRefreshSchemas),
	}
}
