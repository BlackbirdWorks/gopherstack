package mgn

import (
	"context"
	"net/http"
)

func (h *Handler) handleCreateNetworkMigrationDefinition(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req createNetworkMigrationDefinitionRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	in := CreateNetworkMigrationDefinitionInput{
		TargetNetwork:         fromTargetNetworkWire(req.TargetNetwork),
		TargetS3Configuration: fromTargetS3ConfigurationWire(req.TargetS3Configuration),
		ScopeTags:             req.ScopeTags,
		SourceConfigurations:  fromSourceConfigurationsWire(req.SourceConfigurations),
		Tags:                  req.Tags,
		Name:                  req.Name,
		Description:           req.Description,
		TargetDeployment:      req.TargetDeployment,
	}

	d, err := h.Backend.CreateNetworkMigrationDefinition(in)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toNetworkMigrationDefinitionWire(d))
}

func (h *Handler) handleGetNetworkMigrationDefinition(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req networkMigrationDefinitionIDRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	d, err := h.Backend.GetNetworkMigrationDefinition(req.NetworkMigrationDefinitionID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toNetworkMigrationDefinitionWire(d))
}

func (h *Handler) handleUpdateNetworkMigrationDefinition(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req updateNetworkMigrationDefinitionRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	in := UpdateNetworkMigrationDefinitionInput{
		TargetNetworkUpdate:   fromTargetNetworkUpdateWire(req.TargetNetworkUpdate),
		TargetS3Configuration: fromTargetS3ConfigurationUpdateWire(req.TargetS3Configuration),
		ScopeTags:             req.ScopeTags,
		SourceConfigurations:  fromSourceConfigurationsWireOrNil(req.SourceConfigurations),
		Name:                  req.Name,
		Description:           req.Description,
	}
	if req.TargetDeployment != "" {
		in.TargetDeployment = &req.TargetDeployment
	}

	d, err := h.Backend.UpdateNetworkMigrationDefinition(req.NetworkMigrationDefinitionID, in)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toNetworkMigrationDefinitionWire(d))
}

// fromSourceConfigurationsWireOrNil returns nil (not an empty non-nil
// slice) when cs is empty, preserving the "omitted field means don't touch"
// convention for UpdateNetworkMigrationDefinitionInput.SourceConfigurations.
func fromSourceConfigurationsWireOrNil(cs []sourceConfigurationWire) []SourceConfiguration {
	if cs == nil {
		return nil
	}

	return fromSourceConfigurationsWire(cs)
}

func (h *Handler) handleDeleteNetworkMigrationDefinition(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req networkMigrationDefinitionIDRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteNetworkMigrationDefinition(req.NetworkMigrationDefinitionID); err != nil {
		return nil, err
	}

	return marshalResponse(struct{}{})
}

func (h *Handler) handleListNetworkMigrationDefinitions(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req listNetworkMigrationDefinitionsRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	f := ListNetworkMigrationDefinitionsFilters{}
	if req.Filters != nil {
		f.NetworkMigrationDefinitionIDs = req.Filters.NetworkMigrationDefinitionIDs
	}

	pg := h.Backend.ListNetworkMigrationDefinitions(f, req.NextToken, int(req.MaxResults))

	items := make([]networkMigrationDefinitionSummaryWire, len(pg.Data))
	for i, d := range pg.Data {
		items[i] = toNetworkMigrationDefinitionSummaryWire(d)
	}

	return marshalResponse(listNetworkMigrationDefinitionsResponse{Items: items, NextToken: pg.Next})
}

func (h *Handler) handleGetNetworkMigrationMapperSegmentConstruct(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req getNMMapperSegmentConstructRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	err := h.Backend.GetNetworkMigrationMapperSegmentConstruct(
		req.NetworkMigrationDefinitionID, req.NetworkMigrationExecutionID, req.SegmentID, req.ConstructID,
	)

	return nil, err
}

func (h *Handler) handleListNetworkMigrationMapperSegmentConstructs(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req listNMMapperSegmentConstructsRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.ListNetworkMigrationMapperSegmentConstructs(
		req.NetworkMigrationDefinitionID, req.NetworkMigrationExecutionID, req.SegmentID,
	); err != nil {
		return nil, err
	}

	return marshalResponse(genericItemsResponse{Items: []struct{}{}})
}

func (h *Handler) handleListNetworkMigrationMapperSegments(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req listNMScopedRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.ListNetworkMigrationMapperSegments(
		req.NetworkMigrationDefinitionID, req.NetworkMigrationExecutionID,
	); err != nil {
		return nil, err
	}

	return marshalResponse(genericItemsResponse{Items: []struct{}{}})
}

func (h *Handler) handleUpdateNetworkMigrationMapperSegment(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req updateNMMapperSegmentRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	err := h.Backend.UpdateNetworkMigrationMapperSegment(
		req.NetworkMigrationDefinitionID, req.NetworkMigrationExecutionID, req.SegmentID,
	)

	return nil, err
}

func (h *Handler) handleStartNetworkMigrationMapping(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req startNMMappingRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	jobID, err := h.Backend.StartNetworkMigrationMapping(
		req.NetworkMigrationDefinitionID,
		req.NetworkMigrationExecutionID,
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(jobIDResponse{JobID: jobID})
}

func (h *Handler) handleStartNetworkMigrationMappingUpdate(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req startNMMappingUpdateRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	jobID, err := h.Backend.StartNetworkMigrationMappingUpdate(
		req.NetworkMigrationDefinitionID, req.NetworkMigrationExecutionID,
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(jobIDResponse{JobID: jobID})
}

func (h *Handler) handleListNetworkMigrationMappings(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req listNMScopedRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	pg, err := h.Backend.ListNetworkMigrationMappings(
		req.NetworkMigrationDefinitionID, req.NetworkMigrationExecutionID, nmFilterJobIDs(req.Filters),
		req.NextToken, int(req.MaxResults),
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(nmJobDetailsResponse(pg))
}

func (h *Handler) handleListNetworkMigrationMappingUpdates(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req listNMScopedRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	pg, err := h.Backend.ListNetworkMigrationMappingUpdates(
		req.NetworkMigrationDefinitionID, req.NetworkMigrationExecutionID, nmFilterJobIDs(req.Filters),
		req.NextToken, int(req.MaxResults),
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(nmJobDetailsResponse(pg))
}
