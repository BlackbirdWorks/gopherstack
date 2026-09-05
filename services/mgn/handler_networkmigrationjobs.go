package mgn

import (
	"context"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func nmFilterJobIDs(f *nmJobFiltersWire) []string {
	if f == nil {
		return nil
	}

	return f.JobIDs
}

// nmJobDetailsResponse converts a page of NetworkMigrationJob into the
// shared wire response shape every family-N List* job-details op (plus
// family M's ListNetworkMigrationMappings/MappingUpdates) uses --
// networkmigrationjobs.go's doc comment explains why one internal job type
// backs all five real SDK job-details types.
func nmJobDetailsResponse(pg page.Page[*NetworkMigrationJob]) listNMJobDetailsResponse {
	items := make([]networkMigrationJobDetailsWire, len(pg.Data))
	for i, j := range pg.Data {
		items[i] = toNMJobDetailsWire(j)
	}

	return listNMJobDetailsResponse{Items: items, NextToken: pg.Next}
}

func (h *Handler) handleStartNetworkMigrationAnalysis(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req nmScopeRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	jobID, err := h.Backend.StartNetworkMigrationAnalysis(
		req.NetworkMigrationDefinitionID, req.NetworkMigrationExecutionID,
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(jobIDResponse{JobID: jobID})
}

func (h *Handler) handleListNetworkMigrationAnalyses(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req listNMScopedRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	pg, err := h.Backend.ListNetworkMigrationAnalyses(
		req.NetworkMigrationDefinitionID, req.NetworkMigrationExecutionID, nmFilterJobIDs(req.Filters),
		req.NextToken, int(req.MaxResults),
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(nmJobDetailsResponse(pg))
}

func (h *Handler) handleListNetworkMigrationAnalysisResults(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req listNMScopedRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.ListNetworkMigrationAnalysisResults(
		req.NetworkMigrationDefinitionID, req.NetworkMigrationExecutionID,
	); err != nil {
		return nil, err
	}

	return marshalResponse(genericItemsResponse{Items: []struct{}{}})
}

func (h *Handler) handleStartNetworkMigrationCodeGeneration(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req startNMCodeGenerationRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	jobID, err := h.Backend.StartNetworkMigrationCodeGeneration(
		req.NetworkMigrationDefinitionID, req.NetworkMigrationExecutionID, req.CodeGenerationOutputFormatTypes,
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(jobIDResponse{JobID: jobID})
}

func (h *Handler) handleListNetworkMigrationCodeGenerations(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req listNMScopedRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	pg, err := h.Backend.ListNetworkMigrationCodeGenerations(
		req.NetworkMigrationDefinitionID, req.NetworkMigrationExecutionID, nmFilterJobIDs(req.Filters),
		req.NextToken, int(req.MaxResults),
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(nmJobDetailsResponse(pg))
}

func (h *Handler) handleListNetworkMigrationCodeGenerationSegments(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req listNMScopedRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.ListNetworkMigrationCodeGenerationSegments(
		req.NetworkMigrationDefinitionID, req.NetworkMigrationExecutionID,
	); err != nil {
		return nil, err
	}

	return marshalResponse(genericItemsResponse{Items: []struct{}{}})
}

func (h *Handler) handleStartNetworkMigrationDeployment(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req nmScopeRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	jobID, err := h.Backend.StartNetworkMigrationDeployment(
		req.NetworkMigrationDefinitionID, req.NetworkMigrationExecutionID,
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(jobIDResponse{JobID: jobID})
}

func (h *Handler) handleListNetworkMigrationDeployments(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req listNMScopedRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	pg, err := h.Backend.ListNetworkMigrationDeployments(
		req.NetworkMigrationDefinitionID, req.NetworkMigrationExecutionID, nmFilterJobIDs(req.Filters),
		req.NextToken, int(req.MaxResults),
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(nmJobDetailsResponse(pg))
}

func (h *Handler) handleListNetworkMigrationDeployedStacks(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req listNMScopedRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.ListNetworkMigrationDeployedStacks(
		req.NetworkMigrationDefinitionID, req.NetworkMigrationExecutionID,
	); err != nil {
		return nil, err
	}

	return marshalResponse(genericItemsResponse{Items: []struct{}{}})
}

func (h *Handler) handleListNetworkMigrationExecutions(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req listNMExecutionsRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	f := ListNetworkMigrationExecutionsFilters{}
	if req.Filters != nil {
		f = ListNetworkMigrationExecutionsFilters{
			NetworkMigrationExecutionIDs:      req.Filters.NetworkMigrationExecutionIDs,
			NetworkMigrationExecutionStatuses: req.Filters.NetworkMigrationExecutionStatuses,
		}
	}

	pg, err := h.Backend.ListNetworkMigrationExecutions(
		req.NetworkMigrationDefinitionID,
		f,
		req.NextToken,
		int(req.MaxResults),
	)
	if err != nil {
		return nil, err
	}

	items := make([]networkMigrationExecutionWire, len(pg.Data))
	for i, e := range pg.Data {
		items[i] = toNMExecutionWire(e)
	}

	return marshalResponse(listNMExecutionsResponse{Items: items, NextToken: pg.Next})
}
