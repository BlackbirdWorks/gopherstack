package awsconfig

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Operation name constants for stored query ops.
const (
	opDeleteStoredQuery = "DeleteStoredQuery"
	opGetStoredQuery    = "GetStoredQuery"
	opListStoredQueries = "ListStoredQueries"
	opPutStoredQuery    = "PutStoredQuery"
)

// storedQuerySupportedOps returns the operation names this family handles.
func storedQuerySupportedOps() []string {
	return []string{
		opGetStoredQuery,
		opDeleteStoredQuery,
		opListStoredQueries,
		opPutStoredQuery,
	}
}

// DeleteStoredQuery request/response types and handler.
type deleteStoredQueryInput struct {
	QueryName string `json:"QueryName"`
}

func (h *Handler) handleDeleteStoredQuery(
	_ context.Context, in *deleteStoredQueryInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteStoredQuery(in.QueryName)
}

// GetStoredQuery request/response types and handler.
type getStoredQueryInput struct {
	QueryName string `json:"QueryName"`
}
type getStoredQueryOutput struct {
	StoredQuery *StoredQuery `json:"StoredQuery"`
}

func (h *Handler) handleGetStoredQuery(
	_ context.Context, in *getStoredQueryInput,
) (*getStoredQueryOutput, error) {
	return &getStoredQueryOutput{
		StoredQuery: h.Backend.GetStoredQuery(in.QueryName),
	}, nil
}

// ListStoredQueries request/response types and handler.
type listStoredQueriesOutput struct {
	StoredQueryMetadata []StoredQueryMetadata `json:"StoredQueryMetadata"`
}

func (h *Handler) handleListStoredQueries(
	_ context.Context, _ *emptyInput,
) (*listStoredQueriesOutput, error) {
	return &listStoredQueriesOutput{
		StoredQueryMetadata: h.Backend.ListStoredQueries(),
	}, nil
}

// PutStoredQuery request/response types and handler.
type putStoredQueryBody struct {
	QueryName string `json:"QueryName"`
}
type putStoredQueryInput struct {
	StoredQuery putStoredQueryBody `json:"StoredQuery"`
}

func (h *Handler) handlePutStoredQuery(
	_ context.Context, in *putStoredQueryInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.PutStoredQuery(in.StoredQuery.QueryName)
}

// buildStoredQueryDispatch returns dispatch entries for stored query ops.
func (h *Handler) buildStoredQueryDispatch() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opGetStoredQuery:    service.WrapOp(h.handleGetStoredQuery),
		opDeleteStoredQuery: service.WrapOp(h.handleDeleteStoredQuery),
		opListStoredQueries: service.WrapOp(h.handleListStoredQueries),
		opPutStoredQuery:    service.WrapOp(h.handlePutStoredQuery),
	}
}
