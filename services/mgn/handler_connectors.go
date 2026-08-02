package mgn

import (
	"context"
	"net/http"
)

func (h *Handler) handleCreateConnector(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req createConnectorRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	in := CreateConnectorInput{
		SsmCommandConfig: fromConnectorSsmCommandConfigWire(req.SsmCommandConfig),
		Tags:             req.Tags,
		Name:             req.Name,
		SsmInstanceID:    req.SsmInstanceID,
	}

	c, err := h.Backend.CreateConnector(in)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toConnectorWire(c))
}

func (h *Handler) handleUpdateConnector(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req updateConnectorRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	in := UpdateConnectorInput{
		SsmCommandConfig: fromConnectorSsmCommandConfigWire(req.SsmCommandConfig),
		Name:             req.Name,
		SsmInstanceID:    req.SsmInstanceID,
	}

	c, err := h.Backend.UpdateConnector(req.ConnectorID, in)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toConnectorWire(c))
}

func (h *Handler) handleDeleteConnector(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req connectorIDRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteConnector(req.ConnectorID); err != nil {
		return nil, err
	}

	return marshalResponse(struct{}{})
}

func (h *Handler) handleListConnectors(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req listConnectorsRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	f := ListConnectorsFilters{}
	if req.Filters != nil {
		f.ConnectorIDs = req.Filters.ConnectorIDs
	}

	pg, err := h.Backend.ListConnectors(f, req.NextToken, int(req.MaxResults))
	if err != nil {
		return nil, err
	}

	items := make([]connectorWire, len(pg.Data))
	for i, c := range pg.Data {
		items[i] = toConnectorWire(c)
	}

	return marshalResponse(listConnectorsResponse{Items: items, NextToken: pg.Next})
}
