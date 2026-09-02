package mgn

import (
	"context"
	"net/http"
)

func (h *Handler) handlePutSourceServerAction(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req putSourceServerActionRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	in := PutSourceServerActionInput{
		ExternalParameters:    fromDynamicPathMapWire(req.ExternalParameters),
		Parameters:            fromParamMapWire(req.Parameters),
		SourceServerID:        req.SourceServerID,
		ActionID:              req.ActionID,
		ActionName:            req.ActionName,
		Category:              req.Category,
		Description:           req.Description,
		DocumentIdentifier:    req.DocumentIdentifier,
		DocumentVersion:       req.DocumentVersion,
		Order:                 req.Order,
		TimeoutSeconds:        req.TimeoutSeconds,
		Active:                req.Active,
		MustSucceedForCutover: req.MustSucceedForCutover,
	}

	doc, err := h.Backend.PutSourceServerAction(in)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toSourceServerActionDocumentWire(doc))
}

func (h *Handler) handleListSourceServerActions(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req listSourceServerActionsRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	var actionIDs []string
	if req.Filters != nil {
		actionIDs = req.Filters.ActionIDs
	}

	pg, err := h.Backend.ListSourceServerActions(req.SourceServerID, actionIDs, req.NextToken, int(req.MaxResults))
	if err != nil {
		return nil, err
	}

	items := make([]sourceServerActionDocumentWire, len(pg.Data))
	for i, a := range pg.Data {
		items[i] = toSourceServerActionDocumentWire(a)
	}

	return marshalResponse(listSourceServerActionsResponse{Items: items, NextToken: pg.Next})
}

func (h *Handler) handleRemoveSourceServerAction(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req removeSourceServerActionRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.RemoveSourceServerAction(req.SourceServerID, req.ActionID); err != nil {
		return nil, err
	}

	return marshalResponse(struct{}{})
}

func (h *Handler) handlePutTemplateAction(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req putTemplateActionRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	in := PutTemplateActionInput{
		ExternalParameters:            fromDynamicPathMapWire(req.ExternalParameters),
		Parameters:                    fromParamMapWire(req.Parameters),
		LaunchConfigurationTemplateID: req.LaunchConfigurationTemplateID,
		ActionID:                      req.ActionID,
		ActionName:                    req.ActionName,
		Category:                      req.Category,
		Description:                   req.Description,
		DocumentIdentifier:            req.DocumentIdentifier,
		DocumentVersion:               req.DocumentVersion,
		OperatingSystem:               req.OperatingSystem,
		Order:                         req.Order,
		TimeoutSeconds:                req.TimeoutSeconds,
		Active:                        req.Active,
		MustSucceedForCutover:         req.MustSucceedForCutover,
	}

	doc, err := h.Backend.PutTemplateAction(in)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toTemplateActionDocumentWire(doc))
}

func (h *Handler) handleListTemplateActions(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req listTemplateActionsRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	var actionIDs []string
	if req.Filters != nil {
		actionIDs = req.Filters.ActionIDs
	}

	pg, err := h.Backend.ListTemplateActions(
		req.LaunchConfigurationTemplateID, actionIDs, req.NextToken, int(req.MaxResults),
	)
	if err != nil {
		return nil, err
	}

	items := make([]templateActionDocumentWire, len(pg.Data))
	for i, a := range pg.Data {
		items[i] = toTemplateActionDocumentWire(a)
	}

	return marshalResponse(listTemplateActionsResponse{Items: items, NextToken: pg.Next})
}

func (h *Handler) handleRemoveTemplateAction(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req removeTemplateActionRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.RemoveTemplateAction(req.LaunchConfigurationTemplateID, req.ActionID); err != nil {
		return nil, err
	}

	return marshalResponse(struct{}{})
}
