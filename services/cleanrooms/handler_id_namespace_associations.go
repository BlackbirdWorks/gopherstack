package cleanrooms

import (
	"context"
	"encoding/json"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleGetCollaborationIDNamespaceAssociation(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier          string `json:"collaborationIdentifier"`
		IDNamespaceAssociationIdentifier string `json:"idNamespaceAssociationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.GetCollaborationIDNamespaceAssociation(
		req.CollaborationIdentifier,
		req.IDNamespaceAssociationIdentifier,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyIDNamespaceAssociation: a}), nil
}

func (h *Handler) handleListCollaborationIDNamespaceAssociations(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListCollaborationIDNamespaceAssociations(
		req.CollaborationIdentifier,
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"idNamespaceAssociationSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleCreateIDNamespaceAssociation(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		InputReferenceConfig map[string]any    `json:"inputReferenceConfig"`
		IDMappingConfig      map[string]any    `json:"idMappingConfig"`
		Tags                 map[string]string `json:"tags"`
		MembershipIdentifier string            `json:"membershipIdentifier"`
		Name                 string            `json:"name"`
		Description          string            `json:"description"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.CreateIDNamespaceAssociation(
		req.MembershipIdentifier,
		req.Name,
		req.Description,
		req.InputReferenceConfig,
		req.IDMappingConfig,
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyIDNamespaceAssociation: a}), nil
}

func (h *Handler) handleGetIDNamespaceAssociation(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier             string `json:"membershipIdentifier"`
		IDNamespaceAssociationIdentifier string `json:"idNamespaceAssociationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.GetIDNamespaceAssociation(
		req.MembershipIdentifier,
		req.IDNamespaceAssociationIdentifier,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyIDNamespaceAssociation: a}), nil
}

func (h *Handler) handleListIDNamespaceAssociations(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListIDNamespaceAssociations(
		req.MembershipIdentifier,
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"idNamespaceAssociationSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleUpdateIDNamespaceAssociation(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		IDMappingConfig                  map[string]any `json:"idMappingConfig"`
		MembershipIdentifier             string         `json:"membershipIdentifier"`
		IDNamespaceAssociationIdentifier string         `json:"idNamespaceAssociationIdentifier"`
		Description                      string         `json:"description"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.UpdateIDNamespaceAssociation(
		req.MembershipIdentifier,
		req.IDNamespaceAssociationIdentifier,
		req.Description,
		req.IDMappingConfig,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyIDNamespaceAssociation: a}), nil
}

func (h *Handler) handleDeleteIDNamespaceAssociation(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier             string `json:"membershipIdentifier"`
		IDNamespaceAssociationIdentifier string `json:"idNamespaceAssociationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)

	return nil, h.Backend.DeleteIDNamespaceAssociation(
		req.MembershipIdentifier,
		req.IDNamespaceAssociationIdentifier,
	)
}
