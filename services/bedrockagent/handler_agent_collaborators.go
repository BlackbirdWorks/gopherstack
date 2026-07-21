package bedrockagent

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// ---------------------------------------------------------------------------
// Collaborator handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleAssociateCollaborator(
	ctx context.Context, c *echo.Context, agentID, agentVersion string, body []byte,
) error {
	var req struct {
		AgentDescriptor          map[string]any `json:"agentDescriptor"`
		CollaboratorName         string         `json:"collaboratorName"`
		CollaborationInstruction string         `json:"collaborationInstruction"`
		RelayConversationHistory string         `json:"relayConversationHistory"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	collab, err := h.Backend.AssociateAgentCollaborator(ctx, agentID, agentVersion, CollaboratorConfig{
		CollaboratorName:         req.CollaboratorName,
		CollaborationInstruction: req.CollaborationInstruction,
		RelayConversationHistory: req.RelayConversationHistory,
		AgentDescriptor:          req.AgentDescriptor,
	})
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAgentCollaborator: collab})
}

func (h *Handler) handleGetCollaborator(
	ctx context.Context, c *echo.Context, agentID, agentVersion, collaboratorID string,
) error {
	collab, err := h.Backend.GetAgentCollaborator(ctx, agentID, agentVersion, collaboratorID)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAgentCollaborator: collab})
}

func (h *Handler) handleUpdateCollaborator(
	ctx context.Context, c *echo.Context, agentID, agentVersion, collaboratorID string, body []byte,
) error {
	var req struct {
		AgentDescriptor          map[string]any `json:"agentDescriptor"`
		CollaboratorName         string         `json:"collaboratorName"`
		CollaborationInstruction string         `json:"collaborationInstruction"`
		RelayConversationHistory string         `json:"relayConversationHistory"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	collab, err := h.Backend.UpdateAgentCollaborator(ctx, agentID, agentVersion, collaboratorID, CollaboratorConfig{
		CollaboratorName:         req.CollaboratorName,
		CollaborationInstruction: req.CollaborationInstruction,
		RelayConversationHistory: req.RelayConversationHistory,
		AgentDescriptor:          req.AgentDescriptor,
	})
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAgentCollaborator: collab})
}

func (h *Handler) handleDisassociateCollaborator(
	ctx context.Context, c *echo.Context, agentID, agentVersion, collaboratorID string,
) error {
	if err := h.Backend.DisassociateAgentCollaborator(ctx, agentID, agentVersion, collaboratorID); err != nil {
		return handleErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListCollaborators(
	ctx context.Context, c *echo.Context, agentID, agentVersion string,
) error {
	maxResults, nextToken := pageParams(c.Request().URL.Query())

	collabs, outToken, err := h.Backend.ListAgentCollaborators(ctx, agentID, agentVersion, maxResults, nextToken)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"agentCollaboratorSummaries": collabs,
		keyNextToken:                 outToken,
	})
}

func classifyCollabPath(method string, segs []string) string {
	idx := indexOf(segs, "collaborators")
	hasID := len(segs) > idx+1 && segs[idx+1] != ""

	if !hasID {
		switch method {
		case http.MethodPut:
			return opAssociateAgentCollaborator
		case http.MethodPost, http.MethodGet:
			return opListAgentCollaborators
		}
	}

	switch method {
	case http.MethodGet:
		return opGetAgentCollaborator
	case http.MethodPut:
		return opUpdateAgentCollaborator
	case http.MethodDelete:
		return opDisassociateAgentCollaborator
	}

	return opUnknown
}
