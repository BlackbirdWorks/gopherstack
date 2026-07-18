package bedrockagent

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// ---------------------------------------------------------------------------
// Agent–KB association handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleAssociateAgentKB(
	ctx context.Context, c *echo.Context, agentID, agentVersion string, body []byte,
) error {
	var req struct {
		KnowledgeBaseID    string `json:"knowledgeBaseId"`
		Description        string `json:"description"`
		KnowledgeBaseState string `json:"knowledgeBaseState"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	assoc, err := h.Backend.AssociateAgentKnowledgeBase(
		ctx, agentID, agentVersion, req.KnowledgeBaseID, req.Description, req.KnowledgeBaseState,
	)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAgentKB: assoc})
}

func (h *Handler) handleGetAgentKB(
	ctx context.Context, c *echo.Context, agentID, agentVersion, kbID string,
) error {
	assoc, err := h.Backend.GetAgentKnowledgeBase(ctx, agentID, agentVersion, kbID)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAgentKB: assoc})
}

func (h *Handler) handleUpdateAgentKB(
	ctx context.Context, c *echo.Context, agentID, agentVersion, kbID string, body []byte,
) error {
	var req struct {
		Description        string `json:"description"`
		KnowledgeBaseState string `json:"knowledgeBaseState"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	assoc, err := h.Backend.UpdateAgentKnowledgeBase(
		ctx, agentID, agentVersion, kbID, req.Description, req.KnowledgeBaseState,
	)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAgentKB: assoc})
}

func (h *Handler) handleDisassociateAgentKB(
	ctx context.Context, c *echo.Context, agentID, agentVersion, kbID string,
) error {
	if err := h.Backend.DisassociateAgentKnowledgeBase(ctx, agentID, agentVersion, kbID); err != nil {
		return handleErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListAgentKBs(
	ctx context.Context, c *echo.Context, agentID, agentVersion string,
) error {
	maxResults, nextToken := pageParams(c.Request().URL.Query())

	assocs, outToken, err := h.Backend.ListAgentKnowledgeBases(ctx, agentID, agentVersion, maxResults, nextToken)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"agentKnowledgeBaseSummaries": assocs,
		keyNextToken:                  outToken,
	})
}

func classifyAgentKBPath(method string, segs []string) string {
	idx := indexOf(segs, "knowledgebases")
	hasID := len(segs) > idx+1 && segs[idx+1] != ""

	if !hasID {
		switch method {
		case http.MethodPut:
			return opAssociateAgentKnowledgeBase
		case http.MethodPost, http.MethodGet:
			return opListAgentKnowledgeBases
		}
	}

	switch method {
	case http.MethodGet:
		return opGetAgentKnowledgeBase
	case http.MethodPut:
		return opUpdateAgentKnowledgeBase
	case http.MethodDelete:
		return opDisassociateAgentKnowledgeBase
	}

	return opUnknown
}
