package bedrock

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func (h *AgentsHandler) dispatchAgentKBRoutes(
	c *echo.Context,
	agentID, suffix, method string,
	body []byte,
) error {
	// suffix like /agentversions/DRAFT/knowledgebases or /agentversions/DRAFT/knowledgebases/{kbId}
	//
	// ListAgentKnowledgeBases is real bedrock-agent@v1.58.4
	// serializers.go:4341: POST .../knowledgebases/; AssociateAgentKnowledgeBase
	// is real serializers.go:174: PUT (the SAME path) -- method alone
	// disambiguates them. GET is accepted too as harmless extra leniency
	// for this package's own tests.
	if strings.HasSuffix(suffix, "/knowledgebases") && method == http.MethodPut {
		return h.handleAssociateAgentKB(c, agentID, body)
	}

	if strings.HasSuffix(suffix, "/knowledgebases") && (method == http.MethodPost || method == http.MethodGet) {
		return h.handleListAgentKBs(c, agentID)
	}

	// /agentversions/{version}/knowledgebases/{kbId}
	parts := strings.Split(suffix, "/knowledgebases/")
	if len(parts) == splitInTwo {
		kbID := parts[1]

		switch method {
		case http.MethodGet:
			return h.handleGetAgentKB(c, agentID, kbID)
		case http.MethodPut:
			return h.handleUpdateAgentKB(c, agentID, kbID, body)
		case http.MethodDelete:
			return h.handleDisassociateAgentKB(c, agentID, kbID)
		}
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown kb operation"),
	)
}

func (h *AgentsHandler) handleAssociateAgentKB(c *echo.Context, agentID string, body []byte) error {
	var req struct {
		KnowledgeBaseID string `json:"knowledgeBaseId"`
		Description     string `json:"description"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	assoc, err := h.Backend.AssociateAgentKnowledgeBase(
		agentID,
		req.KnowledgeBaseID,
		req.Description,
	)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respAgentKnowledgeBase: assoc})
}

func (h *AgentsHandler) handleGetAgentKB(c *echo.Context, agentID, kbID string) error {
	assoc, err := h.Backend.GetAgentKnowledgeBase(agentID, kbID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respAgentKnowledgeBase: assoc})
}

func (h *AgentsHandler) handleUpdateAgentKB(
	c *echo.Context, agentID, kbID string, body []byte,
) error {
	var req struct {
		Description        string `json:"description"`
		KnowledgeBaseState string `json:"knowledgeBaseState"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, agentErrResp("ValidationException", "invalid request body"))
	}

	assoc, err := h.Backend.UpdateAgentKnowledgeBase(agentID, kbID, req.Description, req.KnowledgeBaseState)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respAgentKnowledgeBase: assoc})
}

func (h *AgentsHandler) handleListAgentKBs(c *echo.Context, agentID string) error {
	list, outToken := h.Backend.ListAgentKnowledgeBases(agentID, 0, c.QueryParam("nextToken"))
	resp := map[string]any{"agentKnowledgeBaseSummaries": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *AgentsHandler) handleDisassociateAgentKB(c *echo.Context, agentID, kbID string) error {
	if err := h.Backend.DisassociateAgentKnowledgeBase(agentID, kbID); err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.NoContent(http.StatusNoContent)
}
