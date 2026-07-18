package bedrock

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func (h *AgentsHandler) dispatchAliasRoutes(
	c *echo.Context,
	agentID, suffix, method string,
	body []byte,
) error {
	if suffix == suffixAgentAliases && (method == http.MethodPost || method == http.MethodPut) {
		return h.handleCreateAgentAlias(c, agentID, body)
	}

	if suffix == suffixAgentAliases && method == http.MethodGet {
		return h.handleListAgentAliases(c, agentID)
	}

	if aliasID, aliasOK := strings.CutPrefix(suffix, suffixAgentAliases+"/"); aliasOK {
		switch method {
		case http.MethodGet:
			return h.handleGetAgentAlias(c, agentID, aliasID)
		case http.MethodPut:
			return h.handleUpdateAgentAlias(c, agentID, aliasID, body)
		case http.MethodDelete:
			return h.handleDeleteAgentAlias(c, agentID, aliasID)
		}
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown alias operation"),
	)
}

func (h *AgentsHandler) handleCreateAgentAlias(c *echo.Context, agentID string, body []byte) error {
	var req struct {
		AgentAliasName       string              `json:"agentAliasName"`
		RoutingConfiguration []AgentAliasRouting `json:"routingConfiguration"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	alias, err := h.Backend.CreateAgentAlias(agentID, req.AgentAliasName, aliasVersion(req.RoutingConfiguration))
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusAccepted, map[string]any{respAgentAlias: alias})
}

func (h *AgentsHandler) handleGetAgentAlias(c *echo.Context, agentID, aliasID string) error {
	alias, err := h.Backend.GetAgentAlias(agentID, aliasID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respAgentAlias: alias})
}

func (h *AgentsHandler) handleListAgentAliases(c *echo.Context, agentID string) error {
	list, outToken := h.Backend.ListAgentAliases(agentID, 0, c.QueryParam("nextToken"))
	resp := map[string]any{"agentAliasSummaries": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *AgentsHandler) handleUpdateAgentAlias(
	c *echo.Context,
	agentID, aliasID string,
	body []byte,
) error {
	var req struct {
		AgentAliasName       string              `json:"agentAliasName"`
		RoutingConfiguration []AgentAliasRouting `json:"routingConfiguration"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	alias, err := h.Backend.UpdateAgentAlias(
		agentID,
		aliasID,
		req.AgentAliasName,
		aliasVersion(req.RoutingConfiguration),
	)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respAgentAlias: alias})
}

func aliasVersion(config []AgentAliasRouting) string {
	if len(config) == 0 {
		return ""
	}

	return config[0].AgentVersion
}

func (h *AgentsHandler) handleDeleteAgentAlias(c *echo.Context, agentID, aliasID string) error {
	if err := h.Backend.DeleteAgentAlias(agentID, aliasID); err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(
		http.StatusAccepted,
		map[string]any{
			keyAgentID:         agentID,
			"agentAliasId":     aliasID,
			"agentAliasStatus": statusDeleting,
		},
	)
}
