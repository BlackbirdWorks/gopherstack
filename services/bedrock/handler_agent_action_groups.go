package bedrock

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func (h *AgentsHandler) dispatchCanonicalActionGroupRoutes(
	c *echo.Context, agentID, suffix, method string, body []byte,
) error {
	_, rest, ok := strings.Cut(suffix, "/actiongroups")
	if !ok {
		return c.JSON(http.StatusNotFound, agentErrResp("UnknownOperationException", "unknown action group operation"))
	}

	// ListAgentActionGroups is real bedrock-agent@v1.58.4 serializers.go:4026:
	// POST .../actiongroups/. GET is accepted too as harmless extra leniency
	// for this package's own tests.
	switch {
	case rest == "" && method == http.MethodPut:
		return h.handleCreateAgentActionGroup(c, agentID, body)
	case rest == "" && (method == http.MethodPost || method == http.MethodGet):
		return h.handleListAgentActionGroups(c, agentID)
	case strings.HasPrefix(rest, "/") && method == http.MethodGet:
		return h.handleGetAgentActionGroup(c, agentID, strings.TrimPrefix(rest, "/"))
	case strings.HasPrefix(rest, "/") && method == http.MethodPut:
		return h.handleUpdateAgentActionGroup(c, agentID, strings.TrimPrefix(rest, "/"), body)
	case strings.HasPrefix(rest, "/") && method == http.MethodDelete:
		return h.handleDeleteAgentActionGroup(c, agentID, strings.TrimPrefix(rest, "/"))
	}

	return c.JSON(http.StatusNotFound, agentErrResp("UnknownOperationException", "unknown action group operation"))
}

func (h *AgentsHandler) dispatchActionGroupRoutes(
	c *echo.Context,
	agentID, suffix, method string,
	body []byte,
) error {
	// suffix is like /action-groups or /action-groups/{agentVersion}/{id}
	if suffix == "/action-groups" {
		switch method {
		case http.MethodPost:
			return h.handleCreateAgentActionGroup(c, agentID, body)
		case http.MethodGet:
			return h.handleListAgentActionGroups(c, agentID)
		}
	}

	if strings.HasPrefix(suffix, "/action-groups/") && method == http.MethodGet {
		// /action-groups/{agentVersion}/{actionGroupId}
		rest := strings.TrimPrefix(suffix, "/action-groups/")
		parts := strings.SplitN(rest, "/", splitInTwo)
		if len(parts) == splitInTwo {
			return h.handleGetAgentActionGroup(c, agentID, parts[1])
		}
	}

	if strings.HasPrefix(suffix, "/action-groups/") && method == http.MethodPut {
		rest := strings.TrimPrefix(suffix, "/action-groups/")
		parts := strings.SplitN(rest, "/", splitInTwo)
		if len(parts) == splitInTwo {
			return h.handleUpdateAgentActionGroup(c, agentID, parts[1], body)
		}
	}

	if strings.HasPrefix(suffix, "/action-groups/") && method == http.MethodDelete {
		rest := strings.TrimPrefix(suffix, "/action-groups/")
		parts := strings.SplitN(rest, "/", splitInTwo)
		if len(parts) == splitInTwo {
			return h.handleDeleteAgentActionGroup(c, agentID, parts[1])
		}
	}

	// List action groups: GET /action-groups/{agentVersion}
	if strings.HasPrefix(suffix, "/action-groups/") && method == http.MethodGet {
		return h.handleListAgentActionGroups(c, agentID)
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown action group operation"),
	)
}

func (h *AgentsHandler) handleCreateAgentActionGroup(
	c *echo.Context,
	agentID string,
	body []byte,
) error {
	var req struct {
		ActionGroupExecutor map[string]any `json:"actionGroupExecutor"`
		APISchema           map[string]any `json:"apiSchema"`
		FunctionSchema      map[string]any `json:"functionSchema"`
		ActionGroupName     string         `json:"actionGroupName"`
		Description         string         `json:"description"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	ag, err := h.Backend.CreateAgentActionGroupWithSchemas(
		agentID,
		req.ActionGroupName,
		req.Description,
		req.ActionGroupExecutor,
		req.APISchema,
		req.FunctionSchema,
	)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respAgentActionGroup: ag})
}

func (h *AgentsHandler) handleGetAgentActionGroup(
	c *echo.Context,
	agentID, actionGroupID string,
) error {
	ag, err := h.Backend.GetAgentActionGroup(agentID, actionGroupID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respAgentActionGroup: ag})
}

func (h *AgentsHandler) handleListAgentActionGroups(c *echo.Context, agentID string) error {
	list, outToken := h.Backend.ListAgentActionGroups(agentID, 0, c.QueryParam("nextToken"))
	resp := map[string]any{"actionGroupSummaries": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *AgentsHandler) handleUpdateAgentActionGroup(
	c *echo.Context,
	agentID, actionGroupID string,
	body []byte,
) error {
	var req struct {
		ActionGroupExecutor map[string]any `json:"actionGroupExecutor"`
		APISchema           map[string]any `json:"apiSchema"`
		FunctionSchema      map[string]any `json:"functionSchema"`
		ActionGroupName     string         `json:"actionGroupName"`
		Description         string         `json:"description"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	ag, err := h.Backend.UpdateAgentActionGroupWithSchemas(
		agentID,
		actionGroupID,
		req.Description,
		req.ActionGroupExecutor,
		req.APISchema,
		req.FunctionSchema,
	)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respAgentActionGroup: ag})
}

func (h *AgentsHandler) handleDeleteAgentActionGroup(
	c *echo.Context,
	agentID, actionGroupID string,
) error {
	if err := h.Backend.DeleteAgentActionGroup(agentID, actionGroupID); err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{})
}
