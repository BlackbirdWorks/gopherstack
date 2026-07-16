package bedrock

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func (h *AgentsHandler) dispatchFlowAliasRoutes(
	c *echo.Context, flowID, suffix, method string, body []byte,
) error {
	if suffix == suffixAliases {
		switch method {
		case http.MethodPost, http.MethodPut:
			return h.handleCreateFlowAlias(c, flowID, body)
		case http.MethodGet:
			return h.handleListFlowAliases(c, flowID)
		}
	}

	if aliasID, ok := strings.CutPrefix(suffix, suffixAliases+"/"); ok {
		switch method {
		case http.MethodGet:
			return h.handleGetFlowAlias(c, flowID, aliasID)
		case http.MethodPut:
			return h.handleUpdateFlowAlias(c, flowID, aliasID, body)
		case http.MethodDelete:
			return h.handleDeleteFlowAlias(c, flowID, aliasID)
		}
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown flow alias operation"),
	)
}

func (h *AgentsHandler) handleCreateFlowAlias(
	c *echo.Context, flowID string, body []byte,
) error {
	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	fa, err := h.Backend.CreateFlowAlias(flowID, req.Name, req.Description)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusCreated, map[string]any{respFlowAlias: fa})
}

func (h *AgentsHandler) handleGetFlowAlias(
	c *echo.Context, flowID, aliasID string,
) error {
	fa, err := h.Backend.GetFlowAlias(flowID, aliasID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respFlowAlias: fa})
}

func (h *AgentsHandler) handleListFlowAliases(c *echo.Context, flowID string) error {
	list, outToken := h.Backend.ListFlowAliases(flowID, 0, c.QueryParam("nextToken"))
	resp := map[string]any{"flowAliasSummaries": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *AgentsHandler) handleUpdateFlowAlias(
	c *echo.Context, flowID, aliasID string, body []byte,
) error {
	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	fa, err := h.Backend.UpdateFlowAlias(flowID, aliasID, req.Name, req.Description)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respFlowAlias: fa})
}

func (h *AgentsHandler) handleDeleteFlowAlias(
	c *echo.Context, flowID, aliasID string,
) error {
	if err := h.Backend.DeleteFlowAlias(flowID, aliasID); err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(
		http.StatusOK,
		map[string]any{keyFlowID: flowID, keyFlowAliasID: aliasID, keyStatus: statusDeleting},
	)
}
