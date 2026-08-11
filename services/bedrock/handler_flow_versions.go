package bedrock

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func (h *AgentsHandler) dispatchFlowVersionRoutes(
	c *echo.Context, flowID, suffix, method string,
) error {
	if suffix == suffixVersions {
		switch method {
		case http.MethodPost, http.MethodPut:
			return h.handleCreateFlowVersion(c, flowID)
		case http.MethodGet:
			return h.handleListFlowVersions(c, flowID)
		}
	}

	if ver, ok := strings.CutPrefix(suffix, suffixVersions+"/"); ok {
		switch method {
		case http.MethodGet:
			return h.handleGetFlowVersion(c, flowID, ver)
		case http.MethodDelete:
			return h.handleDeleteFlowVersion(c, flowID, ver)
		}
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown flow version operation"),
	)
}

func (h *AgentsHandler) handleCreateFlowVersion(c *echo.Context, flowID string) error {
	fv, err := h.Backend.CreateFlowVersion(flowID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusCreated, fv)
}

func (h *AgentsHandler) handleGetFlowVersion(
	c *echo.Context, flowID, version string,
) error {
	fv, err := h.Backend.GetFlowVersion(flowID, version)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, fv)
}

func (h *AgentsHandler) handleListFlowVersions(c *echo.Context, flowID string) error {
	list, outToken := h.Backend.ListFlowVersions(flowID, 0, c.QueryParam("nextToken"))
	resp := map[string]any{"flowVersionSummaries": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *AgentsHandler) handleDeleteFlowVersion(
	c *echo.Context, flowID, version string,
) error {
	if err := h.Backend.DeleteFlowVersion(flowID, version); err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(
		http.StatusOK,
		map[string]any{keyID: flowID, keyVersion: version},
	)
}
