package bedrock

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func (h *AgentsHandler) dispatchPromptVersionRoutes(
	c *echo.Context, promptID, suffix, method string,
) error {
	if suffix == suffixVersions {
		switch method {
		case http.MethodPost:
			return h.handleCreatePromptVersion(c, promptID)
		case http.MethodGet:
			return h.handleListPromptVersions(c, promptID)
		}
	}

	if ver, ok := strings.CutPrefix(suffix, suffixVersions+"/"); ok {
		switch method {
		case http.MethodGet:
			return h.handleGetPromptVersion(c, promptID, ver)
		case http.MethodDelete:
			return h.handleDeletePromptVersion(c, promptID, ver)
		}
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown prompt version operation"),
	)
}

func (h *AgentsHandler) handleCreatePromptVersion(c *echo.Context, promptID string) error {
	pv, err := h.Backend.CreatePromptVersion(promptID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusCreated, map[string]any{respPromptVersion: pv})
}

func (h *AgentsHandler) handleGetPromptVersion(
	c *echo.Context, promptID, version string,
) error {
	pv, err := h.Backend.GetPromptVersion(promptID, version)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respPromptVersion: pv})
}

func (h *AgentsHandler) handleListPromptVersions(c *echo.Context, promptID string) error {
	list, outToken := h.Backend.ListPromptVersions(promptID, 0, c.QueryParam("nextToken"))
	resp := map[string]any{"promptVersionSummaries": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *AgentsHandler) handleDeletePromptVersion(
	c *echo.Context, promptID, version string,
) error {
	if err := h.Backend.DeletePromptVersion(promptID, version); err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(
		http.StatusOK,
		map[string]any{keyID: promptID, keyVersion: version},
	)
}
