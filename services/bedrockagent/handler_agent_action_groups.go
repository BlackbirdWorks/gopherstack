package bedrockagent

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// ---------------------------------------------------------------------------
// Agent action group handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateAgentActionGroup(
	ctx context.Context, c *echo.Context, agentID, agentVersion string, body []byte,
) error {
	var req struct {
		ActionGroupExecutor map[string]any `json:"actionGroupExecutor"`
		APISchema           map[string]any `json:"apiSchema"`
		FunctionSchema      map[string]any `json:"functionSchema"`
		ActionGroupName     string         `json:"actionGroupName"`
		Description         string         `json:"description"`
		ActionGroupState    string         `json:"actionGroupState"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	ag, err := h.Backend.CreateAgentActionGroup(ctx, agentID, agentVersion, ActionGroupConfig{
		ActionGroupName:     req.ActionGroupName,
		Description:         req.Description,
		ActionGroupState:    req.ActionGroupState,
		ActionGroupExecutor: req.ActionGroupExecutor,
		APISchema:           req.APISchema,
		FunctionSchema:      req.FunctionSchema,
	})
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAgentActionGroup: ag})
}

func (h *Handler) handleGetAgentActionGroup(
	ctx context.Context, c *echo.Context, agentID, agentVersion, agID string,
) error {
	ag, err := h.Backend.GetAgentActionGroup(ctx, agentID, agentVersion, agID)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAgentActionGroup: ag})
}

func (h *Handler) handleUpdateAgentActionGroup(
	ctx context.Context, c *echo.Context, agentID, agentVersion, agID string, body []byte,
) error {
	var req struct {
		ActionGroupExecutor map[string]any `json:"actionGroupExecutor"`
		APISchema           map[string]any `json:"apiSchema"`
		FunctionSchema      map[string]any `json:"functionSchema"`
		ActionGroupName     string         `json:"actionGroupName"`
		Description         string         `json:"description"`
		ActionGroupState    string         `json:"actionGroupState"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	ag, err := h.Backend.UpdateAgentActionGroup(ctx, agentID, agentVersion, agID, ActionGroupConfig{
		ActionGroupName:     req.ActionGroupName,
		Description:         req.Description,
		ActionGroupState:    req.ActionGroupState,
		ActionGroupExecutor: req.ActionGroupExecutor,
		APISchema:           req.APISchema,
		FunctionSchema:      req.FunctionSchema,
	})
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAgentActionGroup: ag})
}

func (h *Handler) handleDeleteAgentActionGroup(
	ctx context.Context, c *echo.Context, agentID, agentVersion, agID string,
) error {
	if err := h.Backend.DeleteAgentActionGroup(ctx, agentID, agentVersion, agID); err != nil {
		return handleErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListAgentActionGroups(
	ctx context.Context, c *echo.Context, agentID, agentVersion string,
) error {
	maxResults, nextToken := pageParams(c.Request().URL.Query())

	summaries, outToken, err := h.Backend.ListAgentActionGroups(ctx, agentID, agentVersion, maxResults, nextToken)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"actionGroupSummaries": summaries,
		keyNextToken:           outToken,
	})
}

func classifyActionGroupPath(method string, segs []string) string {
	idx := indexOf(segs, "actiongroups")
	hasID := len(segs) > idx+1 && segs[idx+1] != ""

	if !hasID {
		switch method {
		case http.MethodPut:
			return opCreateAgentActionGroup
		case http.MethodPost, http.MethodGet:
			return opListAgentActionGroups
		}
	}

	switch method {
	case http.MethodGet:
		return opGetAgentActionGroup
	case http.MethodPut:
		return opUpdateAgentActionGroup
	case http.MethodDelete:
		return opDeleteAgentActionGroup
	}

	return opUnknown
}
