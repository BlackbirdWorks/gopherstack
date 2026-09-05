package bedrockagent

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// ---------------------------------------------------------------------------
// Agent alias handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateAgentAlias(
	ctx context.Context, c *echo.Context, agentID string, body []byte,
) error {
	var req struct {
		Tags                 map[string]string `json:"tags"`
		AgentAliasName       string            `json:"agentAliasName"`
		Description          string            `json:"description"`
		RoutingConfiguration []AliasRouting    `json:"routingConfiguration"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	al, err := h.Backend.CreateAgentAlias(ctx, agentID, AliasConfig{
		AliasName:            req.AgentAliasName,
		Description:          req.Description,
		RoutingConfiguration: req.RoutingConfiguration,
		Tags:                 req.Tags,
	})
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAgentAlias: al})
}

func (h *Handler) handleGetAgentAlias(
	ctx context.Context, c *echo.Context, agentID, aliasID string,
) error {
	al, err := h.Backend.GetAgentAlias(ctx, agentID, aliasID)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAgentAlias: al})
}

func (h *Handler) handleUpdateAgentAlias(
	ctx context.Context, c *echo.Context, agentID, aliasID string, body []byte,
) error {
	var req struct {
		Tags                 map[string]string `json:"tags"`
		AgentAliasName       string            `json:"agentAliasName"`
		Description          string            `json:"description"`
		RoutingConfiguration []AliasRouting    `json:"routingConfiguration"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	al, err := h.Backend.UpdateAgentAlias(ctx, agentID, aliasID, AliasConfig{
		AliasName:            req.AgentAliasName,
		Description:          req.Description,
		RoutingConfiguration: req.RoutingConfiguration,
		Tags:                 req.Tags,
	})
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAgentAlias: al})
}

func (h *Handler) handleDeleteAgentAlias(
	ctx context.Context, c *echo.Context, agentID, aliasID string,
) error {
	if err := h.Backend.DeleteAgentAlias(ctx, agentID, aliasID); err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyAgentID:         agentID,
		"agentAliasId":     aliasID,
		"agentAliasStatus": statusDeleting,
	})
}

func (h *Handler) handleListAgentAliases(
	ctx context.Context, c *echo.Context, agentID string, body []byte,
) error {
	maxResults, nextToken := bodyPageParams(body)

	summaries, outToken, err := h.Backend.ListAgentAliases(ctx, agentID, maxResults, nextToken)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"agentAliasSummaries": summaries,
		keyNextToken:          outToken,
	})
}

func classifyAliasPath(method string, segs []string) string {
	idx := indexOf(segs, "agentaliases")
	hasID := len(segs) > idx+1 && segs[idx+1] != ""

	if !hasID {
		switch method {
		case http.MethodPut:
			return opCreateAgentAlias
		case http.MethodPost, http.MethodGet:
			return opListAgentAliases
		}
	}

	switch method {
	case http.MethodGet:
		return opGetAgentAlias
	case http.MethodPut:
		return opUpdateAgentAlias
	case http.MethodDelete:
		return opDeleteAgentAlias
	}

	return opUnknown
}
