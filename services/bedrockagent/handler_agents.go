package bedrockagent

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// ---------------------------------------------------------------------------
// Agent handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateAgent(ctx context.Context, c *echo.Context, body []byte) error {
	var req struct {
		Tags                    map[string]string `json:"tags"`
		Guardrail               map[string]any    `json:"guardrailConfiguration"`
		Memory                  map[string]any    `json:"memoryConfiguration"`
		AgentName               string            `json:"agentName"`
		Collaboration           string            `json:"agentCollaboration"`
		Description             string            `json:"description"`
		FoundationModel         string            `json:"foundationModel"`
		Instruction             string            `json:"instruction"`
		RoleARN                 string            `json:"agentResourceRoleArn"`
		IdleSessionTTLInSeconds int               `json:"idleSessionTTLInSeconds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	agent, err := h.Backend.CreateAgent(ctx, AgentConfig{
		AgentName:               req.AgentName,
		Collaboration:           req.Collaboration,
		Description:             req.Description,
		FoundationModel:         req.FoundationModel,
		Instruction:             req.Instruction,
		RoleARN:                 req.RoleARN,
		Tags:                    req.Tags,
		Guardrail:               req.Guardrail,
		Memory:                  req.Memory,
		IdleSessionTTLInSeconds: req.IdleSessionTTLInSeconds,
	})
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAgent: agent})
}

func (h *Handler) handleGetAgent(ctx context.Context, c *echo.Context, agentID string) error {
	agent, err := h.Backend.GetAgent(ctx, agentID)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAgent: agent})
}

func (h *Handler) handleUpdateAgent(
	ctx context.Context, c *echo.Context, agentID string, body []byte,
) error {
	var req struct {
		Tags                    map[string]string `json:"tags"`
		Guardrail               map[string]any    `json:"guardrailConfiguration"`
		Memory                  map[string]any    `json:"memoryConfiguration"`
		AgentName               string            `json:"agentName"`
		Collaboration           string            `json:"agentCollaboration"`
		Description             string            `json:"description"`
		FoundationModel         string            `json:"foundationModel"`
		Instruction             string            `json:"instruction"`
		RoleARN                 string            `json:"agentResourceRoleArn"`
		IdleSessionTTLInSeconds int               `json:"idleSessionTTLInSeconds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	agent, err := h.Backend.UpdateAgent(ctx, agentID, AgentConfig{
		AgentName:               req.AgentName,
		Collaboration:           req.Collaboration,
		Description:             req.Description,
		FoundationModel:         req.FoundationModel,
		Instruction:             req.Instruction,
		RoleARN:                 req.RoleARN,
		Tags:                    req.Tags,
		Guardrail:               req.Guardrail,
		Memory:                  req.Memory,
		IdleSessionTTLInSeconds: req.IdleSessionTTLInSeconds,
	})
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAgent: agent})
}

func (h *Handler) handleDeleteAgent(ctx context.Context, c *echo.Context, agentID string) error {
	if err := h.Backend.DeleteAgent(ctx, agentID); err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAgentID: agentID, keyAgentStatus: statusDeleting})
}

func (h *Handler) handleListAgents(ctx context.Context, c *echo.Context) error {
	maxResults, nextToken := pageParams(c.Request().URL.Query())

	agents, outToken, err := h.Backend.ListAgents(ctx, maxResults, nextToken)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"agentSummaries": agents, keyNextToken: outToken})
}

func (h *Handler) handlePrepareAgent(ctx context.Context, c *echo.Context, agentID string) error {
	agent, err := h.Backend.PrepareAgent(ctx, agentID)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusAccepted, map[string]any{
		keyAgentID:      agent.AgentID,
		keyAgentStatus:  agent.AgentStatus,
		keyAgentVersion: agent.AgentVersion,
		"preparedAt":    agent.PreparedAt,
	})
}

func classifyAgentPath(method, path string) string {
	rest, _ := strings.CutPrefix(path, agentsBase+"/")
	segs := strings.Split(rest, "/")

	switch {
	case len(segs) == 1 && method == http.MethodGet:
		return opGetAgent
	case len(segs) == 1 && method == http.MethodPut:
		return opUpdateAgent
	case len(segs) == 1 && method == http.MethodDelete:
		return opDeleteAgent
	case len(segs) == 2 && segs[1] == "prepare":
		return opPrepareAgent
	case containsSeg(segs, "agentversions"):
		return classifyAgentVersionedSubPath(method, segs)
	case containsSeg(segs, "agentaliases"):
		return classifyAliasPath(method, segs)
	}

	return opUnknown
}
