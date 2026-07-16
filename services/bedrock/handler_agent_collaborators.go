package bedrock

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// dispatchAgentCollabRoutes handles /agents/{agentId}/agentversions/{v}/agentcollaborators/...
func (h *AgentsHandler) dispatchAgentCollabRoutes(
	c *echo.Context, agentID, suffix, method string, body []byte,
) error {
	// suffix looks like: /agentversions/{ver}/agentcollaborators or
	//                    /agentversions/{ver}/agentcollaborators/{id}
	collabSuffix := collabSuffixFrom(suffix)

	if collabSuffix == "" {
		return c.JSON(
			http.StatusNotFound,
			agentErrResp("UnknownOperationException", "malformed collaborator path"),
		)
	}

	if collabSuffix == "/agentcollaborators" {
		switch method {
		case http.MethodPost:
			return h.handleAssociateAgentCollaborator(c, agentID, body)
		case http.MethodGet:
			return h.handleListAgentCollaborators(c, agentID)
		}
	}

	if collabID, ok := strings.CutPrefix(collabSuffix, "/agentcollaborators/"); ok {
		switch method {
		case http.MethodGet:
			return h.handleGetAgentCollaborator(c, agentID, collabID)
		case http.MethodPut:
			return h.handleUpdateAgentCollaborator(c, agentID, collabID, body)
		case http.MethodDelete:
			return h.handleDisassociateAgentCollaborator(c, agentID, collabID)
		}
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown collaborator operation"),
	)
}

// collabSuffixFrom extracts the /agentcollaborators[/...] part from an agent suffix.
func collabSuffixFrom(suffix string) string {
	idx := strings.Index(suffix, "/agentcollaborators")
	if idx < 0 {
		return ""
	}

	return suffix[idx:]
}

func (h *AgentsHandler) handleAssociateAgentCollaborator(
	c *echo.Context, agentID string, body []byte,
) error {
	var req struct {
		AgentVersion      string `json:"agentVersion"`
		CollaboratorArn   string `json:"collaboratorArn"`
		RelayConversation string `json:"relayConversationHistory"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	ac, err := h.Backend.AssociateAgentCollaborator(
		agentID, req.AgentVersion, req.CollaboratorArn, req.RelayConversation,
	)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respCollaborator: ac})
}

func (h *AgentsHandler) handleGetAgentCollaborator(
	c *echo.Context, agentID, collaboratorID string,
) error {
	ac, err := h.Backend.GetAgentCollaborator(agentID, collaboratorID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respCollaborator: ac})
}

func (h *AgentsHandler) handleListAgentCollaborators(c *echo.Context, agentID string) error {
	list, outToken := h.Backend.ListAgentCollaborators(agentID, 0, c.QueryParam("nextToken"))
	resp := map[string]any{"agentCollaboratorSummaries": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *AgentsHandler) handleUpdateAgentCollaborator(
	c *echo.Context, agentID, collaboratorID string, body []byte,
) error {
	var req struct {
		RelayConversation string `json:"relayConversationHistory"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	ac, err := h.Backend.UpdateAgentCollaborator(agentID, collaboratorID, req.RelayConversation)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respCollaborator: ac})
}

func (h *AgentsHandler) handleDisassociateAgentCollaborator(
	c *echo.Context, agentID, collaboratorID string,
) error {
	if err := h.Backend.DisassociateAgentCollaborator(agentID, collaboratorID); err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.NoContent(http.StatusNoContent)
}
