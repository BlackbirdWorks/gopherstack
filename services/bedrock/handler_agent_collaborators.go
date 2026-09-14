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

	// ListAgentCollaborators is real bedrock-agent@v1.58.4
	// serializers.go:4233: POST .../agentcollaborators/;
	// AssociateAgentCollaborator is real serializers.go:49: PUT (the SAME
	// path) -- method alone disambiguates them. GET is accepted too as
	// harmless extra leniency for this package's own tests.
	if collabSuffix == "/agentcollaborators" {
		switch method {
		case http.MethodPut:
			return h.handleAssociateAgentCollaborator(c, agentID, body)
		case http.MethodPost, http.MethodGet:
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

// wireAgentDescriptor matches types.AgentDescriptor (serializers.go:7469):
// the collaborator agent is identified by its alias ARN, nested here, never
// a flat "collaboratorArn".
type wireAgentDescriptor struct {
	AliasArn string `json:"aliasArn"`
}

type wireAgentCollaboratorRequest struct {
	AgentDescriptor          wireAgentDescriptor `json:"agentDescriptor"`
	AgentVersion             string              `json:"agentVersion"`
	CollaborationInstruction string              `json:"collaborationInstruction"`
	CollaboratorName         string              `json:"collaboratorName"`
	RelayConversationHistory string              `json:"relayConversationHistory"`
}

// wireAgentCollaborator matches types.AgentCollaborator
// (deserializers.go:14971): agentDescriptor, agentId, agentVersion,
// collaborationInstruction, collaboratorId, collaboratorName, createdAt,
// lastUpdatedAt, relayConversationHistory.
type wireAgentCollaborator struct {
	AgentDescriptor          wireAgentDescriptor `json:"agentDescriptor"`
	AgentID                  string              `json:"agentId"`
	AgentVersion             string              `json:"agentVersion"`
	CollaborationInstruction string              `json:"collaborationInstruction"`
	CollaboratorID           string              `json:"collaboratorId"`
	CollaboratorName         string              `json:"collaboratorName"`
	CreatedAt                string              `json:"createdAt"`
	LastUpdatedAt            string              `json:"lastUpdatedAt"`
	RelayConversationHistory string              `json:"relayConversationHistory,omitempty"`
}

func agentCollaboratorWire(ac *AgentCollaborator) wireAgentCollaborator {
	return wireAgentCollaborator{
		AgentDescriptor:          wireAgentDescriptor{AliasArn: ac.AgentAliasArn},
		AgentID:                  ac.AgentID,
		AgentVersion:             ac.AgentVersion,
		CollaborationInstruction: ac.CollaborationInstruction,
		CollaboratorID:           ac.CollaboratorID,
		CollaboratorName:         ac.CollaboratorName,
		CreatedAt:                ac.CreatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z"),
		LastUpdatedAt:            ac.LastUpdatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z"),
		RelayConversationHistory: ac.RelayConversation,
	}
}

func (h *AgentsHandler) handleAssociateAgentCollaborator(
	c *echo.Context, agentID string, body []byte,
) error {
	var req wireAgentCollaboratorRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	ac, err := h.Backend.AssociateAgentCollaborator(
		agentID, req.AgentVersion, req.AgentDescriptor.AliasArn,
		req.CollaboratorName, req.CollaborationInstruction, req.RelayConversationHistory,
	)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respCollaborator: agentCollaboratorWire(ac)})
}

func (h *AgentsHandler) handleGetAgentCollaborator(
	c *echo.Context, agentID, collaboratorID string,
) error {
	ac, err := h.Backend.GetAgentCollaborator(agentID, collaboratorID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respCollaborator: agentCollaboratorWire(ac)})
}

func (h *AgentsHandler) handleListAgentCollaborators(c *echo.Context, agentID string) error {
	list, outToken := h.Backend.ListAgentCollaborators(agentID, 0, c.QueryParam("nextToken"))

	summaries := make([]wireAgentCollaborator, 0, len(list))
	for _, ac := range list {
		summaries = append(summaries, agentCollaboratorWire(ac))
	}

	resp := map[string]any{"agentCollaboratorSummaries": summaries}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *AgentsHandler) handleUpdateAgentCollaborator(
	c *echo.Context, agentID, collaboratorID string, body []byte,
) error {
	var req wireAgentCollaboratorRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	ac, err := h.Backend.UpdateAgentCollaborator(
		agentID, collaboratorID, req.AgentDescriptor.AliasArn,
		req.CollaboratorName, req.CollaborationInstruction, req.RelayConversationHistory,
	)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respCollaborator: agentCollaboratorWire(ac)})
}

func (h *AgentsHandler) handleDisassociateAgentCollaborator(
	c *echo.Context, agentID, collaboratorID string,
) error {
	if err := h.Backend.DisassociateAgentCollaborator(agentID, collaboratorID); err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.NoContent(http.StatusNoContent)
}
