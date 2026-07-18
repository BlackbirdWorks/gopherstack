package bedrock

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *AgentsHandler) handleCreateKnowledgeBase(c *echo.Context, body []byte) error {
	var req struct {
		KnowledgeBaseConfiguration map[string]any    `json:"knowledgeBaseConfiguration"`
		StorageConfiguration       map[string]any    `json:"storageConfiguration"`
		Tags                       map[string]string `json:"tags"`
		Name                       string            `json:"name"`
		Description                string            `json:"description"`
		RoleArn                    string            `json:"roleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	kb, err := h.Backend.CreateKnowledgeBase(
		req.Name,
		req.Description,
		req.RoleArn,
		req.KnowledgeBaseConfiguration,
		req.StorageConfiguration,
		req.Tags,
	)
	if err != nil {
		if errors.Is(err, ErrAlreadyExists) {
			return c.JSON(http.StatusConflict, agentErrResp("ConflictException", err.Error()))
		}

		return c.JSON(http.StatusBadRequest, agentErrResp("ValidationException", err.Error()))
	}

	return c.JSON(http.StatusAccepted, map[string]any{respKnowledgeBase: kb})
}

func (h *AgentsHandler) handleGetKnowledgeBase(c *echo.Context, kbID string) error {
	kb, err := h.Backend.GetKnowledgeBase(kbID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respKnowledgeBase: kb})
}

func (h *AgentsHandler) handleListKnowledgeBases(c *echo.Context) error {
	list, outToken := h.Backend.ListKnowledgeBases(0, c.QueryParam("nextToken"))
	resp := map[string]any{"knowledgeBaseSummaries": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *AgentsHandler) handleUpdateKnowledgeBase(c *echo.Context, kbID string, body []byte) error {
	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		RoleArn     string `json:"roleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	kb, err := h.Backend.UpdateKnowledgeBase(kbID, req.Name, req.Description, req.RoleArn)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respKnowledgeBase: kb})
}

func (h *AgentsHandler) handleDeleteKnowledgeBase(c *echo.Context, kbID string) error {
	if err := h.Backend.DeleteKnowledgeBase(kbID); err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(
		http.StatusAccepted,
		map[string]any{keyKnowledgeBaseID: kbID, keyStatus: statusDeleting},
	)
}
