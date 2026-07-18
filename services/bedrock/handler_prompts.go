package bedrock

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// dispatchPromptRoutes handles /prompts and /prompts/{promptId}/...
func (h *AgentsHandler) dispatchPromptRoutes(
	c *echo.Context, path, method string, body []byte,
) error {
	if path == promptsPath {
		switch method {
		case http.MethodPost, http.MethodPut:
			return h.handleCreatePrompt(c, body)
		case http.MethodGet:
			return h.handleListPrompts(c)
		}
	}

	rest, ok := strings.CutPrefix(path, "/prompts/")
	if !ok {
		return c.JSON(
			http.StatusNotFound,
			agentErrResp("UnknownOperationException", "unknown prompt operation: "+path),
		)
	}

	parts := strings.SplitN(rest, "/", splitInTwo)
	promptID := parts[0]
	suffix := ""

	if len(parts) == splitInTwo {
		suffix = "/" + parts[1]
	}

	return h.dispatchPromptIDRoutes(c, promptID, suffix, method, body)
}

func (h *AgentsHandler) dispatchPromptIDRoutes(
	c *echo.Context, promptID, suffix, method string, body []byte,
) error {
	switch {
	case suffix == "" && method == http.MethodGet:
		return h.handleGetPrompt(c, promptID)
	case suffix == "" && method == http.MethodPut:
		return h.handleUpdatePrompt(c, promptID, body)
	case suffix == "" && method == http.MethodDelete:
		return h.handleDeletePrompt(c, promptID)
	case strings.HasPrefix(suffix, suffixVersions):
		return h.dispatchPromptVersionRoutes(c, promptID, suffix, method)
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown prompt operation"),
	)
}

func (h *AgentsHandler) handleCreatePrompt(c *echo.Context, body []byte) error {
	var req struct {
		Tags        map[string]string `json:"tags"`
		Name        string            `json:"name"`
		Description string            `json:"description"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	p, err := h.Backend.CreatePrompt(req.Name, req.Description, req.Tags)
	if err != nil {
		if errors.Is(err, ErrAlreadyExists) {
			return c.JSON(http.StatusConflict, agentErrResp("ConflictException", err.Error()))
		}

		return c.JSON(http.StatusBadRequest, agentErrResp("ValidationException", err.Error()))
	}

	return c.JSON(http.StatusCreated, map[string]any{respPrompt: p})
}

func (h *AgentsHandler) handleGetPrompt(c *echo.Context, promptID string) error {
	p, err := h.Backend.GetPrompt(promptID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respPrompt: p})
}

func (h *AgentsHandler) handleListPrompts(c *echo.Context) error {
	list, outToken := h.Backend.ListPrompts(0, c.QueryParam("nextToken"))
	resp := map[string]any{"promptSummaries": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *AgentsHandler) handleUpdatePrompt(
	c *echo.Context, promptID string, body []byte,
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

	p, err := h.Backend.UpdatePrompt(promptID, req.Name, req.Description)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respPrompt: p})
}

func (h *AgentsHandler) handleDeletePrompt(c *echo.Context, promptID string) error {
	if err := h.Backend.DeletePrompt(promptID); err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{keyPromptID: promptID, keyStatus: statusDeleting})
}
