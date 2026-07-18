package bedrockagent

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// ---------------------------------------------------------------------------
// Prompt handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreatePrompt(ctx context.Context, c *echo.Context, body []byte) error {
	var req struct {
		Tags           map[string]string `json:"tags"`
		Name           string            `json:"name"`
		Description    string            `json:"description"`
		DefaultVariant string            `json:"defaultVariant"`
		Variants       []map[string]any  `json:"variants"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	p, err := h.Backend.CreatePrompt(ctx, PromptConfig{
		Name:           req.Name,
		Description:    req.Description,
		DefaultVariant: req.DefaultVariant,
		Variants:       req.Variants,
		Tags:           req.Tags,
	})
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusCreated, p)
}

func (h *Handler) handleGetPrompt(ctx context.Context, c *echo.Context, promptID string) error {
	p, err := h.Backend.GetPrompt(ctx, promptID)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, p)
}

func (h *Handler) handleUpdatePrompt(
	ctx context.Context, c *echo.Context, promptID string, body []byte,
) error {
	var req struct {
		Tags           map[string]string `json:"tags"`
		Name           string            `json:"name"`
		Description    string            `json:"description"`
		DefaultVariant string            `json:"defaultVariant"`
		Variants       []map[string]any  `json:"variants"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	p, err := h.Backend.UpdatePrompt(ctx, promptID, PromptConfig{
		Name:           req.Name,
		Description:    req.Description,
		DefaultVariant: req.DefaultVariant,
		Variants:       req.Variants,
		Tags:           req.Tags,
	})
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, p)
}

func (h *Handler) handleDeletePrompt(ctx context.Context, c *echo.Context, promptID string) error {
	if err := h.Backend.DeletePrompt(ctx, promptID); err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"id": promptID})
}

func (h *Handler) handleListPrompts(ctx context.Context, c *echo.Context) error {
	maxResults, nextToken := pageParams(c.Request().URL.Query())

	summaries, outToken, err := h.Backend.ListPrompts(ctx, maxResults, nextToken)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"promptSummaries": summaries, keyNextToken: outToken})
}

// ---------------------------------------------------------------------------
// Prompt version handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreatePromptVersion(
	ctx context.Context, c *echo.Context, promptID string, body []byte,
) error {
	var req struct {
		Description string `json:"description"`
	}

	_ = json.Unmarshal(body, &req)

	pv, err := h.Backend.CreatePromptVersion(ctx, promptID, req.Description)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusCreated, pv)
}

func (h *Handler) handleGetPromptVersion(
	ctx context.Context, c *echo.Context, promptID, version string,
) error {
	pv, err := h.Backend.GetPromptVersion(ctx, promptID, version)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, pv)
}

func (h *Handler) handleDeletePromptVersion(
	ctx context.Context, c *echo.Context, promptID, version string,
) error {
	if err := h.Backend.DeletePromptVersion(ctx, promptID, version); err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"id": promptID, "version": version})
}

func classifyPromptPath(method, path string) string {
	rest, _ := strings.CutPrefix(path, promptsBase+"/")
	segs := strings.Split(rest, "/")

	switch {
	case len(segs) == 1 && method == http.MethodGet:
		return opGetPrompt
	case len(segs) == 1 && method == http.MethodPut:
		return opUpdatePrompt
	case len(segs) == 1 && method == http.MethodDelete:
		return opDeletePrompt
	case containsSeg(segs, "versions"):
		return classifyPromptVersionPath(method, segs)
	}

	return opUnknown
}

func classifyPromptVersionPath(method string, segs []string) string {
	idx := indexOf(segs, "versions")
	hasID := len(segs) > idx+1 && segs[idx+1] != ""

	if !hasID && method == http.MethodPost {
		return opCreatePromptVersion
	}

	switch method {
	case http.MethodGet:
		return opGetPromptVersion
	case http.MethodDelete:
		return opDeletePromptVersion
	}

	return opUnknown
}
