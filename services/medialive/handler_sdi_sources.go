package medialive

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- SdiSource handlers ---

func toSdiSourceOutput(s *SdiSource) map[string]any {
	inputs := s.Inputs
	if inputs == nil {
		inputs = []string{}
	}

	return map[string]any{
		keyArn: s.ARN, keyID: s.ID, keyName: s.Name,
		"type": s.Type, "mode": s.Mode, keyState: s.State, "inputs": inputs,
	}
}

func (h *Handler) handleCreateSdiSource(c *echo.Context, body map[string]any) error {
	name, _ := body["name"].(string)
	sdiType, _ := body["type"].(string)
	mode, _ := body["mode"].(string)
	tags := extractTags(body)

	s, err := h.Backend.CreateSdiSource(name, sdiType, mode, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, map[string]any{keySdiSource: toSdiSourceOutput(s)})
}

func (h *Handler) handleDescribeSdiSource(c *echo.Context, sdiSourceID string) error {
	s, err := h.Backend.DescribeSdiSource(sdiSourceID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keySdiSource: toSdiSourceOutput(s)})
}

func (h *Handler) handleUpdateSdiSource(
	c *echo.Context,
	sdiSourceID string,
	body map[string]any,
) error {
	name, _ := body["name"].(string)
	sdiType, _ := body["type"].(string)
	mode, _ := body["mode"].(string)

	s, err := h.Backend.UpdateSdiSource(sdiSourceID, name, sdiType, mode)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keySdiSource: toSdiSourceOutput(s)})
}

func (h *Handler) handleDeleteSdiSource(c *echo.Context, sdiSourceID string) error {
	s, err := h.Backend.DeleteSdiSource(sdiSourceID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keySdiSource: toSdiSourceOutput(s)})
}

func (h *Handler) handleListSdiSources(c *echo.Context) error {
	sources, nextToken, err := h.Backend.ListSdiSources(0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(sources))
	for _, s := range sources {
		out = append(out, toSdiSourceOutput(s))
	}

	resp := map[string]any{"sdiSources": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}
