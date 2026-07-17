package mediaconvert

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func parsePresetRoute(method, suffix string) mcRoute {
	name := strings.TrimPrefix(suffix, "/")

	if name == "" {
		switch method {
		case http.MethodGet:
			return mcRoute{operation: opListPresets}
		case http.MethodPost:
			return mcRoute{operation: opCreatePreset}
		}
	}

	switch method {
	case http.MethodGet:
		return mcRoute{operation: opGetPreset, resource: name}
	case http.MethodPut:
		return mcRoute{operation: opUpdatePreset, resource: name}
	case http.MethodDelete:
		return mcRoute{operation: opDeletePreset, resource: name}
	}

	return mcRoute{operation: opUnknown}
}

// --- Preset handlers ---

type createPresetInput struct {
	Settings    map[string]any    `json:"settings,omitempty"`
	Tags        map[string]string `json:"tags,omitempty"`
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	Category    string            `json:"category,omitempty"`
}

type presetWrapper struct {
	Preset *Preset `json:"preset"`
}

type presetsListOutput struct {
	Presets []*Preset `json:"presets"`
}

func (h *Handler) handleCreatePreset(c *echo.Context, body []byte) error {
	var in createPresetInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if in.Name == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "name is required"))
	}

	p, err := h.Backend.CreatePreset(in.Name, in.Description, in.Category, in.Settings, in.Tags)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusCreated, presetWrapper{Preset: p})
}

func (h *Handler) handleGetPreset(c *echo.Context, name string) error {
	p, err := h.Backend.GetPreset(name)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, presetWrapper{Preset: p})
}

func (h *Handler) handleListPresets(c *echo.Context) error {
	presets := h.Backend.ListPresets()
	if presets == nil {
		presets = []*Preset{}
	}

	q := c.Request().URL.Query()
	category := q.Get("category")

	if category != "" {
		filtered := presets[:0:0]

		for _, p := range presets {
			if p.Category == category {
				filtered = append(filtered, p)
			}
		}

		presets = filtered
	}

	if q.Get("order") == orderDescending {
		reverseSlice(presets)
	}

	return c.JSON(http.StatusOK, presetsListOutput{Presets: limitSlice(presets, parseMaxResults(q.Get("maxResults")))})
}

func (h *Handler) handleDeletePreset(c *echo.Context, name string) error {
	if err := h.Backend.DeletePreset(name); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

type updatePresetInput struct {
	Settings    map[string]any `json:"settings,omitempty"`
	Description string         `json:"description,omitempty"`
	Category    string         `json:"category,omitempty"`
}

func (h *Handler) handleUpdatePreset(c *echo.Context, name string, body []byte) error {
	var in updatePresetInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	p, err := h.Backend.UpdatePreset(name, in.Description, in.Category, in.Settings)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, presetWrapper{Preset: p})
}
