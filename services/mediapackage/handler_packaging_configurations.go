package mediapackage

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- packaging configuration handlers ---

func (h *Handler) handleCreatePackagingConfiguration(c *echo.Context, body map[string]any) error {
	id, _ := body["id"].(string)
	if id == "" {
		return h.jsonError(c, http.StatusUnprocessableEntity, ErrInvalidParameter)
	}
	groupID, _ := body["packagingGroupId"].(string)
	description, _ := body["description"].(string)
	tags := extractTags(body)

	pc, err := h.Backend.CreatePackagingConfiguration(id, groupID, description, tags)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, toPackagingConfigOutput(pc))
}

func (h *Handler) handleDescribePackagingConfiguration(c *echo.Context, id string) error {
	pc, err := h.Backend.DescribePackagingConfiguration(id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, toPackagingConfigOutput(pc))
}

func (h *Handler) handleDeletePackagingConfiguration(c *echo.Context, id string) error {
	if err := h.Backend.DeletePackagingConfiguration(id); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusAccepted, map[string]any{})
}

func (h *Handler) handleListPackagingConfigurations(c *echo.Context) error {
	items, nextToken, err := h.Backend.ListPackagingConfigurations(0, "")
	if err != nil {
		return h.mapError(c, err)
	}

	out := make([]map[string]any, 0, len(items))
	for _, pc := range items {
		out = append(out, toPackagingConfigOutput(pc))
	}

	resp := map[string]any{"packagingConfigurations": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func toPackagingConfigOutput(pc *PackagingConfiguration) map[string]any {
	return map[string]any{
		"id":               pc.ID,
		"arn":              pc.ARN,
		"packagingGroupId": pc.PackagingGroupID,
		"description":      pc.Description,
		"createdAt":        pc.CreatedAt,
		"tags":             pc.Tags,
	}
}
