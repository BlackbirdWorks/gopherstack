package iot

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func resolveOTAUpdateOps(path, method string) string {
	switch {
	case path == "/otaUpdates" && method == http.MethodGet:

		return opListOTAUpdates
	case strings.HasPrefix(path, "/otaUpdates/") && method == http.MethodPost:

		return opCreateOTAUpdate
	case strings.HasPrefix(path, "/otaUpdates/") && method == http.MethodGet:

		return opGetOTAUpdate
	case strings.HasPrefix(path, "/otaUpdates/") && method == http.MethodDelete:

		return opDeleteOTAUpdate
	}

	return unknownOperation
}

func (h *Handler) handleCreateOTAUpdate(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/otaUpdates/")
	var req struct {
		Description string    `json:"description"`
		RoleARN     string    `json:"roleArn"`
		Targets     []string  `json:"targets"`
		Files       []any     `json:"otaUpdateFiles"`
		Tags        []tags.KV `json:"tags"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	o, err := h.Backend.CreateOTAUpdate(
		id, req.Description, req.RoleARN, req.Targets, req.Files, tags.MapFromKV(req.Tags),
	)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"otaUpdateId":     o.OTAUpdateID,
		"otaUpdateArn":    o.OTAUpdateARN,
		"otaUpdateStatus": o.Status,
	})
}

func (h *Handler) handleGetOTAUpdate(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/otaUpdates/")
	o, err := h.Backend.GetOTAUpdate(id)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"otaUpdateInfo": o})
}

func (h *Handler) handleDeleteOTAUpdate(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/otaUpdates/")
	if err := h.Backend.DeleteOTAUpdate(id); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListOTAUpdates(c *echo.Context) error {
	items := h.Backend.ListOTAUpdates()
	summaries := make([]map[string]any, len(items))
	for i, o := range items {
		summaries[i] = map[string]any{
			"otaUpdateId":  o.OTAUpdateID,
			"otaUpdateArn": o.OTAUpdateARN,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{"otaUpdates": summaries})
}

func (h *Handler) dispatchOTAUpdateOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opCreateOTAUpdate:
		return true, h.handleCreateOTAUpdate(c)
	case opGetOTAUpdate:
		return true, h.handleGetOTAUpdate(c)
	case opDeleteOTAUpdate:
		return true, h.handleDeleteOTAUpdate(c)
	case opListOTAUpdates:
		return true, h.handleListOTAUpdates(c)
	}

	return false, nil
}
