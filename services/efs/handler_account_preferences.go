package efs

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleDescribeAccountPreferences(c *echo.Context) error {
	prefs := h.Backend.DescribeAccountPreferences()

	return c.JSON(http.StatusOK, map[string]any{
		"ResourceIdPreference": map[string]any{
			"ResourceIdType": prefs.ResourceIDType,
			"Resources":      []string{"FILE_SYSTEM", "MOUNT_TARGET"},
		},
	})
}

type putAccountPreferencesBody struct {
	ResourceIDPreference struct {
		ResourceIDType string `json:"ResourceIdType"`
	} `json:"ResourceIdPreference"`
}

func (h *Handler) handlePutAccountPreferences(c *echo.Context, body []byte) error {
	var in putAccountPreferencesBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	prefs, err := h.Backend.PutAccountPreferences(in.ResourceIDPreference.ResourceIDType)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ResourceIdPreference": map[string]any{
			"ResourceIdType": prefs.ResourceIDType,
			"Resources":      []string{"FILE_SYSTEM", "MOUNT_TARGET"},
		},
	})
}
