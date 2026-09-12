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

// putAccountPreferencesBody matches the real PutAccountPreferencesInput wire
// shape: ResourceIdType is a flat, top-level required field on the request
// (efs@v1.48.0 api_op_PutAccountPreferences.go) -- unlike the response, which
// nests it under ResourceIdPreference. A request-shaped ResourceIdPreference
// wrapper here was copied from the response and never matched what a real
// client actually sends, so ResourceIdType always decoded empty.
type putAccountPreferencesBody struct {
	ResourceIDType string `json:"ResourceIdType"`
}

func (h *Handler) handlePutAccountPreferences(c *echo.Context, body []byte) error {
	var in putAccountPreferencesBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	prefs, err := h.Backend.PutAccountPreferences(in.ResourceIDType)
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
