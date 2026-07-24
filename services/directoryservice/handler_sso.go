package directoryservice

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleEnableSso(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "DirectoryId is required"))
	}

	if ssoErr := h.Backend.EnableSso(h.contextWithRegion(c), req.DirectoryID); ssoErr != nil {
		return h.mapError(c, ssoErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDisableSso(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "DirectoryId is required"))
	}

	if ssoErr := h.Backend.DisableSso(h.contextWithRegion(c), req.DirectoryID); ssoErr != nil {
		return h.mapError(c, ssoErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}
