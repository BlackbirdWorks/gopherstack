package directoryservice

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleEnableRadius(c *echo.Context) error { //nolint:dupl // existing issue.
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID    string `json:"DirectoryId"`
		RadiusSettings struct {
			AuthenticationProtocol string   `json:"AuthenticationProtocol"`
			DisplayLabel           string   `json:"DisplayLabel"`
			SharedSecret           string   `json:"SharedSecret"`
			RadiusServers          []string `json:"RadiusServers"`
			RadiusPort             int32    `json:"RadiusPort"`
			RadiusRetries          int32    `json:"RadiusRetries"`
			RadiusTimeout          int32    `json:"RadiusTimeout"`
			UseSameUsername        bool     `json:"UseSameUsername"`
		} `json:"RadiusSettings"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	settings := RadiusSettingsInput{
		AuthenticationProtocol: req.RadiusSettings.AuthenticationProtocol,
		DisplayLabel:           req.RadiusSettings.DisplayLabel,
		RadiusServers:          req.RadiusSettings.RadiusServers,
		SharedSecret:           req.RadiusSettings.SharedSecret,
		RadiusPort:             req.RadiusSettings.RadiusPort,
		RadiusRetries:          req.RadiusSettings.RadiusRetries,
		RadiusTimeout:          req.RadiusSettings.RadiusTimeout,
		UseSameUsername:        req.RadiusSettings.UseSameUsername,
	}

	if enableErr := h.Backend.EnableRadius(h.contextWithRegion(c), req.DirectoryID, settings); enableErr != nil {
		return h.mapError(c, enableErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDisableRadius(c *echo.Context) error {
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
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	if disableErr := h.Backend.DisableRadius(h.contextWithRegion(c), req.DirectoryID); disableErr != nil {
		return h.mapError(c, disableErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleUpdateRadius(c *echo.Context) error { //nolint:dupl // existing issue.
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID    string `json:"DirectoryId"`
		RadiusSettings struct {
			AuthenticationProtocol string   `json:"AuthenticationProtocol"`
			DisplayLabel           string   `json:"DisplayLabel"`
			SharedSecret           string   `json:"SharedSecret"`
			RadiusServers          []string `json:"RadiusServers"`
			RadiusPort             int32    `json:"RadiusPort"`
			RadiusRetries          int32    `json:"RadiusRetries"`
			RadiusTimeout          int32    `json:"RadiusTimeout"`
			UseSameUsername        bool     `json:"UseSameUsername"`
		} `json:"RadiusSettings"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	settings := RadiusSettingsInput{
		AuthenticationProtocol: req.RadiusSettings.AuthenticationProtocol,
		DisplayLabel:           req.RadiusSettings.DisplayLabel,
		RadiusServers:          req.RadiusSettings.RadiusServers,
		SharedSecret:           req.RadiusSettings.SharedSecret,
		RadiusPort:             req.RadiusSettings.RadiusPort,
		RadiusRetries:          req.RadiusSettings.RadiusRetries,
		RadiusTimeout:          req.RadiusSettings.RadiusTimeout,
		UseSameUsername:        req.RadiusSettings.UseSameUsername,
	}

	if updateErr := h.Backend.UpdateRadius(h.contextWithRegion(c), req.DirectoryID, settings); updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}
