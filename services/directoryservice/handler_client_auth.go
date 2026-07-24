package directoryservice

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleEnableClientAuthentication(c *echo.Context) error { //nolint:dupl // enable/disable pair.
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		Type        string `json:"Type"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" || req.Type == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "DirectoryId and Type are required"))
	}
	if !validEnum(req.Type,
		string(ClientAuthenticationTypeSmartCard), string(ClientAuthenticationTypeSmartCardOrPassword)) {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid Type"))
	}

	enableErr := h.Backend.EnableClientAuthentication(h.contextWithRegion(c), req.DirectoryID, req.Type)
	if enableErr != nil {
		return h.mapError(c, enableErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDisableClientAuthentication(c *echo.Context) error { //nolint:dupl // enable/disable pair.
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		Type        string `json:"Type"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" || req.Type == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "DirectoryId and Type are required"))
	}
	if !validEnum(req.Type,
		string(ClientAuthenticationTypeSmartCard), string(ClientAuthenticationTypeSmartCardOrPassword)) {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid Type"))
	}

	disableErr := h.Backend.DisableClientAuthentication(h.contextWithRegion(c), req.DirectoryID, req.Type)
	if disableErr != nil {
		return h.mapError(c, disableErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDescribeClientAuthenticationSettings(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		Type        string `json:"Type"`
		NextToken   string `json:"NextToken"`
		PageSize    int32  `json:"PageSize"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "DirectoryId is required"))
	}

	settings, nextToken, descErr := h.Backend.DescribeClientAuthenticationSettings(
		h.contextWithRegion(c),
		req.DirectoryID, req.Type, req.PageSize, req.NextToken,
	)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	settingList := make([]map[string]any, 0, len(settings))
	for _, s := range settings {
		settingList = append(settingList, map[string]any{
			"Type":                s.AuthType, //nolint:goconst // existing issue.
			keyStatus:             s.Status,
			"LastUpdatedDateTime": awstime.Epoch(s.LastUpdatedDateTime), //nolint:goconst // existing issue.
		})
	}

	resp := map[string]any{"ClientAuthenticationSettingsInfo": settingList}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}
