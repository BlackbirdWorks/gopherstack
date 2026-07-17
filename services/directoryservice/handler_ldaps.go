package directoryservice

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleEnableLDAPS(c *echo.Context) error { //nolint:dupl // existing issue.
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

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	ldapsType := req.Type
	if ldapsType == "" {
		ldapsType = "Client"
	}

	if enableErr := h.Backend.EnableLDAPS(h.contextWithRegion(c), req.DirectoryID, ldapsType); enableErr != nil {
		return h.mapError(c, enableErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDisableLDAPS(c *echo.Context) error { //nolint:dupl // existing issue.
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

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	ldapsType := req.Type
	if ldapsType == "" {
		ldapsType = "Client"
	}

	if disableErr := h.Backend.DisableLDAPS(h.contextWithRegion(c), req.DirectoryID, ldapsType); disableErr != nil {
		return h.mapError(c, disableErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDescribeLDAPSSettings(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		Type        string `json:"Type"`
		NextToken   string `json:"NextToken"`
		Limit       int32  `json:"Limit"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	settings, nextToken, descErr := h.Backend.DescribeLDAPSSettings(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.Type,
		req.Limit,
		req.NextToken,
	)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	settingList := make([]map[string]any, 0, len(settings))
	for _, s := range settings {
		settingList = append(settingList, map[string]any{
			"LDAPSType":                 s.LDAPSType,
			"CertificateId":             s.CertificateID, //nolint:goconst // existing issue.
			"LDAPSStatus":               s.State,
			"LastUpdatedDateTime":       awstime.Epoch(s.LastUpdatedDateTime), //nolint:goconst // existing issue.
			"CertificateExpiryDateTime": awstime.Epoch(s.CertificateExpiryDateTime),
		})
	}

	resp := map[string]any{"LDAPSSettingsInfo": settingList}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}
