package directoryservice

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// --- Directory Data Access ---

func (h *Handler) handleEnableDirectoryDataAccess(c *echo.Context) error {
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

	if enableErr := h.Backend.EnableDirectoryDataAccess(h.contextWithRegion(c), req.DirectoryID); enableErr != nil {
		return h.mapError(c, enableErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDisableDirectoryDataAccess(c *echo.Context) error {
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

	if disableErr := h.Backend.DisableDirectoryDataAccess(h.contextWithRegion(c), req.DirectoryID); disableErr != nil {
		return h.mapError(c, disableErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDescribeDirectoryDataAccess(c *echo.Context) error {
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

	status, descErr := h.Backend.DescribeDirectoryDataAccess(h.contextWithRegion(c), req.DirectoryID)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	dataAccessStatus := "Disabled" //nolint:goconst // existing issue.
	if status.Enabled {
		dataAccessStatus = "Enabled" //nolint:goconst // existing issue.
	}

	return c.JSON(http.StatusOK, map[string]any{
		"DirectoryDataAccessStatus": dataAccessStatus,
	})
}

// --- Settings ---

func (h *Handler) handleUpdateSettings(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		Settings    []struct {
			Name  string `json:"Name"`
			Value string `json:"Value"`
		} `json:"Settings"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "DirectoryId is required"))
	}

	settings := make([]DirectorySetting, 0, len(req.Settings))
	for _, s := range req.Settings {
		settings = append(settings, DirectorySetting{Name: s.Name, Value: s.Value})
	}

	directoryID, updateErr := h.Backend.UpdateSettings(h.contextWithRegion(c), req.DirectoryID, settings)
	if updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyDirectoryID: directoryID})
}

func (h *Handler) handleDescribeSettings(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		Status      string `json:"Status"`
		NextToken   string `json:"NextToken"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "DirectoryId is required"))
	}

	settings, nextToken, descErr := h.Backend.DescribeSettings(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.Status,
		req.NextToken,
	)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	settingList := make([]map[string]any, 0, len(settings))
	for _, s := range settings {
		settingList = append(settingList, map[string]any{
			"Name":                s.Name, //nolint:goconst // existing issue.
			"AllowedValues":       s.AllowedValues,
			"AppliedValue":        s.AppliedValue,
			"RequestedValue":      s.RequestedValue,
			keyStatus:             s.Status,
			"LastUpdatedDateTime": awstime.Epoch(s.LastUpdatedDateTime), //nolint:goconst // existing issue.
		})
	}

	resp := map[string]any{
		keyDirectoryID:   req.DirectoryID,
		"SettingEntries": settingList,
	}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateDirectorySetup(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID                string `json:"DirectoryId"`
		UpdateType                 string `json:"UpdateType"`
		CreateSnapshotBeforeUpdate bool   `json:"CreateSnapshotBeforeUpdate"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" || req.UpdateType == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterException", "DirectoryId and UpdateType are required"),
		)
	}
	if !validEnum(req.UpdateType, string(UpdateTypeOS), string(UpdateTypeNetwork), string(UpdateTypeSize)) {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid UpdateType"))
	}

	if updateErr := h.Backend.UpdateDirectorySetup(
		h.contextWithRegion(c),
		req.DirectoryID, req.UpdateType, req.CreateSnapshotBeforeUpdate,
	); updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDescribeUpdateDirectory(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		UpdateType  string `json:"UpdateType"`
		NextToken   string `json:"NextToken"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "DirectoryId is required"))
	}

	entries, nextToken, descErr := h.Backend.DescribeUpdateDirectory(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.UpdateType,
		req.NextToken,
	)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	entryList := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		entryList = append(entryList, map[string]any{
			"UpdateType":          e.UpdateType,
			keyStatus:             e.Status,
			"NewValue":            e.NewValue,
			"PreviousValue":       e.PreviousValue,
			"InitiatedBy":         e.InitiatedBy,
			keyRegion:             e.Region,
			keyStartTime:          awstime.Epoch(e.StartTime),
			"LastUpdatedDateTime": awstime.Epoch(e.LastUpdatedDateTime),
		})
	}

	resp := map[string]any{"UpdateDirectoryInfo": entryList}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}
