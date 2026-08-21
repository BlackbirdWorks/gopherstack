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

	// DescribeDirectoryDataAccessOutput's real member is "DataAccessStatus"
	// (directoryservice@v1.41.4 deserializers.go's
	// awsAwsjson11_deserializeOpDocumentDescribeDirectoryDataAccessOutput),
	// not "DirectoryDataAccessStatus".
	return c.JSON(http.StatusOK, map[string]any{
		"DataAccessStatus": dataAccessStatus,
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
			"Name":           s.Name, //nolint:goconst // existing issue.
			"AllowedValues":  s.AllowedValues,
			"AppliedValue":   s.AppliedValue,
			"RequestedValue": s.RequestedValue,
			// Real types.SettingEntry has no "Status" member -- the request-side
			// filter field DescribeSettingsInput.Status shares that name, and
			// it was copied onto the response by mistake. The real response
			// member is "RequestStatus" (confirmed against
			// types.SettingEntry); a real client's RequestStatus field
			// silently decoded to its zero value on every call.
			"RequestStatus":       s.Status,
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
			// UpdateType is not a real types.UpdateInfoEntry member -- harmless,
			// informational (the request-side filter's own value), left in
			// place per this campaign's precedent for extra fields a real
			// client simply ignores rather than removing something that buys
			// nothing testable.
			"UpdateType":          e.UpdateType,
			keyStatus:             e.Status,
			"InitiatedBy":         e.InitiatedBy,
			keyRegion:             e.Region,
			keyStartTime:          awstime.Epoch(e.StartTime),
			"LastUpdatedDateTime": awstime.Epoch(e.LastUpdatedDateTime),
			// NewValue/PreviousValue (types.UpdateInfoEntry) are real members,
			// but this backend never populates real content (always the Go
			// zero value) -- omitted rather than emitted as a flat "" string:
			// the real member type is *types.UpdateValue{OSUpdateSettings},
			// a nested struct, so a flat string would hard-fail every real
			// client's decode.
		})
	}

	// Wrapper key is "UpdateActivities" (confirmed against
	// DescribeUpdateDirectoryOutput); the fabricated "UpdateDirectoryInfo"
	// this handler used to emit meant a real typed client's
	// resp.UpdateActivities field silently decoded to nil on every call.
	resp := map[string]any{"UpdateActivities": entryList}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}
