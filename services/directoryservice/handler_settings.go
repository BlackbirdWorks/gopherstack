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
		entry := map[string]any{
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
		}
		if len(s.RegionStatuses) > 0 {
			entry["RequestDetailedStatus"] = s.RegionStatuses
		}
		if !s.LastRequestedTime.IsZero() {
			entry["LastRequestedDateTime"] = awstime.Epoch(s.LastRequestedTime)
		}
		settingList = append(settingList, entry)
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
		OSUpdateSettings *struct {
			OSVersion string `json:"OSVersion"`
		} `json:"OSUpdateSettings"`
		NetworkUpdateSettings *struct {
			NetworkType      string   `json:"NetworkType"`
			CustomerDNSIPsV6 []string `json:"CustomerDnsIpsV6"`
		} `json:"NetworkUpdateSettings"`
		DirectorySizeUpdateSettings *struct {
			DirectorySize string `json:"DirectorySize"`
		} `json:"DirectorySizeUpdateSettings"`
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

	update := DirectorySetupUpdate{
		UpdateType:                 req.UpdateType,
		CreateSnapshotBeforeUpdate: req.CreateSnapshotBeforeUpdate,
	}

	if req.OSUpdateSettings != nil {
		update.OSVersion = req.OSUpdateSettings.OSVersion
	}

	if req.NetworkUpdateSettings != nil {
		update.NetworkType = req.NetworkUpdateSettings.NetworkType
		update.CustomerDNSIPsV6 = req.NetworkUpdateSettings.CustomerDNSIPsV6
	}

	if req.DirectorySizeUpdateSettings != nil {
		update.DirectorySize = req.DirectorySizeUpdateSettings.DirectorySize
	}

	if msg := validateSetupUpdate(update); msg != "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", msg))
	}

	if updateErr := h.Backend.UpdateDirectorySetup(
		h.contextWithRegion(c), req.DirectoryID, update,
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
		RegionName  string `json:"RegionName"`
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
		req.RegionName,
		req.NextToken,
	)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	entryList := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		item := map[string]any{
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
		}

		// NewValue/PreviousValue are *types.UpdateValue{OSUpdateSettings}, never flat strings.
		if e.NewValue != "" {
			item["NewValue"] = map[string]any{"OSUpdateSettings": map[string]any{"OSVersion": e.NewValue}}
		}

		if e.PreviousValue != "" {
			item["PreviousValue"] = map[string]any{"OSUpdateSettings": map[string]any{"OSVersion": e.PreviousValue}}
		}

		entryList = append(entryList, item)
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

func validateSetupUpdate(u DirectorySetupUpdate) string {
	if u.OSVersion != "" && !validEnum(u.OSVersion, string(OSVersionVersion2012), string(OSVersionVersion2019)) {
		return "invalid OSVersion"
	}

	if u.NetworkType != "" && !validNetworkType(NetworkType(u.NetworkType)) {
		return "invalid NetworkType"
	}

	if u.DirectorySize != "" && !validEnum(u.DirectorySize, string(DirectorySizeSmall), string(DirectorySizeLarge)) {
		return "invalid DirectorySize"
	}

	return ""
}
