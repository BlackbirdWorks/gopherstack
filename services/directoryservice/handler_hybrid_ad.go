package directoryservice

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleCreateHybridAD(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		Name        string `json:"Name"`
		ShortName   string `json:"ShortName"`
		Description string `json:"Description"`
		Password    string `json:"Password"`
		Edition     string `json:"Edition"`
		Tags        []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.Name == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "Name is required"))
	}

	edition := DirectoryEdition(req.Edition)
	if edition == "" {
		edition = DirectoryEditionEnterprise
	}

	tags := reqTagsToTags(req.Tags)
	d, requestID, createErr := h.Backend.CreateHybridAD(
		h.contextWithRegion(c),
		req.Name,
		req.ShortName,
		req.Description,
		req.Password,
		edition,
		tags,
	)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyDirectoryID: d.DirectoryID,
		keyRequestID:   requestID,
	})
}

func (h *Handler) handleUpdateHybridAD(c *echo.Context) error {
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

	requestID, updateErr := h.Backend.UpdateHybridAD(h.contextWithRegion(c), req.DirectoryID)
	if updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyRequestID: requestID})
}

func (h *Handler) handleDescribeHybridADUpdate(c *echo.Context) error {
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

	updates, descErr := h.Backend.DescribeHybridADUpdate(h.contextWithRegion(c), req.DirectoryID)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	updateList := make([]map[string]any, 0, len(updates))
	for _, u := range updates {
		updateList = append(updateList, map[string]any{
			keyRequestID:   u.RequestID,
			keyDirectoryID: u.DirectoryID,
			keyStatus:      u.Status,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"HybridADUpdateInfo": updateList})
}
