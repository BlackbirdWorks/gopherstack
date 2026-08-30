package directoryservice

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleStartSchemaExtension(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID                         string `json:"DirectoryId"`
		Description                         string `json:"Description"`
		LdifContent                         string `json:"LdifContent"`
		CreateSnapshotBeforeSchemaExtension bool   `json:"CreateSnapshotBeforeSchemaExtension"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "DirectoryId is required"))
	}

	id, startErr := h.Backend.StartSchemaExtension(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.Description,
		req.LdifContent,
		req.CreateSnapshotBeforeSchemaExtension,
	)
	if startErr != nil {
		return h.mapError(c, startErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"SchemaExtensionId": id,
	})
}

func (h *Handler) handleCancelSchemaExtension(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID       string `json:"DirectoryId"`
		SchemaExtensionID string `json:"SchemaExtensionId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" || req.SchemaExtensionID == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterException", "DirectoryId and SchemaExtensionId are required"),
		)
	}

	cancelErr := h.Backend.CancelSchemaExtension(h.contextWithRegion(c), req.DirectoryID, req.SchemaExtensionID)
	if cancelErr != nil {
		return h.mapError(c, cancelErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListSchemaExtensions(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		NextToken   string `json:"NextToken"`
		Limit       int32  `json:"Limit"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "DirectoryId is required"))
	}

	exts, nextToken, listErr := h.Backend.ListSchemaExtensions(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.Limit,
		req.NextToken,
	)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	extList := make([]map[string]any, 0, len(exts))
	for _, e := range exts {
		extList = append(extList, map[string]any{
			keyDirectoryID:          e.DirectoryID,
			"SchemaExtensionId":     e.ExtensionID,
			"Description":           e.Description, //nolint:goconst // existing issue.
			"SchemaExtensionStatus": e.Status,
			"StartDateTime":         awstime.Epoch(e.StartTime),
			"EndDateTime":           awstime.Epoch(e.EndTime),
		})
	}

	resp := map[string]any{"SchemaExtensionsInfo": extList}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}
