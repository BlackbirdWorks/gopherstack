package directoryservice

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleShareDirectory(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		ShareMethod string `json:"ShareMethod"`
		ShareNotes  string `json:"ShareNotes"`
		ShareTarget struct {
			ID   string `json:"Id"`
			Type string `json:"Type"`
		} `json:"ShareTarget"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "DirectoryId is required"))
	}

	shareMethod := req.ShareMethod
	if shareMethod == "" {
		shareMethod = "HANDSHAKE"
	}

	sharedDirID, shareErr := h.Backend.ShareDirectory(
		h.contextWithRegion(c),
		req.DirectoryID,
		shareMethod,
		req.ShareNotes,
		req.ShareTarget.ID,
	)
	if shareErr != nil {
		return h.mapError(c, shareErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"SharedDirectoryId": sharedDirID}) //nolint:goconst // existing issue.
}

func (h *Handler) handleUnshareDirectory(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID   string `json:"DirectoryId"`
		UnshareTarget struct {
			ID   string `json:"Id"`
			Type string `json:"Type"`
		} `json:"UnshareTarget"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "DirectoryId is required"))
	}

	sharedDirID, unshareErr := h.Backend.UnshareDirectory(h.contextWithRegion(c), req.DirectoryID, req.UnshareTarget.ID)
	if unshareErr != nil {
		return h.mapError(c, unshareErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"SharedDirectoryId": sharedDirID})
}

func (h *Handler) handleAcceptSharedDirectory(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		SharedDirectoryID string `json:"SharedDirectoryId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.SharedDirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "SharedDirectoryId is required"))
	}

	info, acceptErr := h.Backend.AcceptSharedDirectory(h.contextWithRegion(c), req.SharedDirectoryID)
	if acceptErr != nil {
		return h.mapError(c, acceptErr)
	}

	// AcceptSharedDirectoryOutput.SharedDirectory is a full types.SharedDirectory,
	// not just the ID (confirmed against api_op_AcceptSharedDirectory.go);
	// every other field silently decoded to nil on a real client before this
	// fix. DescribeSharedDirectories's sibling response already emitted the
	// full shape correctly -- only this op was broken.
	return c.JSON(http.StatusOK, map[string]any{
		"SharedDirectory": map[string]any{
			"SharedDirectoryId":    info.SharedDirectoryID,
			"OwnerDirectoryId":     info.OwnerDirectoryID,
			"OwnerAccountId":       info.OwnerAccountID,
			"SharedAccountId":      info.SharedAccountID,
			"ShareMethod":          info.ShareMethod,
			"ShareStatus":          info.ShareStatus,
			"ShareNotes":           info.ShareNotes,
			keyCreatedDateTime:     awstime.Epoch(info.CreatedDateTime),
			keyLastUpdatedDateTime: awstime.Epoch(info.LastUpdatedDateTime),
		},
	})
}

func (h *Handler) handleRejectSharedDirectory(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		SharedDirectoryID string `json:"SharedDirectoryId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.SharedDirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "SharedDirectoryId is required"))
	}

	id, rejectErr := h.Backend.RejectSharedDirectory(h.contextWithRegion(c), req.SharedDirectoryID)
	if rejectErr != nil {
		return h.mapError(c, rejectErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"SharedDirectoryId": id})
}

func (h *Handler) handleDescribeSharedDirectories(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		OwnerDirectoryID   string   `json:"OwnerDirectoryId"`
		NextToken          string   `json:"NextToken"`
		SharedDirectoryIDs []string `json:"SharedDirectoryIds"`
		Limit              int32    `json:"Limit"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	if req.OwnerDirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "OwnerDirectoryId is required"))
	}

	dirs, nextToken, descErr := h.Backend.DescribeSharedDirectories(
		h.contextWithRegion(c),
		req.OwnerDirectoryID, req.SharedDirectoryIDs, req.Limit, req.NextToken,
	)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	dirList := make([]map[string]any, 0, len(dirs))
	for _, d := range dirs {
		dirList = append(dirList, map[string]any{
			"SharedDirectoryId":    d.SharedDirectoryID,
			"OwnerDirectoryId":     d.OwnerDirectoryID,
			"OwnerAccountId":       d.OwnerAccountID,
			"SharedAccountId":      d.SharedAccountID,
			"ShareMethod":          d.ShareMethod,
			"ShareStatus":          d.ShareStatus,
			"ShareNotes":           d.ShareNotes,
			keyCreatedDateTime:     awstime.Epoch(d.CreatedDateTime),
			keyLastUpdatedDateTime: awstime.Epoch(d.LastUpdatedDateTime),
		})
	}

	resp := map[string]any{"SharedDirectories": dirList}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}
