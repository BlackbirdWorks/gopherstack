package inspector2

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	opGetEncryptionKey    = "GetEncryptionKey"
	opResetEncryptionKey  = "ResetEncryptionKey"
	opUpdateEncryptionKey = "UpdateEncryptionKey"

	pathEncryptionKeyGet    = "/encryptionkey/get"
	pathEncryptionKeyReset  = "/encryptionkey/reset"
	pathEncryptionKeyUpdate = "/encryptionkey/update"
)

func (h *Handler) handleGetEncryptionKey(c *echo.Context) error {
	resourceType := c.Request().URL.Query().Get("resourceType")
	scanType := c.Request().URL.Query().Get("scanType")

	key, err := h.Backend.GetEncryptionKey(resourceType, scanType)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"kmsKeyId":     key.KmsKeyID,
		"resourceType": key.ResourceType,
		"scanType":     key.ScanType,
	})
}

func (h *Handler) handleResetEncryptionKey(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ResourceType string `json:"resourceType"`
		ScanType     string `json:"scanType"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	if resetErr := h.Backend.ResetEncryptionKey(req.ResourceType, req.ScanType); resetErr != nil {
		return h.mapError(c, resetErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleUpdateEncryptionKey(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		KmsKeyID     string `json:"kmsKeyId"`
		ResourceType string `json:"resourceType"`
		ScanType     string `json:"scanType"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if updateErr := h.Backend.UpdateEncryptionKey(req.KmsKeyID, req.ResourceType, req.ScanType); updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}
