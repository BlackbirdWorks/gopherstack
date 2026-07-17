package glacier

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleSetVaultNotifications(
	c *echo.Context,
	vaultName string,
	body []byte,
) error {
	var req vaultNotificationConfig
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			"invalid request body: "+err.Error(),
		)
	}

	if req.SNSTopic == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			"SNSTopic is required for SetVaultNotifications")
	}

	if len(req.Events) == 0 {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			"Events must not be empty for SetVaultNotifications")
	}

	if err := h.Backend.SetVaultNotifications(
		h.AccountID,
		h.DefaultRegion,
		vaultName,
		req.SNSTopic,
		req.Events,
	); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleGetVaultNotifications(c *echo.Context, vaultName string) error {
	snsTopic, events, err := h.Backend.GetVaultNotifications(
		h.AccountID,
		h.DefaultRegion,
		vaultName,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	if snsTopic == "" {
		return h.writeError(
			c,
			http.StatusNotFound,
			"ResourceNotFoundException",
			"vault notification configuration not found",
		)
	}

	return c.JSON(http.StatusOK, vaultNotificationConfig{
		SNSTopic: snsTopic,
		Events:   events,
	})
}

func (h *Handler) handleDeleteVaultNotifications(c *echo.Context, vaultName string) error {
	if err := h.Backend.DeleteVaultNotifications(h.AccountID, h.DefaultRegion, vaultName); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
