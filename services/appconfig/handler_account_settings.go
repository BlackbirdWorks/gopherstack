package appconfig

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleGetAccountSettings(c *echo.Context) error {
	settings, err := h.Backend.GetAccountSettings()
	if err != nil {
		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.JSON(http.StatusOK, settings)
}

func (h *Handler) handleUpdateAccountSettings(c *echo.Context) error {
	var req struct {
		DeletionProtection *DeletionProtectionSettings `json:"DeletionProtection"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: errInvalidRequestBody},
		)
	}

	settings, err := h.Backend.UpdateAccountSettings(req.DeletionProtection)
	if err != nil {
		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.JSON(http.StatusOK, settings)
}
