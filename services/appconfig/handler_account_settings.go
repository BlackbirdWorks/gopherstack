package appconfig

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleGetAccountSettings(c *echo.Context) error {
	settings, err := h.Backend.GetAccountSettings()
	if err != nil {
		return internalServerErrorResponse(c, err)
	}

	return c.JSON(http.StatusOK, settings)
}

func (h *Handler) handleUpdateAccountSettings(c *echo.Context) error {
	var req struct {
		DeletionProtection *DeletionProtectionSettings `json:"DeletionProtection"`
		VendedMetrics      *VendedMetricsSettings      `json:"VendedMetrics"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: errInvalidRequestBody},
		)
	}

	settings, err := h.Backend.UpdateAccountSettings(req.DeletionProtection, req.VendedMetrics)
	if err != nil {
		return internalServerErrorResponse(c, err)
	}

	return c.JSON(http.StatusOK, settings)
}
