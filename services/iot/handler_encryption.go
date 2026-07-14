package iot

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleDescribeEncryptionConfiguration(c *echo.Context) error {
	return c.JSON(http.StatusOK, h.Backend.DescribeEncryptionConfiguration())
}

func (h *Handler) handleUpdateEncryptionConfiguration(c *echo.Context) error {
	var req UpdateEncryptionConfigurationInput
	if err := readBody(c, &req); err != nil {
		return err
	}

	if err := h.Backend.UpdateEncryptionConfiguration(&req); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{})
}
