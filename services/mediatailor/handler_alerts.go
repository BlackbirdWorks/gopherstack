package mediatailor

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- Alerts handler ---

func (h *Handler) handleListAlerts(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{keyItems: []map[string]any{}})
}
