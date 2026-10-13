package mediatailor

import (
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleListAlerts(c *echo.Context) error {
	q := c.Request().URL.Query()
	if q.Get("resourceArn") == "" && q.Get("ResourceArn") == "" {
		return respondErr(c, fmt.Errorf("%w: resourceArn is required", ErrInvalidParameter))
	}

	return c.JSON(http.StatusOK, map[string]any{keyItems: []map[string]any{}})
}
