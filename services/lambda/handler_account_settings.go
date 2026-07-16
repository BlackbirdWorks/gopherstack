package lambda

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- Account settings handler ---

// handleGetAccountSettings handles GET /2016-08-19/account-settings.
func (h *Handler) handleGetAccountSettings(c *echo.Context) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	return c.JSON(http.StatusOK, lambdaBk.GetAccountSettings())
}
