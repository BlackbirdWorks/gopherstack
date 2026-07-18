package vpclattice

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// ------- Auth Policy handlers -------

func (h *Handler) handlePutAuthPolicy(
	c *echo.Context,
	resourceID string,
	body map[string]any,
) error {
	policy, _ := body[keyPolicy].(string)

	ap, err := h.Backend.PutAuthPolicy(resourceID, policy)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyPolicy: ap.Policy,
		"state":   ap.State,
	})
}

func (h *Handler) handleGetAuthPolicy(c *echo.Context, resourceID string) error {
	ap, err := h.Backend.GetAuthPolicy(resourceID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyPolicy: ap.Policy,
		"state":   ap.State,
	})
}

func (h *Handler) handleDeleteAuthPolicy(c *echo.Context, resourceID string) error {
	if err := h.Backend.DeleteAuthPolicy(resourceID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
