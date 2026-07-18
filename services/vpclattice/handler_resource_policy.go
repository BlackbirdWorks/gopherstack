package vpclattice

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// ------- Resource Policy handlers -------

func (h *Handler) handlePutResourcePolicy(
	c *echo.Context,
	resourceArn string,
	body map[string]any,
) error {
	policy, _ := body[keyPolicy].(string)

	if err := h.Backend.PutResourcePolicy(resourceArn, policy); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetResourcePolicy(c *echo.Context, resourceArn string) error {
	policy, err := h.Backend.GetResourcePolicy(resourceArn)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyPolicy: policy})
}

func (h *Handler) handleDeleteResourcePolicy(c *echo.Context, resourceArn string) error {
	if err := h.Backend.DeleteResourcePolicy(resourceArn); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
