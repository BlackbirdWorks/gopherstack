package appconfig

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceArn string) error {
	tags, err := h.Backend.ListTagsForResource(resourceArn)
	if err != nil {
		return internalServerErrorResponse(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"Tags": tags})
}

func (h *Handler) handleTagResource(c *echo.Context, resourceArn string) error {
	var req struct {
		Tags map[string]string `json:"Tags"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: errInvalidRequestBody},
		)
	}

	if err := h.Backend.TagResource(resourceArn, req.Tags); err != nil {
		return internalServerErrorResponse(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceArn string) error {
	keysToRemove := c.Request().URL.Query()["tagKeys"]

	if err := h.Backend.UntagResource(resourceArn, keysToRemove); err != nil {
		return internalServerErrorResponse(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
