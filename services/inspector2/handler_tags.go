package inspector2

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// handleListTagsForResource handles GET /tags/{resourceArn}.
func (h *Handler) handleListTagsForResource(c *echo.Context) error {
	resourceARN := extractResourceARN(c.Request().URL.Path)
	if resourceARN == "" {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "resourceArn is required"),
		)
	}

	tags, listErr := h.Backend.ListTagsForResource(resourceARN)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	if tags == nil {
		tags = map[string]string{}
	}

	return c.JSON(http.StatusOK, map[string]any{"tags": tags})
}

// handleTagResource handles POST /tags/{resourceArn}.
func (h *Handler) handleTagResource(c *echo.Context) error {
	resourceARN := extractResourceARN(c.Request().URL.Path)
	if resourceARN == "" {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "resourceArn is required"),
		)
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Tags map[string]string `json:"tags"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if tagErr := h.Backend.TagResource(resourceARN, req.Tags); tagErr != nil {
		return h.mapError(c, tagErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// handleUntagResource handles DELETE /tags/{resourceArn}.
func (h *Handler) handleUntagResource(c *echo.Context) error {
	resourceARN := extractResourceARN(c.Request().URL.Path)
	if resourceARN == "" {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "resourceArn is required"),
		)
	}

	tagKeys := c.Request().URL.Query()["tagKeys"]

	if untagErr := h.Backend.UntagResource(resourceARN, tagKeys); untagErr != nil {
		return h.mapError(c, untagErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}
