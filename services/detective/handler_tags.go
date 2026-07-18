package detective

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleTagResource(c *echo.Context) error {
	resourceARN, ok := extractTagARN(c.Request().URL.Path)
	if !ok {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid resource ARN"))
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Tags map[string]string `json:"Tags"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if tagErr := h.Backend.TagResource(resourceARN, req.Tags); tagErr != nil {
		return h.mapError(c, tagErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleUntagResource(c *echo.Context) error {
	resourceARN, ok := extractTagARN(c.Request().URL.Path)
	if !ok {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid resource ARN"))
	}

	tagKeys := c.Request().URL.Query()["tagKeys"]

	if untagErr := h.Backend.UntagResource(resourceARN, tagKeys); untagErr != nil {
		return h.mapError(c, untagErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListTagsForResource(c *echo.Context) error {
	resourceARN, ok := extractTagARN(c.Request().URL.Path)
	if !ok {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid resource ARN"))
	}

	tags, listErr := h.Backend.ListTagsForResource(resourceARN)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"Tags": tags,
	})
}

func extractTagARN(path string) (string, bool) {
	arn, ok := strings.CutPrefix(path, pathTagsPrefix)

	return arn, ok && arn != ""
}
