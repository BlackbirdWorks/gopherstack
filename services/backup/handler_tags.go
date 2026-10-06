package backup

import (
	"encoding/json"
	"maps"
	"net/http"
	"slices"

	"github.com/labstack/echo/v5"
)

type tagResourceBody struct {
	Tags map[string]string `json:"Tags"`
}

func (h *Handler) handleTagResource(c *echo.Context, resourceArn string, body []byte) error {
	var in tagResourceBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterValueException", "invalid request body"))
	}

	if in.Tags == nil {
		in.Tags = make(map[string]string)
	}

	if err := h.Backend.TagResource(resourceArn, in.Tags); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListTags(c *echo.Context, resourceArn string) error {
	t, err := h.Backend.ListTags(resourceArn)
	if err != nil {
		return h.handleError(c, err)
	}

	keys := slices.Sorted(maps.Keys(t))
	keys, next := pageQuery(c.Request().URL.Query(), keys, func(k string) string { return k })

	pageTags := make(map[string]string, len(keys))
	for _, k := range keys {
		pageTags[k] = t[k]
	}

	return c.JSON(http.StatusOK, withNextToken(map[string]any{"Tags": pageTags}, next))
}

type untagResourceBody struct {
	TagKeyList []string `json:"TagKeyList"`
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceArn string, body []byte) error {
	if resourceArn == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("MissingParameterValueException", "ResourceArn is required"),
		)
	}

	var in untagResourceBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("InvalidParameterValueException", "invalid request body"),
			)
		}
	}

	if err := h.Backend.UntagResource(resourceArn, in.TagKeyList); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- New operation handlers ---
