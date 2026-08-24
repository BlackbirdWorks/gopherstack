package amplify

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// handleTags dispatches GET/POST/DELETE /tags/{resourceArn}.
func (h *Handler) handleTags(ctx context.Context, c *echo.Context) error {
	resourceARN := extractResourceARN(c.Request().URL.RawPath, c.Request().URL.Path)

	switch c.Request().Method {
	case http.MethodGet:
		return h.listTagsForResource(ctx, c, resourceARN)
	case http.MethodPost:
		return h.tagResource(ctx, c, resourceARN)
	case http.MethodDelete:
		return h.untagResource(ctx, c, resourceARN)
	default:
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// listTagsForResource handles GET /tags/{resourceArn}.
func (h *Handler) listTagsForResource(ctx context.Context, c *echo.Context, resourceARN string) error {
	tagMap, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return h.handleBackendError(ctx, c, opListTagsForResource, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"tags": tagMap})
}

// tagResource handles POST /tags/{resourceArn}.
func (h *Handler) tagResource(ctx context.Context, c *echo.Context, resourceARN string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return amplifyErrorJSON(c, http.StatusInternalServerError, err.Error())
	}

	var input struct {
		Tags map[string]string `json:"tags"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return amplifyErrorJSON(c, http.StatusBadRequest, "invalid request body")
	}

	if tagErr := h.Backend.TagResource(resourceARN, input.Tags); tagErr != nil {
		return h.handleBackendError(ctx, c, opTagResource, tagErr)
	}

	return c.NoContent(http.StatusOK)
}

// untagResource handles DELETE /tags/{resourceArn}?tagKeys=key1&tagKeys=key2.
func (h *Handler) untagResource(ctx context.Context, c *echo.Context, resourceARN string) error {
	tagKeys := c.Request().URL.Query()["tagKeys"]

	if untagErr := h.Backend.UntagResource(resourceARN, tagKeys); untagErr != nil {
		return h.handleBackendError(ctx, c, opUntagResource, untagErr)
	}

	return c.NoContent(http.StatusOK)
}

// parseTagsOperation maps the method for /tags/{resourceArn} to its operation name.
func parseTagsOperation(method string) string {
	switch method {
	case http.MethodGet:
		return opListTagsForResource
	case http.MethodPost:
		return opTagResource
	case http.MethodDelete:
		return opUntagResource
	default:
		return opUnknown
	}
}

// extractResourceARN extracts and URL-decodes the resource ARN from a /tags/{arn} path.
func extractResourceARN(rawPath, decodedPath string) string {
	if rawPath == "" {
		rawPath = decodedPath
	}

	const tagsPrefix = "/tags/"
	encoded := strings.TrimPrefix(rawPath, tagsPrefix)

	decoded, err := url.PathUnescape(encoded)
	if err != nil {
		return encoded
	}

	return decoded
}
