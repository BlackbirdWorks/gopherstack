package appsync

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// handleTagsByARN handles /v1/tags/{resourceArn} — the real AWS SDK path for
// TagResource/UntagResource/ListTagsForResource (tag ops are NOT nested under
// /v1/apis/{apiId}/tags on the wire). AppSync tags GraphqlApi (v1) and Api (v2)
// resources, both identified by an "arn:...:appsync:...:apis/{apiId}" ARN.
func (h *Handler) handleTagsByARN(ctx context.Context, c *echo.Context, segs []string) error {
	if len(segs) < pathSegsAPIID {
		return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
	}

	apiID, ok := apiIDFromResourceARN(segs[2:])
	if !ok {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid resourceArn"))
	}

	switch c.Request().Method {
	case http.MethodPost:
		return h.tagResource(ctx, c, apiID)
	case http.MethodGet:
		return h.listTagsForResource(ctx, c, apiID)
	case http.MethodDelete:
		return h.untagResource(ctx, c, apiID)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// apiIDFromResourceARN extracts the "{apiId}" resource identifier from an AppSync
// resource ARN's path segments. The AWS SDK percent-encodes "/" as %2F in the
// resourceArn URI label, but net/http decodes the request path before routing reaches
// here, so the ARN's internal slashes (e.g. "apis/{apiId}") have already been split
// into separate segments by splitPath and must be rejoined.
func apiIDFromResourceARN(arnSegs []string) (string, bool) {
	full := strings.Join(arnSegs, "/")

	const apisResourcePrefix = "apis/"

	idx := strings.LastIndex(full, apisResourcePrefix)
	if idx == -1 {
		return "", false
	}

	id := full[idx+len(apisResourcePrefix):]
	if id == "" {
		return "", false
	}

	// Defend against a deeper resource path (e.g. "apis/{id}/apikeys/{key}"): only
	// the api ID itself is ever a valid taggable resource for this backend.
	if slash := strings.IndexByte(id, '/'); slash != -1 {
		id = id[:slash]
	}

	return id, true
}

// handleTags handles /v1/apis/{apiId}/tags.
func (h *Handler) handleTags(ctx context.Context, c *echo.Context, apiID string) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.tagResource(ctx, c, apiID)
	case http.MethodGet:
		return h.listTagsForResource(ctx, c, apiID)
	case http.MethodDelete:
		return h.untagResource(ctx, c, apiID)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// tagResource handles POST /v1/apis/{apiId}/tags.
func (h *Handler) tagResource(ctx context.Context, c *echo.Context, apiID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		Tags map[string]string `json:"tags"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if tagErr := h.Backend.TagResource(apiID, input.Tags); tagErr != nil {
		return h.handleError(ctx, c, opTagResource, tagErr)
	}

	return c.NoContent(http.StatusNoContent)
}

// untagResource handles DELETE /v1/apis/{apiId}/tags.
func (h *Handler) untagResource(ctx context.Context, c *echo.Context, apiID string) error {
	tagKeys := c.Request().URL.Query()["tagKeys"]
	if len(tagKeys) == 0 {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("BadRequestException", "tagKeys query parameter is required"),
		)
	}

	if tagErr := h.Backend.UntagResource(apiID, tagKeys); tagErr != nil {
		return h.handleError(ctx, c, opUntagResource, tagErr)
	}

	return c.NoContent(http.StatusNoContent)
}

// listTagsForResource handles GET /v1/apis/{apiId}/tags.
func (h *Handler) listTagsForResource(ctx context.Context, c *echo.Context, apiID string) error {
	tagMap, err := h.Backend.ListTagsForResource(apiID)
	if err != nil {
		return h.handleError(ctx, c, opListTagsForResource, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"tags": tagMap})
}
