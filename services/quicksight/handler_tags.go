package quicksight

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func isTagOp(op string) bool {
	switch op {
	case opTagResource, opUntagResource, opListTagsForResource:
		return true
	}

	return false
}

func (h *Handler) dispatchTag(c *echo.Context, op string) error {
	switch op {
	case opTagResource:
		return h.handleTagResource(c)
	case opUntagResource:
		return h.handleUntagResource(c)
	case opListTagsForResource:
		return h.handleListTagsForResource(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		fmt.Sprintf("operation %q not implemented", op),
	)
}

// ---- Tag handlers ----

func (h *Handler) handleTagResource(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	// /resources/{arnParts...}/tags
	arn := strings.Join(segs[1:len(segs)-1], "/")

	body, bodyErr := readBody(c)
	if bodyErr != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	tags := tagsFromBody(body)
	if err := h.Backend.TagResource(arn, tags); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: newReqID(),
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleUntagResource(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	arn := strings.Join(segs[1:len(segs)-1], "/")

	keys := c.Request().URL.Query()["keys"]

	if err := h.Backend.UntagResource(arn, keys); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: newReqID(),
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListTagsForResource(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	arn := strings.Join(segs[1:len(segs)-1], "/")

	tags, err := h.Backend.ListTagsForResource(arn)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(tags))
	for k, v := range tags {
		items = append(items, map[string]any{"Key": k, "Value": v})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: newReqID(),
		keyStatus:    http.StatusOK,
		"Tags":       items,
	})
}
