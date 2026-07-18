package cleanrooms

import (
	"context"
	"encoding/json"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleListTagsForResource(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		ResourceArn string `json:"resourceArn"`
	}
	_ = json.Unmarshal(body, &req)
	tags, err := h.Backend.ListTagsForResource(req.ResourceArn)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{"tags": tags}), nil
}

func (h *Handler) handleTagResource(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags        map[string]string `json:"tags"`
		ResourceArn string            `json:"resourceArn"`
	}
	_ = json.Unmarshal(body, &req)

	return nil, h.Backend.TagResource(req.ResourceArn, req.Tags)
}

func (h *Handler) handleUntagResource(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		ResourceArn string   `json:"resourceArn"`
		TagKeys     []string `json:"tagKeys"`
	}
	_ = json.Unmarshal(body, &req)
	// tagKeys can also come from query params
	if len(req.TagKeys) == 0 {
		req.TagKeys = c.Request().URL.Query()["tagKeys"]
	}

	return nil, h.Backend.UntagResource(req.ResourceArn, req.TagKeys)
}

func (h *Handler) buildTagHandlers() map[string]opHandlerFn {
	return map[string]opHandlerFn{
		opListTagsForResource: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleListTagsForResource(ctx, body)
		},
		opTagResource: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleTagResource(ctx, body)
		},
		opUntagResource: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleUntagResource(ctx, body, ec)
		},
	}
}
