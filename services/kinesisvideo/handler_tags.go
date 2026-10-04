package kinesisvideo

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) buildTagOps() map[string]handlerFunc {
	return map[string]handlerFunc{
		pathTagStream:           h.handleTagStream,
		pathUntagStream:         h.handleUntagStream,
		pathListTagsForStream:   h.handleListTagsForStream,
		pathTagResource:         h.handleTagResource,
		pathUntagResource:       h.handleUntagResource,
		pathListTagsForResource: h.handleListTagsForResource,
	}
}

func (h *Handler) handleTagStream(c *echo.Context, body []byte) error {
	var req tagStreamRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	if err := h.Backend.TagStream(req.StreamName, req.StreamARN, req.Tags); err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, struct{}{})
}

func (h *Handler) handleUntagStream(c *echo.Context, body []byte) error {
	var req untagStreamRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	if err := h.Backend.UntagStream(req.StreamName, req.StreamARN, req.TagKeyList); err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, struct{}{})
}

func (h *Handler) handleListTagsForStream(c *echo.Context, body []byte) error {
	var req listTagsForStreamRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	tags, err := h.Backend.ListTagsForStream(req.StreamName, req.StreamARN)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, listTagsForStreamResponse{Tags: tags})
}

func (h *Handler) handleTagResource(c *echo.Context, body []byte) error {
	var req tagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	if req.ResourceARN == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "ResourceARN is required")
	}

	tags := make(map[string]string, len(req.Tags))
	for _, t := range req.Tags {
		tags[t.Key] = t.Value
	}

	if err := h.Backend.TagResource(req.ResourceARN, tags); err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, struct{}{})
}

func (h *Handler) handleUntagResource(c *echo.Context, body []byte) error {
	var req untagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	if req.ResourceARN == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "ResourceARN is required")
	}

	if err := h.Backend.UntagResource(req.ResourceARN, req.TagKeyList); err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, struct{}{})
}

func (h *Handler) handleListTagsForResource(c *echo.Context, body []byte) error {
	var req listTagsForResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	if req.ResourceARN == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "ResourceARN is required")
	}

	tags, err := h.Backend.ListTagsForResource(req.ResourceARN)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, listTagsForResourceResponse{Tags: tags})
}
