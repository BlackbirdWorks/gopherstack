package organizations

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type tagResourceRequest struct {
	ResourceID string `json:"ResourceId"`
	Tags       []Tag  `json:"Tags"`
}

type untagResourceRequest struct {
	ResourceID string   `json:"ResourceId"`
	TagKeys    []string `json:"TagKeys"`
}

type listTagsForResourceRequest struct {
	ResourceID string `json:"ResourceId"`
	NextToken  string `json:"NextToken,omitempty"`
}

type listTagsForResourceResponse struct {
	NextToken string `json:"NextToken,omitempty"`
	Tags      []Tag  `json:"Tags"`
}

// dispatchTags handles tag operations.
func (h *Handler) dispatchTags(c *echo.Context, op string, body []byte) (bool, error) {
	switch op {
	case "TagResource":
		return true, h.handleTagResource(c, body)
	case "UntagResource":
		return true, h.handleUntagResource(c, body)
	case "ListTagsForResource":
		return true, h.handleListTagsForResource(c, body)
	}

	return false, nil
}

// ----------------------------------------
// Tag handlers
// ----------------------------------------

func (h *Handler) handleTagResource(c *echo.Context, body []byte) error {
	var req tagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if err := h.Backend.TagResource(req.ResourceID, req.Tags); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleUntagResource(c *echo.Context, body []byte) error {
	var req untagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if err := h.Backend.UntagResource(req.ResourceID, req.TagKeys); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleListTagsForResource(c *echo.Context, body []byte) error {
	var req listTagsForResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	tags, err := h.Backend.ListTagsForResource(req.ResourceID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	p := page.New(tags, req.NextToken, 0, defaultMaxResults)

	return c.JSON(http.StatusOK, listTagsForResourceResponse{Tags: p.Data, NextToken: p.Next})
}
