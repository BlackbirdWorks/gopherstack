package iotwireless

import (
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type tagResourceRequest struct {
	Tags []tags.KV `json:"Tags"`
}

type listTagsResponse struct {
	Tags []tags.KV `json:"Tags"`
}

// --- Tag handlers ---
//
// TagResource / UntagResource / ListTagsForResource all bind to the fixed
// path "/tags" (never "/tags/{arn}"): the resourceArn travels as the
// "resourceArn" query parameter (real AWS's httpQuery binding), and — for
// TagResource — Tags travels in the JSON body as []Tag{Key,Value}, not a
// bare map. See parsePartnerAccountPath-adjacent routing in
// parseIoTWirelessPath's "tags" case.

func (h *Handler) listTagsForResource(c *echo.Context, resourceArn string) error {
	if resourceArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException: resourceArn is required")
	}

	tagMap, err := h.Backend.ListTagsForResource(resourceArn)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusOK, listTagsResponse{Tags: tagMapToKVs(tagMap)})
}

func (h *Handler) tagResource(c *echo.Context, resourceArn string, body []byte) error {
	if resourceArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException: resourceArn is required")
	}

	var req tagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if err := h.Backend.TagResource(resourceArn, tagKVsToMap(req.Tags)); err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) untagResource(c *echo.Context, resourceArn string, query url.Values) error {
	if resourceArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException: resourceArn is required")
	}

	tagKeys := query["tagKeys"]

	if err := h.Backend.UntagResource(resourceArn, tagKeys); err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}
