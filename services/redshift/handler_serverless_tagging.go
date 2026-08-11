package redshift

import (
	"encoding/json"
	"net/http"
	"sort"

	"github.com/labstack/echo/v5"
)

// slTagWire is the wire shape of a single Redshift Serverless tag -- a
// {"key":..., "value":...} object, NOT a JSON map (confirmed against the
// "Tag"/"TagList" shapes in service-2.json: TagResourceRequest.tags is a
// TagList of Tag structs with required "key"/"value" members).
type slTagWire struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// slTagsFromWire converts the wire TagList shape into the plain map this
// backend stores tags as internally.
func slTagsFromWire(list []slTagWire) map[string]string {
	if len(list) == 0 {
		return nil
	}

	m := make(map[string]string, len(list))
	for _, t := range list {
		m[t.Key] = t.Value
	}

	return m
}

// slTagsToWire converts the internal tag map into the wire TagList shape,
// sorted by key for deterministic output.
func slTagsToWire(m map[string]string) []slTagWire {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	list := make([]slTagWire, 0, len(m))
	for _, k := range keys {
		list = append(list, slTagWire{Key: k, Value: m[k]})
	}

	return list
}

func (h *ServerlessHandler) handleTagResource(c *echo.Context, body []byte) error {
	var req struct {
		ResourceArn string      `json:"resourceArn"`
		Tags        []slTagWire `json:"tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	if req.ResourceArn == "" || len(req.Tags) == 0 {
		return slBadRequest(c, "resourceArn and tags are required")
	}

	if err := h.Backend.TagServerlessResource(req.ResourceArn, slTagsFromWire(req.Tags)); err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *ServerlessHandler) handleUntagResource(c *echo.Context, body []byte) error {
	var req struct {
		ResourceArn string   `json:"resourceArn"`
		TagKeys     []string `json:"tagKeys"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	if req.ResourceArn == "" || len(req.TagKeys) == 0 {
		return slBadRequest(c, "resourceArn and tagKeys are required")
	}

	if err := h.Backend.UntagServerlessResource(req.ResourceArn, req.TagKeys); err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *ServerlessHandler) handleListTagsForResource(c *echo.Context, body []byte) error {
	var req struct {
		ResourceArn string `json:"resourceArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	if req.ResourceArn == "" {
		return slBadRequest(c, "resourceArn is required")
	}

	tagMap, err := h.Backend.ListServerlessResourceTags(req.ResourceArn)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"tags": slTagsToWire(tagMap)})
}
