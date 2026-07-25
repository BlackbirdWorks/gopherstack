package directoryservice

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleAddTagsToResource(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		ResourceID string `json:"ResourceId"`
		Tags       []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.ResourceID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "ResourceId is required"))
	}

	tags := reqTagsToTags(req.Tags)

	if tagErr := h.Backend.AddTagsToResource(h.contextWithRegion(c), req.ResourceID, tags); tagErr != nil {
		return h.mapError(c, tagErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleRemoveTagsFromResource(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		ResourceID string   `json:"ResourceId"`
		TagKeys    []string `json:"TagKeys"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.ResourceID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "ResourceId is required"))
	}

	untagErr := h.Backend.RemoveTagsFromResource(h.contextWithRegion(c), req.ResourceID, req.TagKeys)
	if untagErr != nil {
		return h.mapError(c, untagErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListTagsForResource(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		ResourceID string `json:"ResourceId"`
		NextToken  string `json:"NextToken"`
		Limit      int32  `json:"Limit"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.ResourceID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "ResourceId is required"))
	}

	tags, nextToken, listErr := h.Backend.ListTagsForResource(
		h.contextWithRegion(c),
		req.ResourceID,
		req.Limit,
		req.NextToken,
	)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	tagList := make([]map[string]any, 0, len(tags))
	for _, t := range tags {
		tagList = append(tagList, map[string]any{
			"Key":   t.Key,
			"Value": t.Value,
		})
	}

	resp := map[string]any{
		"Tags": tagList,
	}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}
