package cloudtrail

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- AddTags ---

type addTagsBody struct {
	ResourceID string `json:"ResourceId"`
	TagsList   []struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	} `json:"TagsList"`
}

func (h *Handler) handleAddTags(c *echo.Context, body []byte) error {
	var in addTagsBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	kv := make(map[string]string, len(in.TagsList))
	for _, tag := range in.TagsList {
		kv[tag.Key] = tag.Value
	}

	if err := h.Backend.AddTags(in.ResourceID, kv); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- RemoveTags ---

type removeTagsBody struct {
	ResourceID string `json:"ResourceId"`
	TagsList   []struct {
		Key string `json:"Key"`
	} `json:"TagsList"`
}

func (h *Handler) handleRemoveTags(c *echo.Context, body []byte) error {
	var in removeTagsBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	keys := make([]string, 0, len(in.TagsList))
	for _, tag := range in.TagsList {
		keys = append(keys, tag.Key)
	}

	if err := h.Backend.RemoveTags(in.ResourceID, keys); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- ListTags ---

type listTagsBody struct {
	ResourceIDList []string `json:"ResourceIdList"`
}

func (h *Handler) handleListTags(c *echo.Context, body []byte) error {
	var in listTagsBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	tagsByResource := h.Backend.ListTags(in.ResourceIDList)
	resourceTagList := make([]map[string]any, 0, len(tagsByResource))

	for resourceID, kv := range tagsByResource {
		tagList := make([]map[string]string, 0, len(kv))
		for k, v := range kv {
			tagList = append(tagList, map[string]string{keyKey: k, keyValue: v})
		}
		resourceTagList = append(resourceTagList, map[string]any{
			"ResourceId": resourceID,
			"TagsList":   tagList,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ResourceTagList": resourceTagList,
	})
}
