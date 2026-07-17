package mediaconvert

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// parseTagRoute maps tag-path requests to operations. Per the real
// MediaConvert restjson1 model: TagResource is POST /tags with the target
// ARN carried in the JSON body (not the URL), ListTagsForResource is
// GET /tags/{Arn}, and UntagResource is PUT /tags/{Arn} (not DELETE) with
// tagKeys carried in the JSON body.
func parseTagRoute(method, suffix string) mcRoute {
	resourceARN := strings.TrimPrefix(suffix, "/")

	switch method {
	case http.MethodGet:
		return mcRoute{operation: opListTagsForResource, resource: resourceARN}
	case http.MethodPost:
		return mcRoute{operation: opTagResource}
	case http.MethodPut:
		return mcRoute{operation: opUntagResource, resource: resourceARN}
	}

	return mcRoute{operation: opUnknown}
}

// --- Tags handlers ---

type resourceTagsOutput struct {
	ResourceTags resourceTagsEntry `json:"resourceTags"`
}

type resourceTagsEntry struct {
	Tags map[string]string `json:"tags"`
	Arn  string            `json:"arn"`
}

// tagResourceInput mirrors the real TagResourceInput wire shape: the target
// ARN is carried in the JSON body ("arn"), not in the URL path -- the real
// TagResource endpoint is POST /2017-08-29/tags with no ARN suffix.
type tagResourceInput struct {
	Tags map[string]string `json:"tags"`
	Arn  string            `json:"arn"`
}

// untagResourceInput mirrors the real UntagResourceInput wire shape: the
// keys to remove are carried in the JSON body ("tagKeys"), not as a
// repeated query parameter.
type untagResourceInput struct {
	TagKeys []string `json:"tagKeys"`
}

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceARN string) error {
	tags := h.Backend.GetTags(resourceARN)
	if tags == nil {
		tags = map[string]string{}
	}

	return c.JSON(http.StatusOK, resourceTagsOutput{
		ResourceTags: resourceTagsEntry{
			Arn:  resourceARN,
			Tags: tags,
		},
	})
}

func (h *Handler) handleTagResource(c *echo.Context, body []byte) error {
	var in tagResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if in.Arn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "arn is required"))
	}

	h.Backend.TagResource(in.Arn, in.Tags)

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceARN string, body []byte) error {
	var in untagResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	h.Backend.UntagResource(resourceARN, in.TagKeys)

	return c.NoContent(http.StatusNoContent)
}
