package eks

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"
)

// dispatchTagOps handles the generic ARN-based tag operations.
func (h *Handler) dispatchTagOps(c *echo.Context, route eksRoute, body []byte) (bool, error) {
	switch route.operation {
	case opTagResource:
		return true, h.handleTagResource(c, route.resourceARN, body)
	case opUntagResource:
		return true, h.handleUntagResource(c, route.resourceARN)
	case opListTagsForResource:
		return true, h.handleListTagsForResource(c, route.resourceARN)
	}

	return false, nil
}

// handleTagError maps TagResource/UntagResource/ListTagsForResource errors
// to their real codes. eks@v1.90.4 deserializers.go's
// awsRestjson1_deserializeOpError<Op> switch for all three of these ops
// models only BadRequestException/NotFoundException -- a different
// exception family from the ResourceNotFoundException/InvalidParameterException
// pair the rest of this service's ops use via handleError.
func (h *Handler) handleTagError(c *echo.Context, err error) error {
	if errors.Is(err, ErrNotFound) {
		return c.JSON(http.StatusNotFound, errResp("NotFoundException", err.Error()))
	}

	return c.JSON(http.StatusBadRequest, errResp("BadRequestException", err.Error()))
}

// validateTagMap checks AWS EKS tag constraints: key 1-128 chars, value 0-256 chars,
// max 50 tags per resource. existingCount is the number of tags already on the resource.
func validateTagMap(kv map[string]string, existingCount int) error {
	if existingCount+len(kv) > maxTagsPerRes {
		return ErrValidation
	}

	for k, v := range kv {
		if len(k) == 0 || len(k) > maxTagKeyLen {
			return ErrValidation
		}

		if len(v) > maxTagValLen {
			return ErrValidation
		}
	}

	return nil
}

type tagResourceBody struct {
	Tags map[string]string `json:"tags"`
}

func (h *Handler) handleTagResource(c *echo.Context, resourceARN string, body []byte) error {
	var in tagResourceBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequestException", "invalid request body"))
	}

	if in.Tags == nil {
		in.Tags = make(map[string]string)
	}

	existing, existErr := h.Backend.ListTagsForResource(resourceARN)
	if existErr != nil {
		return h.handleTagError(c, existErr)
	}

	if validateErr := validateTagMap(in.Tags, len(existing)); validateErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequestException",
			"tag key must be 1-128 chars, value 0-256 chars, max 50 tags per resource"))
	}

	if err := h.Backend.TagResource(resourceARN, in.Tags); err != nil {
		return h.handleTagError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceARN string) error {
	tagKeys := c.Request().URL.Query()["tagKeys"]

	if err := h.Backend.UntagResource(resourceARN, tagKeys); err != nil {
		return h.handleTagError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceARN string) error {
	t, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return h.handleTagError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyTags: t,
	})
}
