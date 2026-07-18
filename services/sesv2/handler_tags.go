package sesv2

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/labstack/echo/v5"
)

type listTagsOutput struct {
	Tags []tagEntry `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(c *echo.Context) (any, error) {
	arn := c.QueryParam("ResourceArn")

	tags, err := h.Backend.ListTagsForResource(arn)
	if err != nil {
		return nil, err
	}

	entries := make([]tagEntry, 0, len(tags))
	for k, v := range tags {
		entries = append(entries, tagEntry{Key: k, Value: v})
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })

	return &listTagsOutput{Tags: entries}, nil
}

type tagResourceInput struct {
	ResourceArn string     `json:"ResourceArn"`
	Tags        []tagEntry `json:"Tags"`
}

func (h *Handler) handleTagResource(c *echo.Context) (any, error) {
	var in tagResourceInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid body", ErrInvalidInput)
	}

	tags := make(map[string]string, len(in.Tags))
	for _, t := range in.Tags {
		tags[t.Key] = t.Value
	}

	if err := h.Backend.TagResource(in.ResourceArn, tags); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleUntagResource(c *echo.Context) (any, error) {
	arn := c.QueryParam("ResourceArn")
	keysParam := c.QueryParam("TagKeys")

	var keys []string
	if keysParam != "" {
		keys = strings.Split(keysParam, ",")
	}

	if err := h.Backend.UntagResource(arn, keys); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}
