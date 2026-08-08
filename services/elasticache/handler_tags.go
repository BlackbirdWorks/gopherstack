package elasticache

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/labstack/echo/v5"
)

// applyCreateTimeTags applies any Tags.Tag.N.Key/Value form fields to resourceARN
// right after creation. Errors are discarded: resourceARN was just created by the
// caller, so AddTagsToResource can only fail here if the ARN lookup itself is broken.
func (h *Handler) applyCreateTimeTags(ctx context.Context, form url.Values, resourceARN string) {
	if initialTags := parseFormTags(form); len(initialTags) > 0 {
		_ = h.Backend.AddTagsToResource(ctx, resourceARN, initialTags)
	}
}

func (h *Handler) listTagsForResource(ctx context.Context, c *echo.Context, form url.Values) error {
	arn := form.Get("ResourceName")
	tags, err := h.Backend.ListTagsForResource(ctx, arn)
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidARN", err.Error())
	}

	type tag struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	type tagList struct {
		Tag []tag `xml:"Tag"`
	}
	type result struct {
		XMLName xml.Name `xml:"ListTagsForResourceResponse"`
		Xmlns   string   `xml:"xmlns,attr"`
		TagList tagList  `xml:"ListTagsForResourceResult>TagList"`
	}

	items := make([]tag, 0, len(tags))
	for k, v := range tags {
		items = append(items, tag{Key: k, Value: v})
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:   elasticacheNS,
		TagList: tagList{Tag: items},
	})
}

func (h *Handler) addTagsToResource(ctx context.Context, c *echo.Context, form url.Values) error {
	resourceARN := form.Get("ResourceName")

	newTags := make(map[string]string)
	for i := 1; ; i++ {
		key := form.Get(fmt.Sprintf("Tags.Tag.%d.Key", i))
		if key == "" {
			break
		}
		val := form.Get(fmt.Sprintf("Tags.Tag.%d.Value", i))
		newTags[key] = val
	}

	if err := h.Backend.AddTagsToResource(ctx, resourceARN, newTags); err != nil {
		if errors.Is(err, ErrResourceNotFound) {
			return xmlError(c, http.StatusBadRequest, "InvalidARN", err.Error())
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type tag struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	type tagList struct {
		Tag []tag `xml:"Tag"`
	}
	type result struct {
		XMLName xml.Name `xml:"AddTagsToResourceResponse"`
		Xmlns   string   `xml:"xmlns,attr"`
		TagList tagList  `xml:"AddTagsToResourceResult>TagList"`
	}

	items := make([]tag, 0, len(newTags))
	for k, v := range newTags {
		items = append(items, tag{Key: k, Value: v})
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:   elasticacheNS,
		TagList: tagList{Tag: items},
	})
}

func (h *Handler) removeTagsFromResource(ctx context.Context, c *echo.Context, form url.Values) error {
	resourceARN := form.Get("ResourceName")

	var tagKeys []string
	for i := 1; ; i++ {
		key := form.Get(fmt.Sprintf("TagKeys.member.%d", i))
		if key == "" {
			break
		}
		tagKeys = append(tagKeys, key)
	}

	if err := h.Backend.RemoveTagsFromResource(ctx, resourceARN, tagKeys); err != nil {
		if errors.Is(err, ErrResourceNotFound) {
			return xmlError(c, http.StatusBadRequest, "InvalidARN", err.Error())
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type tag struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	type tagList struct {
		Tag []tag `xml:"Tag"`
	}
	type result struct {
		XMLName xml.Name `xml:"RemoveTagsFromResourceResponse"`
		Xmlns   string   `xml:"xmlns,attr"`
		TagList tagList  `xml:"RemoveTagsFromResourceResult>TagList"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:   elasticacheNS,
		TagList: tagList{Tag: []tag{}},
	})
}
