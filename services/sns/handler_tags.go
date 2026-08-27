package sns

import (
	"encoding/xml"
	"fmt"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func (h *Handler) handleListTagsForResource(c *echo.Context) error {
	resourceArn := c.Request().FormValue("ResourceArn")
	tags := h.Backend.GetTopicTags(resourceArn)
	tagList := make([]svcTags.KV, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, svcTags.KV{Key: k, Value: v})
	}

	return h.writeXML(c, snsListTagsResponse{
		Result:           snsListTagsResult{Tags: tagList},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	})
}

// parseSNSTagsFromForm reads Tags.member.N.Key/Value pairs from the form.
func parseSNSTagsFromForm(c *echo.Context) map[string]string {
	tags := make(map[string]string)
	for i := 1; ; i++ {
		k := c.Request().FormValue(fmt.Sprintf("Tags.member.%d.Key", i))
		if k == "" {
			return tags
		}
		tags[k] = c.Request().FormValue(fmt.Sprintf("Tags.member.%d.Value", i))
	}
}

// parseSNSTagKeysFromForm reads TagKeys.member.N values from the form.
func parseSNSTagKeysFromForm(c *echo.Context) []string {
	var keys []string
	for i := 1; ; i++ {
		k := c.Request().FormValue(fmt.Sprintf("TagKeys.member.%d", i))
		if k == "" {
			return keys
		}
		keys = append(keys, k)
	}
}

func (h *Handler) handleTagResource(c *echo.Context) error {
	resourceArn := c.Request().FormValue("ResourceArn")
	kv := parseSNSTagsFromForm(c)
	h.Backend.SetTopicTags(resourceArn, svcTags.FromMap("sns."+resourceArn+".tags.input", kv))

	return h.writeXML(
		c,
		snsEmptyResponse{
			XMLName: xml.Name{
				Space: "https://sns.amazonaws.com/doc/2010-03-31/",
				Local: "TagResourceResponse",
			},
			Result: struct {
				XMLName xml.Name `xml:""`
			}{XMLName: xml.Name{Local: "TagResourceResult"}},
		},
	)
}

func (h *Handler) handleUntagResource(c *echo.Context) error {
	resourceArn := c.Request().FormValue("ResourceArn")
	keys := parseSNSTagKeysFromForm(c)
	h.Backend.RemoveTopicTags(resourceArn, keys)

	return h.writeXML(
		c,
		snsEmptyResponse{
			XMLName: xml.Name{
				Space: "https://sns.amazonaws.com/doc/2010-03-31/",
				Local: "UntagResourceResponse",
			},
			Result: struct {
				XMLName xml.Name `xml:""`
			}{XMLName: xml.Name{Local: "UntagResourceResult"}},
		},
	)
}
