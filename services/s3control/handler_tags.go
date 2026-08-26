package s3control

import (
	"encoding/xml"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const pathTagsPrefix = "/v20180820/tags/"

// extractTagOps handles resource tagging operations.
func extractTagOps(path, method string) string {
	switch {
	case strings.HasPrefix(path, pathTagsPrefix) && method == http.MethodGet:
		return "ListTagsForResource"
	case strings.HasPrefix(path, pathTagsPrefix) && method == http.MethodPost:
		return "TagResource"
	case strings.HasPrefix(path, pathTagsPrefix) && method == http.MethodDelete:
		return "UntagResource"
	}

	return opUnknown
}

// dispatchTagDispatch handles resource tagging dispatch.
func (h *Handler) dispatchTagDispatch(c *echo.Context, path, method string) error {
	switch {
	case strings.HasPrefix(path, pathTagsPrefix) && method == http.MethodGet:
		return h.handleListTagsForResource(c)
	case strings.HasPrefix(path, pathTagsPrefix) && method == http.MethodPost:
		return h.handleTagResource(c)
	case strings.HasPrefix(path, pathTagsPrefix) && method == http.MethodDelete:
		return h.handleUntagResource(c)
	}

	return writeXMLErrorCode(c, http.StatusNotFound, "NotFound", "not found")
}

// ---- Resource Tags ----

type resourceTagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type listTagsForResourceResultXML struct {
	XMLName xml.Name         `xml:"ListTagsForResourceResult"`
	Tags    []resourceTagXML `xml:"Tags>Tag"`
}

func (h *Handler) handleListTagsForResource(c *echo.Context) error {
	arn := strings.TrimPrefix(c.Request().URL.Path, pathTagsPrefix)

	tags := h.Backend.ListTagsForResource(arn)
	items := make([]resourceTagXML, 0, len(tags))

	for k, v := range tags {
		items = append(items, resourceTagXML{Key: k, Value: v})
	}

	return writeXML(c, listTagsForResourceResultXML{Tags: items})
}

type tagResourceRequestXML struct {
	XMLName xml.Name         `xml:"TagResourceRequest"`
	Tags    []resourceTagXML `xml:"Tags>Tag"`
}

func (h *Handler) handleTagResource(c *echo.Context) error {
	arn := strings.TrimPrefix(c.Request().URL.Path, pathTagsPrefix)

	var body tagResourceRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	tags := make(map[string]string, len(body.Tags))
	for _, t := range body.Tags {
		tags[t.Key] = t.Value
	}

	h.Backend.TagResource(arn, tags)

	return writeXML(c, struct {
		XMLName xml.Name `xml:"TagResourceResult"`
	}{})
}

// handleUntagResource. UntagResourceInput has no XML request body in the
// real API — TagKeys travels as repeated "tagKeys" query-string parameters
// (awsRestxml_serializeOpHttpBindingsUntagResourceInput calls
// encoder.AddQuery("tagKeys") for each key; there is no body serializer).
func (h *Handler) handleUntagResource(c *echo.Context) error {
	arn := strings.TrimPrefix(c.Request().URL.Path, pathTagsPrefix)
	tagKeys := c.Request().URL.Query()["tagKeys"]

	h.Backend.UntagResource(arn, tagKeys)

	return c.NoContent(http.StatusNoContent)
}
