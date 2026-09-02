package cloudfront

import (
	"encoding/xml"
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// handleTagAPIError maps TagResource/UntagResource errors. Both ops' own
// deserializers (cloudfront@v1.67.4 deserializers.go) model NoSuchResource
// for an unrecognized resource ARN, not NoSuchDistribution -- unlike most
// other ops that reuse ErrNotFound.
func (h *Handler) handleTagAPIError(c *echo.Context, err error) error {
	if errors.Is(err, ErrNotFound) {
		return xmlResp(c, http.StatusNotFound, cfErrorXML("NoSuchResource", err.Error()))
	}

	return h.handleError(c, err)
}

type tagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type tagsXML struct {
	XMLName xml.Name `xml:"Tags"`
	XMLNS   string   `xml:"xmlns,attr,omitempty"`
	Items   []tagXML `xml:"Items>Tag"`
}

type untagBody struct {
	Keys []string `xml:"Items>Key"`
}

// tagsXMLToMap converts a decoded tagsXML into the map[string]string shape
// backend Create methods take.
func tagsXMLToMap(t tagsXML) map[string]string {
	if len(t.Items) == 0 {
		return nil
	}

	kv := make(map[string]string, len(t.Items))
	for _, item := range t.Items {
		kv[item.Key] = item.Value
	}

	return kv
}

func (h *Handler) handleTagResource(c *echo.Context) error {
	resourceARN := c.Request().URL.Query().Get("Resource")

	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var tags tagsXML
	if xmlErr := xml.Unmarshal(body, &tags); xmlErr != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "invalid Tags XML"))
	}

	kv := make(map[string]string, len(tags.Items))
	for _, t := range tags.Items {
		kv[t.Key] = t.Value
	}

	if tagErr := h.Backend.TagResource(resourceARN, kv); tagErr != nil {
		return h.handleTagAPIError(c, tagErr)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUntagResource(c *echo.Context) error {
	resourceARN := c.Request().URL.Query().Get("Resource")

	var keys []string

	// Keys may be in query params or body.
	keys = append(keys, c.Request().URL.Query()["TagKeys.Key"]...)

	if len(keys) == 0 {
		body, err := readBody(c)
		if err == nil && len(body) > 0 {
			var ub untagBody
			if xmlErr := xml.Unmarshal(body, &ub); xmlErr == nil {
				keys = ub.Keys
			}
		}
	}

	if untagErr := h.Backend.UntagResource(resourceARN, keys); untagErr != nil {
		return h.handleTagAPIError(c, untagErr)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListTagsForResource(c *echo.Context) error {
	resourceARN := c.Request().URL.Query().Get("Resource")

	kv, err := h.Backend.ListTags(resourceARN)
	if err != nil {
		return h.handleTagAPIError(c, err)
	}

	// Sort tags by key for deterministic output.
	keys := collections.SortedKeys(kv)

	items := make([]tagXML, 0, len(kv))
	for _, k := range keys {
		items = append(items, tagXML{Key: k, Value: kv[k]})
	}

	tags := tagsXML{XMLNS: cfNS, Items: items}

	out, xmlErr := xml.Marshal(tags)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

// --- Invalidation handlers ---
