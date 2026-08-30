package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
)

// validDescribeTagsFilters is the set of filter names accepted by DescribeTags.
//
//nolint:gochecknoglobals // lookup set
var validDescribeTagsFilters = map[string]bool{
	"key":                 true,
	"resource-id":         true,
	filterKeyResourceType: true,
	"value":               true,
}

// handleDescribeTags returns tags for EC2 resources, supporting Filter.N.Name / Filter.N.Value.* semantics.
// Supports resource-id, key, value, and resource-type filters.
// Unknown filter names are rejected with InvalidParameterValue per AWS behaviour.
func (h *Handler) handleDescribeTags(vals url.Values, reqID string) (any, error) {
	var resourceIDs []string

	// keyFilters, valueFilters, typeFilters are post-fetch AND filters.
	var keyFilters, valueFilters, typeFilters []string

	for i := 1; i <= maxFiltersPerRequest; i++ {
		name := vals.Get(fmt.Sprintf("Filter.%d.Name", i))
		if name == "" {
			break
		}

		if !validDescribeTagsFilters[name] {
			return nil, fmt.Errorf(
				"%w: unknown filter name %q for DescribeTags",
				ErrInvalidParameter,
				name,
			)
		}

		filterVals := parseMemberList(vals, fmt.Sprintf("Filter.%d.Value", i))

		switch name {
		case "resource-id":
			resourceIDs = filterVals
		case "key":
			keyFilters = filterVals
		case "value":
			valueFilters = filterVals
		case filterKeyResourceType:
			typeFilters = filterVals
		}
	}

	entries := h.Backend.DescribeTags(resourceIDs)

	items := make([]tagItem, 0, len(entries))
	for _, e := range entries {
		if len(keyFilters) > 0 && !anyEqual(e.Key, keyFilters) {
			continue
		}
		if len(valueFilters) > 0 && !anyEqual(e.Value, valueFilters) {
			continue
		}
		if len(typeFilters) > 0 && !anyEqual(e.ResourceType, typeFilters) {
			continue
		}
		items = append(items, tagItem(e))
	}

	return &describeTagsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		TagSet:    tagItemSet{Items: items},
	}, nil
}

// handleCreateTags applies tags to one or more resources.
func (h *Handler) handleCreateTags(vals url.Values, reqID string) (any, error) {
	resourceIDs := parseMemberList(vals, "ResourceId")
	tags := parseEC2Tags(vals)

	if err := h.Backend.CreateTags(resourceIDs, tags); err != nil {
		return nil, err
	}

	return &createTagsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    ec2BooleanTrue,
	}, nil
}

// handleDeleteTags removes tags from one or more resources.
func (h *Handler) handleDeleteTags(vals url.Values, reqID string) (any, error) {
	resourceIDs := parseMemberList(vals, "ResourceId")
	keys := parseEC2TagKeys(vals)

	if err := h.Backend.DeleteTags(resourceIDs, keys); err != nil {
		return nil, err
	}

	return &deleteTagsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    ec2BooleanTrue,
	}, nil
}

type tagItem struct {
	ResourceID   string `xml:"resourceId"`
	ResourceType string `xml:"resourceType"`
	Key          string `xml:"key"`
	Value        string `xml:"value"`
}

type tagItemSet struct {
	Items []tagItem `xml:"item"`
}

type describeTagsResponse struct {
	XMLName   xml.Name   `xml:"DescribeTagsResponse"`
	Xmlns     string     `xml:"xmlns,attr"`
	RequestID string     `xml:"requestId"`
	TagSet    tagItemSet `xml:"tagSet"`
}

type createTagsResponse struct {
	XMLName   xml.Name `xml:"CreateTagsResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    string   `xml:"return"`
}

type deleteTagsResponse struct {
	XMLName   xml.Name `xml:"DeleteTagsResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    string   `xml:"return"`
}
