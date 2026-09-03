package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strings"
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

// tagKeyValueFilter is one "tag:<key>" filter (api_op_DescribeTags.go):
// matches entries whose Key equals key and whose Value is in values.
type tagKeyValueFilter struct {
	key    string
	values []string
}

// describeTagsFilters holds the parsed, post-fetch AND filters for
// handleDescribeTags: resourceIDs narrows the Backend.DescribeTags call
// itself, the rest are applied per-entry.
type describeTagsFilters struct {
	resourceIDs        []string
	keyFilters         []string
	valueFilters       []string
	typeFilters        []string
	tagKeyValueFilters []tagKeyValueFilter
}

// parseDescribeTagsFilters reads Filter.N.Name/Filter.N.Value.* from vals.
// Supports resource-id, key, value, resource-type, and tag:<key> filters.
// Unknown filter names are rejected with InvalidParameterValue per AWS behaviour.
func parseDescribeTagsFilters(vals url.Values) (describeTagsFilters, error) {
	var f describeTagsFilters

	for i := 1; i <= maxFiltersPerRequest; i++ {
		name := vals.Get(fmt.Sprintf("Filter.%d.Name", i))
		if name == "" {
			break
		}

		filterVals := parseMemberList(vals, fmt.Sprintf("Filter.%d.Value", i))

		if tagKey, ok := strings.CutPrefix(name, "tag:"); ok {
			f.tagKeyValueFilters = append(f.tagKeyValueFilters, tagKeyValueFilter{key: tagKey, values: filterVals})

			continue
		}

		if !validDescribeTagsFilters[name] {
			return describeTagsFilters{}, fmt.Errorf(
				"%w: unknown filter name %q for DescribeTags",
				ErrInvalidParameter,
				name,
			)
		}

		switch name {
		case "resource-id":
			f.resourceIDs = filterVals
		case "key":
			f.keyFilters = filterVals
		case "value":
			f.valueFilters = filterVals
		case filterKeyResourceType:
			f.typeFilters = filterVals
		}
	}

	return f, nil
}

// matches reports whether e satisfies every parsed filter (AND across
// filter names, OR within a filter's values).
func (f describeTagsFilters) matches(e TagEntry) bool {
	if len(f.keyFilters) > 0 && !anyEqual(e.Key, f.keyFilters) {
		return false
	}
	if len(f.valueFilters) > 0 && !anyEqual(e.Value, f.valueFilters) {
		return false
	}
	if len(f.typeFilters) > 0 && !anyEqual(e.ResourceType, f.typeFilters) {
		return false
	}

	for _, tf := range f.tagKeyValueFilters {
		if e.Key != tf.key || !anyEqual(e.Value, tf.values) {
			return false
		}
	}

	return true
}

// handleDescribeTags returns tags for EC2 resources, supporting Filter.N.Name / Filter.N.Value.* semantics.
func (h *Handler) handleDescribeTags(vals url.Values, reqID string) (any, error) {
	filters, err := parseDescribeTagsFilters(vals)
	if err != nil {
		return nil, err
	}

	entries := h.Backend.DescribeTags(filters.resourceIDs)

	items := make([]tagItem, 0, len(entries))
	for _, e := range entries {
		if filters.matches(e) {
			items = append(items, tagItem(e))
		}
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
