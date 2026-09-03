package autoscaling

import (
	"encoding/xml"
	"fmt"
	"net/url"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// defaultTagsMaxRecords and maxTagsMaxRecords are DescribeTags's documented default/max page
// size (api_op_DescribeTags.go: "The default value is 50 and the maximum value is 100").
const (
	defaultTagsMaxRecords = 50
	maxTagsMaxRecords     = 100
)

func (h *Handler) handleCreateOrUpdateTags(vals url.Values) (any, error) {
	tags := parseResourceTags(vals, "Tags.member")

	if err := h.Backend.CreateOrUpdateTags(tags); err != nil {
		return nil, err
	}

	return &createOrUpdateTagsResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-create-or-update-tags"},
	}, nil
}

func (h *Handler) handleDeleteTags(vals url.Values) (any, error) {
	tags := parseResourceTags(vals, "Tags.member")

	if err := h.Backend.DeleteTags(tags); err != nil {
		return nil, err
	}

	return &deleteTagsResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-delete-tags"},
	}, nil
}

func (h *Handler) handleDescribeTags(vals url.Values) (any, error) {
	filters := parseTagFilters(vals)

	tags, err := h.Backend.DescribeTags(filters)
	if err != nil {
		return nil, err
	}

	maxRecords := defaultTagsMaxRecords
	if v := vals.Get("MaxRecords"); v != "" {
		if n, parseErr := parseIntVal(v); parseErr == nil && n > 0 {
			maxRecords = min(int(n), maxTagsMaxRecords)
		}
	}

	p := page.New(tags, vals.Get("NextToken"), maxRecords, defaultTagsMaxRecords)

	members := make([]xmlResourceTag, 0, len(p.Data))
	for _, tag := range p.Data {
		members = append(members, xmlResourceTag(tag))
	}

	return &describeTagsResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeTagsResult{
			NextToken: p.Next,
			Tags:      xmlResourceTagList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-tags"},
	}, nil
}

// parseResourceTags parses resource-scoped tags from form values.
func parseResourceTags(vals url.Values, prefix string) []ResourceTag {
	result := make([]ResourceTag, 0)

	for i := 1; ; i++ {
		keyParam := fmt.Sprintf("%s.%d.Key", prefix, i)
		k := vals.Get(keyParam)

		if k == "" {
			break
		}

		propagate := true
		if v := vals.Get(fmt.Sprintf("%s.%d.PropagateAtLaunch", prefix, i)); v != "" {
			propagate = v == formValueTrue
		}

		result = append(result, ResourceTag{
			ResourceID:        vals.Get(fmt.Sprintf("%s.%d.ResourceId", prefix, i)),
			ResourceType:      vals.Get(fmt.Sprintf("%s.%d.ResourceType", prefix, i)),
			Key:               k,
			Value:             vals.Get(fmt.Sprintf("%s.%d.Value", prefix, i)),
			PropagateAtLaunch: propagate,
		})
	}

	return result
}

type createOrUpdateTagsResponse struct {
	XMLName          xml.Name            `xml:"CreateOrUpdateTagsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type deleteTagsResponse struct {
	XMLName          xml.Name            `xml:"DeleteTagsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type xmlResourceTag struct {
	ResourceID        string `xml:"ResourceId"`
	ResourceType      string `xml:"ResourceType"`
	Key               string `xml:"Key"`
	Value             string `xml:"Value,omitempty"`
	PropagateAtLaunch bool   `xml:"PropagateAtLaunch,omitempty"`
}

type xmlResourceTagList struct {
	Members []xmlResourceTag `xml:"member"`
}

type describeTagsResult struct {
	NextToken string             `xml:"NextToken,omitempty"`
	Tags      xmlResourceTagList `xml:"Tags"`
}

type describeTagsResponse struct {
	XMLName          xml.Name            `xml:"DescribeTagsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
	Result           describeTagsResult  `xml:"DescribeTagsResult"`
}

// parseTagFilters parses Filters from form values used in DescribeTags.
func parseTagFilters(vals url.Values) []TagFilter {
	var filters []TagFilter

	for i := 1; ; i++ {
		nameKey := fmt.Sprintf("Filters.member.%d.Name", i)
		name := vals.Get(nameKey)

		if name == "" {
			break
		}

		var values []string

		for j := 1; ; j++ {
			valKey := fmt.Sprintf("Filters.member.%d.Values.member.%d", i, j)
			v := vals.Get(valKey)

			if v == "" {
				break
			}

			values = append(values, v)
		}

		filters = append(filters, TagFilter{Name: name, Values: values})
	}

	return filters
}
