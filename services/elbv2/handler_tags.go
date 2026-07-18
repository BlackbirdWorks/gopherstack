package elbv2

import (
	"encoding/xml"
	"fmt"
	"net/url"
)

func (h *Handler) handleAddTags(vals url.Values) (any, error) {
	resourceArns := parseMembers(vals, "ResourceArns.member")
	if len(resourceArns) == 0 {
		return nil, fmt.Errorf("%w: at least one ResourceArn is required", ErrInvalidParameter)
	}

	kvs := parseTagKVs(vals)

	if err := h.Backend.AddTags(resourceArns, kvs); err != nil {
		return nil, err
	}

	return &addTagsResponse{
		Xmlns:            elbv2XMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-addtags"},
	}, nil
}

func (h *Handler) handleRemoveTags(vals url.Values) (any, error) {
	resourceArns := parseMembers(vals, "ResourceArns.member")
	if len(resourceArns) == 0 {
		return nil, fmt.Errorf("%w: at least one ResourceArn is required", ErrInvalidParameter)
	}

	keys := parseTagKeys(vals, "TagKeys.member")

	if err := h.Backend.RemoveTags(resourceArns, keys); err != nil {
		return nil, err
	}

	return &removeTagsResponse{
		Xmlns:            elbv2XMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-removetags"},
	}, nil
}

func (h *Handler) handleDescribeTags(vals url.Values) (any, error) {
	resourceArns := parseMembers(vals, "ResourceArns.member")
	if len(resourceArns) == 0 {
		return nil, fmt.Errorf("%w: at least one ResourceArn is required", ErrInvalidParameter)
	}

	tagMap, err := h.Backend.DescribeTags(resourceArns)
	if err != nil {
		return nil, err
	}

	tagDescs := make([]xmlTagDescription, 0, len(resourceArns))
	for _, resArn := range resourceArns {
		kvs := tagMap[resArn]
		xmlKVs := make([]xmlTag, 0, len(kvs))

		for _, kv := range kvs {
			xmlKVs = append(xmlKVs, xmlTag{Key: kv.Key, Value: kv.Value})
		}

		tagDescs = append(tagDescs, xmlTagDescription{
			ResourceArn: resArn,
			Tags:        xmlTagList{Members: xmlKVs},
		})
	}

	return &describeTagsResponse{
		Xmlns: elbv2XMLNS,
		Result: describeTagsResult{
			TagDescriptions: xmlTagDescriptionList{Members: tagDescs},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describetags"},
	}, nil
}

func (h *Handler) handleGetResourcePolicy(vals url.Values) (any, error) {
	resourceArn := vals.Get("ResourceArn")
	if resourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrInvalidParameter)
	}

	policy, err := h.Backend.GetResourcePolicy(resourceArn)
	if err != nil {
		return nil, err
	}

	return &getResourcePolicyResponse{
		Xmlns:            elbv2XMLNS,
		Result:           getResourcePolicyResult{Policy: policy},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-get-resource-policy"},
	}, nil
}

type xmlTag struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type xmlTagList struct {
	Members []xmlTag `xml:"member"`
}

type xmlTagDescription struct {
	ResourceArn string     `xml:"ResourceArn"`
	Tags        xmlTagList `xml:"Tags"`
}

type xmlTagDescriptionList struct {
	Members []xmlTagDescription `xml:"member"`
}

type addTagsResponse struct {
	Result           struct{}            `xml:"AddTagsResult"`
	XMLName          xml.Name            `xml:"AddTagsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type removeTagsResponse struct {
	Result           struct{}            `xml:"RemoveTagsResult"`
	XMLName          xml.Name            `xml:"RemoveTagsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type describeTagsResult struct {
	TagDescriptions xmlTagDescriptionList `xml:"TagDescriptions"`
}

type describeTagsResponse struct {
	XMLName          xml.Name            `xml:"DescribeTagsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
	Result           describeTagsResult  `xml:"DescribeTagsResult"`
}

type getResourcePolicyResult struct {
	Policy string `xml:"Policy,omitempty"`
}

type getResourcePolicyResponse struct {
	XMLName          xml.Name                `xml:"GetResourcePolicyResponse"`
	Xmlns            string                  `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata     `xml:"ResponseMetadata"`
	Result           getResourcePolicyResult `xml:"GetResourcePolicyResult"`
}
