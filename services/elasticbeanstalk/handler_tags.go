package elasticbeanstalk

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
)

// --- Tags operations ---

type tagDescType struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type listTagsForResourceResult struct {
	ResourceArn  string        `xml:"ResourceArn"`
	ResourceTags []tagDescType `xml:"ResourceTags>member"`
}

type listTagsForResourceResponse struct {
	XMLName                   xml.Name                  `xml:"ListTagsForResourceResponse"`
	Xmlns                     string                    `xml:"xmlns,attr"`
	ResponseMetadata          responseMetadata          `xml:"ResponseMetadata"`
	ListTagsForResourceResult listTagsForResourceResult `xml:"ListTagsForResourceResult"`
}

func (h *Handler) handleListTagsForResource(ctx context.Context, vals url.Values) (any, error) {
	resourceARN := vals.Get("ResourceArn")
	if resourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrInvalidParameter)
	}

	tags, err := h.Backend.ListTagsForResource(ctx, resourceARN)
	if err != nil {
		return nil, err
	}

	keys := sortedTagKeys(tags)
	members := make([]tagDescType, 0, len(keys))

	for _, k := range keys {
		members = append(members, tagDescType{Key: k, Value: tags[k]})
	}

	return &listTagsForResourceResponse{
		Xmlns: ebXMLNS,
		ListTagsForResourceResult: listTagsForResourceResult{
			ResourceArn:  resourceARN,
			ResourceTags: members,
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-list-tags"},
	}, nil
}

type updateTagsForResourceResponse struct {
	XMLName          xml.Name         `xml:"UpdateTagsForResourceResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
}

func (h *Handler) handleUpdateTagsForResource(ctx context.Context, vals url.Values) (any, error) {
	resourceARN := vals.Get("ResourceArn")
	if resourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrInvalidParameter)
	}

	addTags := parseTagList(vals, "TagsToAdd.member")
	removeTagKeys := parseMembers(vals, "TagsToRemove.member")

	removeTags := make(map[string]string, len(removeTagKeys))

	for _, k := range removeTagKeys {
		removeTags[k] = ""
	}

	if err := h.Backend.UpdateTagsForResource(ctx, resourceARN, addTags, removeTags); err != nil {
		return nil, err
	}

	return &updateTagsForResourceResponse{
		Xmlns:            ebXMLNS,
		ResponseMetadata: responseMetadata{RequestID: "eb-update-tags"},
	}, nil
}
