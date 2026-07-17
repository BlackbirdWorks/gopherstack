package elb

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
	"regexp"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func (h *Handler) handleAddTags(ctx context.Context, vals url.Values) (any, error) {
	names := parseMembers(vals, "LoadBalancerNames.member")
	if len(names) == 0 {
		return nil, fmt.Errorf("%w: at least one LoadBalancerName is required", ErrInvalidParameter)
	}

	kvs := parseTagKVs(vals, "Tags.member")

	for _, kv := range kvs {
		if err := validateTagKey(kv.Key); err != nil {
			return nil, err
		}
	}

	if err := h.Backend.AddTags(ctx, names, kvs); err != nil {
		return nil, err
	}

	return &addTagsResponse{
		Xmlns:            elbXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-addtags"},
	}, nil
}

func (h *Handler) handleDescribeTags(ctx context.Context, vals url.Values) (any, error) {
	names := parseMembers(vals, "LoadBalancerNames.member")
	if len(names) == 0 {
		return nil, fmt.Errorf("%w: at least one LoadBalancerName is required", ErrInvalidParameter)
	}

	tagMap, err := h.Backend.DescribeTags(ctx, names)
	if err != nil {
		return nil, err
	}

	tagDescs := make([]xmlTagDescription, 0, len(names))
	for _, name := range names {
		kvs := tagMap[name]
		xmlKVs := make([]xmlTag, 0, len(kvs))

		for _, kv := range kvs {
			xmlKVs = append(xmlKVs, xmlTag{Key: kv.Key, Value: kv.Value})
		}

		tagDescs = append(tagDescs, xmlTagDescription{
			LoadBalancerName: name,
			Tags:             xmlTagList{Members: xmlKVs},
		})
	}

	return &describeTagsResponse{
		Xmlns: elbXMLNS,
		Result: describeTagsResult{
			TagDescriptions: xmlTagDescriptionList{Members: tagDescs},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-describetags"},
	}, nil
}

func (h *Handler) handleRemoveTags(ctx context.Context, vals url.Values) (any, error) {
	names := parseMembers(vals, "LoadBalancerNames.member")
	if len(names) == 0 {
		return nil, fmt.Errorf("%w: at least one LoadBalancerName is required", ErrInvalidParameter)
	}

	keys := parseTagKeys(vals, "Tags.member")

	if len(keys) == 0 {
		return nil, fmt.Errorf("%w: Tags must not be empty", ErrInvalidParameter)
	}

	if err := h.Backend.RemoveTags(ctx, names, keys); err != nil {
		return nil, err
	}

	return &removeTagsResponse{
		Xmlns:            elbXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-removetags"},
	}, nil
}

// parseTagKVs extracts key-value tag pairs from Tags.member.N.Key/Value form values.
// Uses gap-tolerant scanning.
func parseTagKVs(vals url.Values, prefix string) []tags.KV {
	indexes := collectMemberIndexes(vals, prefix+".")
	result := make([]tags.KV, 0, len(indexes))

	for _, i := range indexes {
		k := vals.Get(fmt.Sprintf("%s.%d.Key", prefix, i))
		if k != "" {
			result = append(result, tags.KV{Key: k, Value: vals.Get(fmt.Sprintf("%s.%d.Value", prefix, i))})
		}
	}

	return result
}

// parseTagKeys extracts tag keys from Tags.member.N.Key form values (for RemoveTags).
// Uses gap-tolerant scanning.
func parseTagKeys(vals url.Values, prefix string) []string {
	indexes := collectMemberIndexes(vals, prefix+".")
	result := make([]string, 0, len(indexes))

	for _, i := range indexes {
		k := vals.Get(fmt.Sprintf("%s.%d.Key", prefix, i))
		if k != "" {
			result = append(result, k)
		}
	}

	return result
}

// tagKeyRe matches valid ELB tag keys.  Keys must not begin with the reserved
// prefixes "aws:", "amazon:", or "elasticloadbalancing:".
var tagKeyRe = regexp.MustCompile(`(?i)^(aws:|amazon:|elasticloadbalancing:)`)

// validateTagKey returns an error if key uses a reserved tag prefix.
func validateTagKey(key string) error {
	if tagKeyRe.MatchString(key) {
		return fmt.Errorf("%w: tag key %q uses a reserved prefix", ErrInvalidParameter, key)
	}

	return nil
}

type xmlTag struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type xmlTagList struct {
	Members []xmlTag `xml:"member"`
}

type xmlTagDescription struct {
	LoadBalancerName string     `xml:"LoadBalancerName"`
	Tags             xmlTagList `xml:"Tags"`
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

type describeTagsResult struct {
	TagDescriptions xmlTagDescriptionList `xml:"TagDescriptions"`
}

type describeTagsResponse struct {
	XMLName          xml.Name            `xml:"DescribeTagsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
	Result           describeTagsResult  `xml:"DescribeTagsResult"`
}

type removeTagsResponse struct {
	Result           struct{}            `xml:"RemoveTagsResult"`
	XMLName          xml.Name            `xml:"RemoveTagsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}
