package iam

import (
	"cmp"
	"encoding/xml"
	"fmt"
	"maps"
	"net/url"
	"slices"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func (h *Handler) iamTagDispatchTable() map[string]iamActionFn {
	table := make(map[string]iamActionFn)
	maps.Copy(table, h.iamListTagActions())
	maps.Copy(table, h.iamMutateTagActions())

	return table
}

// resourceTagDispatch returns the List<kind>Tags / Tag<kind> / Untag<kind>
// dispatch entries shared by every simple key-value-tagged resource (instance
// profiles, OIDC providers, SAML providers, server certificates, ...): a
// resource identified by the request parameter paramName, whose tags are
// stored under the Handler-level tag map keyed by tagPrefix+id. Consolidating
// this pattern into one generic helper avoids near-identical dispatch
// functions for every such resource kind.
func (h *Handler) resourceTagDispatch(kind, tagPrefix, paramName string) map[string]iamActionFn {
	return map[string]iamActionFn{
		"List" + kind + "Tags": func(vals url.Values, reqID string) (any, error) {
			id := vals.Get(paramName)
			members := tagsMapToKV(h.getTags(tagPrefix + id))

			return &iamListTagsResponse{
				XMLName: xml.Name{Local: "List" + kind + "TagsResponse"},
				Xmlns:   iamXMLNS,
				Result: iamListTagsResult{
					XMLName: xml.Name{Local: "List" + kind + "TagsResult"},
					Tags:    members,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"Tag" + kind: func(vals url.Values, reqID string) (any, error) {
			h.setTags(tagPrefix+vals.Get(paramName), parseIAMTags(vals))

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "Tag" + kind + "Response"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"Untag" + kind: func(vals url.Values, reqID string) (any, error) {
			h.removeTags(tagPrefix+vals.Get(paramName), parseIAMTagKeys(vals))

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "Untag" + kind + "Response"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamListTagActions returns the List*Tags dispatch entries.
func (h *Handler) iamListTagActions() map[string]iamActionFn {
	return map[string]iamActionFn{
		"ListRoleTags": func(vals url.Values, reqID string) (any, error) {
			r, err := h.Backend.GetRole(vals.Get("RoleName"))
			if err != nil {
				return nil, err
			}

			members := tagsMapToKV(r.Tags)

			return &iamListTagsResponse{
				XMLName:          xml.Name{Local: "ListRoleTagsResponse"},
				Xmlns:            iamXMLNS,
				Result:           iamListTagsResult{XMLName: xml.Name{Local: "ListRoleTagsResult"}, Tags: members},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"ListPolicyTags": func(vals url.Values, reqID string) (any, error) {
			p, err := h.Backend.GetPolicy(vals.Get("PolicyArn"))
			if err != nil {
				return nil, err
			}

			members := tagsMapToKV(p.Tags)

			return &iamListTagsResponse{
				XMLName:          xml.Name{Local: "ListPolicyTagsResponse"},
				Xmlns:            iamXMLNS,
				Result:           iamListTagsResult{XMLName: xml.Name{Local: "ListPolicyTagsResult"}, Tags: members},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"ListUserTags": func(vals url.Values, reqID string) (any, error) {
			u, err := h.Backend.GetUser(vals.Get("UserName"))
			if err != nil {
				return nil, err
			}

			members := tagsMapToKV(u.Tags)

			return &iamListTagsResponse{
				XMLName:          xml.Name{Local: "ListUserTagsResponse"},
				Xmlns:            iamXMLNS,
				Result:           iamListTagsResult{XMLName: xml.Name{Local: "ListUserTagsResult"}, Tags: members},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		// Note: real IAM has no ListGroupTags action — Group is not a taggable
		// resource type (types.Group has no Tags field in the SDK).
	}
}

// iamMutateTagActions returns the Tag*/Untag* dispatch entries.
func (h *Handler) iamMutateTagActions() map[string]iamActionFn {
	return map[string]iamActionFn{
		"TagRole": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.TagRole(vals.Get("RoleName"), parseIAMTags(vals)); err != nil {
				return nil, err
			}

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "TagRoleResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UntagRole": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.UntagRole(vals.Get("RoleName"), parseIAMTagKeys(vals)); err != nil {
				return nil, err
			}

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "UntagRoleResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"TagPolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.TagPolicy(vals.Get("PolicyArn"), parseIAMTags(vals)); err != nil {
				return nil, err
			}

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "TagPolicyResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UntagPolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.UntagPolicy(vals.Get("PolicyArn"), parseIAMTagKeys(vals)); err != nil {
				return nil, err
			}

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "UntagPolicyResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"TagUser": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.TagUser(vals.Get("UserName"), parseIAMTags(vals)); err != nil {
				return nil, err
			}

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "TagUserResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UntagUser": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.UntagUser(vals.Get("UserName"), parseIAMTagKeys(vals)); err != nil {
				return nil, err
			}

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "UntagUserResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		// Note: real IAM has no TagGroup/UntagGroup actions — Group is not a
		// taggable resource type (types.Group has no Tags field in the SDK).
	}
}

// tagsMapToKV converts map[string]string to a svcTags.KV slice sorted by key,
// matching every IAM List*Tags operation's documented order (e.g. iam@v1.58.1
// api_op_ListRoleTags.go:14: "The returned list of tags is sorted by tag
// key.").
func tagsMapToKV(tags map[string]string) []svcTags.KV {
	if len(tags) == 0 {
		return nil
	}

	result := make([]svcTags.KV, 0, len(tags))
	for k, v := range tags {
		result = append(result, svcTags.KV{Key: k, Value: v})
	}

	slices.SortFunc(result, func(a, b svcTags.KV) int { return cmp.Compare(a.Key, b.Key) })

	return result
}

// parseIAMTags parses Tags.member.N.Key / Tags.member.N.Value form values.
func parseIAMTags(vals url.Values) map[string]string {
	tags := make(map[string]string)
	for i := 1; ; i++ {
		k := vals.Get(fmt.Sprintf("Tags.member.%d.Key", i))
		if k == "" {
			return tags
		}
		tags[k] = vals.Get(fmt.Sprintf("Tags.member.%d.Value", i))
	}
}

// parseIAMTagKeys parses TagKeys.member.N form values.
func parseIAMTagKeys(vals url.Values) []string {
	var keys []string
	for i := 1; ; i++ {
		k := vals.Get(fmt.Sprintf("TagKeys.member.%d", i))
		if k == "" {
			return keys
		}
		keys = append(keys, k)
	}
}
