package redshift

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strings"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type redshiftTaggedResource struct {
	Tag          svcTags.KV `xml:"Tag"`
	ResourceName string     `xml:"ResourceName"`
	ResourceType string     `xml:"ResourceType"`
}

// handleDescribeTags returns tagged resources, optionally filtered by ResourceName,
// ResourceType, TagKey, and TagValue. Real AWS DescribeTags supports these filters.
func (h *Handler) handleDescribeTags(vals url.Values) (any, error) {
	resourceName := vals.Get("ResourceName")
	resourceType := vals.Get("ResourceType")
	tagKey := vals.Get("TagKey")
	tagValue := vals.Get("TagValue")

	allTags := h.Backend.DescribeTags()

	type describeTagsResult struct {
		XMLName         xml.Name                 `xml:"DescribeTagsResult"`
		Marker          string                   `xml:"Marker,omitempty"`
		TaggedResources []redshiftTaggedResource `xml:"TaggedResources>TaggedResource,omitempty"`
	}
	type response struct {
		XMLName            xml.Name           `xml:"DescribeTagsResponse"`
		Xmlns              string             `xml:"xmlns,attr"`
		DescribeTagsResult describeTagsResult `xml:"DescribeTagsResult"`
	}

	// ResourceType filter: only "cluster" resources are currently stored.
	if resourceType != "" && resourceType != keyResourceCluster {
		return &response{Xmlns: redshiftXMLNS}, nil
	}

	var resources []redshiftTaggedResource

	for clusterID, tags := range allTags {
		if resourceName != "" {
			// Accept exact cluster-ID match or ARN suffix match.
			if clusterID != resourceName && !strings.HasSuffix(resourceName, ":cluster:"+clusterID) {
				continue
			}
		}

		for k, v := range tags {
			if tagKey != "" && k != tagKey {
				continue
			}
			if tagValue != "" && v != tagValue {
				continue
			}

			resources = append(resources, redshiftTaggedResource{
				Tag:          svcTags.KV{Key: k, Value: v},
				ResourceName: clusterID,
				ResourceType: keyResourceCluster,
			})
		}
	}

	return &response{
		Xmlns: redshiftXMLNS,
		DescribeTagsResult: describeTagsResult{
			TaggedResources: resources,
		},
	}, nil
}

func (h *Handler) handleCreateTags(vals url.Values) (any, error) {
	clusterID := vals.Get("ResourceName")
	tags := parseRedshiftTags(vals)

	if err := h.Backend.CreateTags(clusterID, tags); err != nil {
		return nil, err
	}

	type response struct {
		XMLName xml.Name `xml:"CreateTagsResponse"`
		Xmlns   string   `xml:"xmlns,attr"`
	}

	return &response{Xmlns: redshiftXMLNS}, nil
}

func (h *Handler) handleDeleteTags(vals url.Values) (any, error) {
	clusterID := vals.Get("ResourceName")
	keys := parseRedshiftTagKeys(vals)

	if err := h.Backend.DeleteTags(clusterID, keys); err != nil {
		return nil, err
	}

	type response struct {
		XMLName xml.Name `xml:"DeleteTagsResponse"`
		Xmlns   string   `xml:"xmlns,attr"`
	}

	return &response{Xmlns: redshiftXMLNS}, nil
}

// parseRedshiftTags extracts Tags.Tag.N.Key/Tags.Tag.N.Value from form values.
// At most maxListItems tags are returned to prevent resource exhaustion.
// Returns as soon as an empty key is found (tags are expected to be consecutive).
func parseRedshiftTags(vals url.Values) map[string]string {
	tags := make(map[string]string)

	for i := 1; i <= maxListItems; i++ {
		prefix := fmt.Sprintf("Tags.Tag.%d.", i)
		key := vals.Get(prefix + "Key")

		if key == "" {
			// Tags are 1-indexed and consecutive; first missing key ends iteration.
			return tags
		}

		tags[key] = vals.Get(prefix + "Value")
	}

	// maxListItems exhausted without finding a missing key.
	return tags
}

// parseRedshiftTagKeys extracts TagKeys.TagKey.N from form values.
// At most maxListItems keys are returned to prevent resource exhaustion.
func parseRedshiftTagKeys(vals url.Values) []string {
	var keys []string

	for i := 1; i <= maxListItems; i++ {
		key := vals.Get(fmt.Sprintf("TagKeys.TagKey.%d", i))
		if key == "" {
			return keys
		}

		keys = append(keys, key)
	}

	return keys
}

const maxListItems = 1000

// parseStringList extracts a numbered list from form values using the given prefix.
// e.g. prefix="SnapshotIdentifierList.SnapshotIdentifier." yields elements at indices 1, 2, ...
// At most maxListItems items are returned to prevent resource exhaustion.
func parseStringList(vals url.Values, prefix string) []string {
	var result []string

	for i := 1; i <= maxListItems; i++ {
		v := vals.Get(fmt.Sprintf("%s%d", prefix, i))
		if v == "" {
			return result
		}

		result = append(result, v)
	}

	return result
}
