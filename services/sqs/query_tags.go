package sqs

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// parseQueryTagMembers parses Query-protocol Tag.N.Key / Tag.N.Value pairs (the
// encoding used by the SQS TagQueue Query API: TagMap is flattened with
// locationName "Tag", key locationName "Key", value locationName "Value" --
// see sqs 2012-11-05 service-2.json). Returns nil if no tags are present so
// callers can leave the backend Tags input nil.
func parseQueryTagMembers(vals url.Values) map[string]string {
	tagMap := make(map[string]string)

	for i := 1; i <= maxParseIterations; i++ {
		key := vals.Get(fmt.Sprintf("Tag.%d.Key", i))
		if key == "" {
			break
		}

		tagMap[key] = vals.Get(fmt.Sprintf("Tag.%d.Value", i))
	}

	if len(tagMap) == 0 {
		return nil
	}

	return tagMap
}

func (h *Handler) queryTagQueue(vals url.Values, region string) ([]byte, int, *queryError) {
	tagMap := parseQueryTagMembers(vals)

	var tagInput *tags.Tags
	if len(tagMap) > 0 {
		tagInput = tags.FromMap("sqs.query.tagqueue", tagMap)
		defer tagInput.Close()
	}

	if err := h.Backend.TagQueue(&TagQueueInput{
		QueueURL: vals.Get("QueueUrl"),
		Region:   region,
		Tags:     tagInput,
	}); err != nil {
		return nil, 0, buildQueryError(err)
	}

	resp := TagQueueResponse{
		Xmlns:            sqsNamespace,
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) queryUntagQueue(vals url.Values, region string) ([]byte, int, *queryError) {
	// TagKeyList is flattened with member locationName "TagKey" (sqs
	// 2012-11-05 service-2.json), not "TagKeys.member".
	tagKeys := parseQueryList(vals, "TagKey")

	if err := h.Backend.UntagQueue(&UntagQueueInput{
		QueueURL: vals.Get("QueueUrl"),
		Region:   region,
		TagKeys:  tagKeys,
	}); err != nil {
		return nil, 0, buildQueryError(err)
	}

	resp := UntagQueueResponse{
		Xmlns:            sqsNamespace,
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) queryListQueueTags(vals url.Values, region string) ([]byte, int, *queryError) {
	out, err := h.Backend.ListQueueTags(&ListQueueTagsInput{
		QueueURL: vals.Get("QueueUrl"),
		Region:   region,
	})
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	var entries []TagEntry
	if out.Tags != nil {
		out.Tags.Range(func(k, v string) bool {
			entries = append(entries, TagEntry{Key: k, Value: v})

			return true
		})
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })

	resp := ListQueueTagsResponse{
		Xmlns:            sqsNamespace,
		Result:           ListQueueTagsResult{Tags: entries},
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}
