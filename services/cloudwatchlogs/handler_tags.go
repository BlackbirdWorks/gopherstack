package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"maps"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type listTagsLogGroupInput struct {
	LogGroupName string `json:"logGroupName"`
}

type listTagsForResourceInput struct {
	ResourceArn string `json:"resourceArn"`
}

type tagLogGroupInput struct {
	Tags         *tags.Tags `json:"tags"`
	LogGroupName string     `json:"logGroupName"`
}

type untagLogGroupInput struct {
	LogGroupName string   `json:"logGroupName"`
	Tags         []string `json:"tags"`
}

type tagResourceInput struct {
	Tags        map[string]string `json:"tags"`
	ResourceArn string            `json:"resourceArn"`
}

type untagResourceInput struct {
	ResourceArn string   `json:"resourceArn"`
	TagKeys     []string `json:"tagKeys"`
}

func (h *Handler) setTags(resourceID string, kv map[string]string) {
	h.tagsMu.Lock("setTags")
	defer h.tagsMu.Unlock()
	if h.tags[resourceID] == nil {
		h.tags[resourceID] = tags.New("cwl." + resourceID + ".tags")
	}
	h.tags[resourceID].Merge(kv)
}

func (h *Handler) removeTags(resourceID string, keys []string) {
	h.tagsMu.RLock("removeTags")
	t := h.tags[resourceID]
	h.tagsMu.RUnlock()
	if t != nil {
		t.DeleteKeys(keys)
	}
}

func (h *Handler) getTags(resourceID string) map[string]string {
	h.tagsMu.RLock("getTags")
	t := h.tags[resourceID]
	h.tagsMu.RUnlock()
	if t == nil {
		return map[string]string{}
	}

	return t.Clone()
}

// TagResource adds or updates tags on the resource identified by resourceARN, the
// same store the "TagResource" action (below) writes through h.setTags. Exported for
// cross-service tagging (resourcegroupstaggingapi, wired in cli.go's
// wireTaggingCloudWatchLogs).
func (h *Handler) TagResource(resourceARN string, kv map[string]string) {
	h.setTags(resourceARN, kv)
}

// UntagResource removes tag keys from the resource identified by resourceARN.
func (h *Handler) UntagResource(resourceARN string, keys []string) {
	h.removeTags(resourceARN, keys)
}

// GetTagsForResource returns the tags for the resource identified by resourceARN.
func (h *Handler) GetTagsForResource(resourceARN string) map[string]string {
	return h.getTags(resourceARN)
}

// TaggedEntry pairs a resource ARN with its tag map, for cross-service tag
// enumeration by the Resource Groups Tagging API (see cli.go's
// wireTaggingCloudWatchLogs).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every tagged resource whose h.tags key is itself a full
// ARN (starts with "arn:"). The legacy TagLogGroup/ListTagsLogGroup/UntagLogGroup
// actions key this same map by bare log group name instead (see logTagActions'
// "TagLogGroup" case below), so a log group tagged only through those legacy actions
// has no ARN-keyed entry to report here -- this backend does not unify the two keys
// for the same log group, matching its existing (pre-existing, out of scope here)
// behavior.
func (h *Handler) TaggedResources() []TaggedEntry {
	h.tagsMu.RLock("TaggedResources")
	defer h.tagsMu.RUnlock()

	out := make([]TaggedEntry, 0, len(h.tags))

	for key, t := range h.tags {
		if !strings.HasPrefix(key, "arn:") || t.Len() == 0 {
			continue
		}

		out = append(out, TaggedEntry{ARN: key, Tags: t.Clone()})
	}

	return out
}

type listTagsLogGroupOutput struct {
	Tags map[string]string `json:"tags"`
}

type listTagsForResourceOutput struct {
	Tags map[string]string `json:"tags"`
}

type tagLogGroupOutput struct{}

type untagLogGroupOutput struct{}

type tagResourceOutput struct{}

type untagResourceOutput struct{}

func (h *Handler) logTagActions() map[string]actionFn {
	return map[string]actionFn{
		"ListTagsLogGroup": func(ctx context.Context, b []byte) (any, error) { //nolint:revive // existing issue.
			var input listTagsLogGroupInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return &listTagsLogGroupOutput{Tags: h.getTags(input.LogGroupName)}, nil
		},
		"ListTagsForResource": func(ctx context.Context, b []byte) (any, error) { //nolint:revive // existing issue.
			var input listTagsForResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return &listTagsForResourceOutput{Tags: h.getTags(input.ResourceArn)}, nil
		},
		"TagLogGroup": func(ctx context.Context, b []byte) (any, error) { //nolint:revive // existing issue.
			var input tagLogGroupInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			var kv map[string]string
			if input.Tags != nil {
				kv = input.Tags.Clone()
			}
			h.setTags(input.LogGroupName, kv)

			return &tagLogGroupOutput{}, nil
		},
		"UntagLogGroup": func(ctx context.Context, b []byte) (any, error) { //nolint:revive // existing issue.
			var input untagLogGroupInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			h.removeTags(input.LogGroupName, input.Tags)

			return &untagLogGroupOutput{}, nil
		},
		"TagResource": func(ctx context.Context, b []byte) (any, error) { //nolint:revive // existing issue.
			var input tagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			h.setTags(input.ResourceArn, maps.Clone(input.Tags))

			return &tagResourceOutput{}, nil
		},
		"UntagResource": func(ctx context.Context, b []byte) (any, error) { //nolint:revive // existing issue.
			var input untagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			h.removeTags(input.ResourceArn, input.TagKeys)

			return &untagResourceOutput{}, nil
		},
	}
}
