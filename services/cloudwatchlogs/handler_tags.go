package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"maps"

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
