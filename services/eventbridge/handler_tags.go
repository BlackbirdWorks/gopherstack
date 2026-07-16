package eventbridge

import (
	"context"
	"encoding/json"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type listTagsForResourceInput struct {
	ResourceARN string `json:"ResourceARN"`
}

type tagResourceInput struct {
	ResourceARN string       `json:"ResourceARN"`
	Tags        []svcTags.KV `json:"Tags"`
}

type untagResourceInput struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

type listTagsForResourceOutput struct {
	Tags []svcTags.KV `json:"Tags"`
}

type tagResourceOutput struct{}

type untagResourceOutput struct{}

func (h *Handler) tagActions() map[string]actionFn {
	return map[string]actionFn{
		"ListTagsForResource": func(ctx context.Context, b []byte) (any, error) { //nolint:revive // existing issue.
			var input listTagsForResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			tagMap := h.getTags(input.ResourceARN)
			tagList := make([]svcTags.KV, 0, len(tagMap))
			for k, v := range tagMap {
				tagList = append(tagList, svcTags.KV{Key: k, Value: v})
			}

			return &listTagsForResourceOutput{Tags: tagList}, nil
		},
		"TagResource": func(ctx context.Context, b []byte) (any, error) { //nolint:revive // existing issue.
			var input tagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			kv := make(map[string]string, len(input.Tags))
			for _, t := range input.Tags {
				kv[t.Key] = t.Value
			}
			h.setTags(input.ResourceARN, kv)

			return &tagResourceOutput{}, nil
		},
		"UntagResource": func(ctx context.Context, b []byte) (any, error) { //nolint:revive // existing issue.
			var input untagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			h.removeTags(input.ResourceARN, input.TagKeys)

			return &untagResourceOutput{}, nil
		},
	}
}
