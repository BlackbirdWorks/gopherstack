package eventbridge

import (
	"context"
	"encoding/json"
	"strings"

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
		"ListTagsForResource": func(ctx context.Context, b []byte) (any, error) {
			var input listTagsForResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.requireTaggable(ctx, input.ResourceARN); err != nil {
				return nil, err
			}
			tagMap := h.getTags(canonicalRuleARN(input.ResourceARN))
			tagList := make([]svcTags.KV, 0, len(tagMap))
			for k, v := range tagMap {
				tagList = append(tagList, svcTags.KV{Key: k, Value: v})
			}

			return &listTagsForResourceOutput{Tags: tagList}, nil
		},
		"TagResource": func(ctx context.Context, b []byte) (any, error) {
			var input tagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.requireTaggable(ctx, input.ResourceARN); err != nil {
				return nil, err
			}
			kv := make(map[string]string, len(input.Tags))
			for _, t := range input.Tags {
				kv[t.Key] = t.Value
			}
			h.setTags(canonicalRuleARN(input.ResourceARN), kv)

			return &tagResourceOutput{}, nil
		},
		"UntagResource": func(ctx context.Context, b []byte) (any, error) {
			var input untagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.requireTaggable(ctx, input.ResourceARN); err != nil {
				return nil, err
			}
			h.removeTags(canonicalRuleARN(input.ResourceARN), input.TagKeys)

			return &untagResourceOutput{}, nil
		},
	}
}

// requireTaggable fails with ResourceNotFoundException when arn names a rule
// or event bus that does not exist; other resource kinds are not checked.
func (h *Handler) requireTaggable(ctx context.Context, resourceARN string) error {
	parts := strings.SplitN(resourceARN, ":", arnPartCount)
	if len(parts) != arnPartCount || parts[2] != servicePrefixEvents {
		return nil
	}

	kind, rest, _ := strings.Cut(parts[5], "/")

	switch kind {
	case "rule":
		bus, name, hasBus := strings.Cut(rest, "/")
		if !hasBus {
			bus, name = defaultEventBusName, bus
		}
		_, err := h.Backend.DescribeRule(ctx, name, bus)

		return err
	case "event-bus":
		_, err := h.Backend.DescribeEventBus(ctx, rest)

		return err
	}

	return nil
}

const arnPartCount = 6
