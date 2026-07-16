package stepfunctions

import (
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type sfnListTagsForResourceInput struct {
	ResourceArn string `json:"resourceArn"`
}

type sfnTagResourceInput struct {
	Tags        *tags.Tags `json:"tags"`
	ResourceArn string     `json:"resourceArn"`
}

type sfnUntagResourceInput struct {
	ResourceArn string   `json:"resourceArn"`
	TagKeys     []string `json:"tagKeys"`
}

type sfnTagEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type listTagsForResourceOutput struct {
	Tags []sfnTagEntry `json:"tags"`
}

type tagResourceOutput struct{}

type untagResourceOutput struct{}

const (
	maxTagKeyLen       = 128
	maxTagValueLen     = 256
	maxTagsPerResource = 50
)

// validateTags checks AWS tag constraints: key 1-128 chars, value 0-256 chars,
// and that adding newTags will not push the resource over 50 total tags.
func validateTags(existing, newTags map[string]string) error {
	for k, v := range newTags {
		if len(k) == 0 || len(k) > maxTagKeyLen {
			return fmt.Errorf(
				"%w: tag key must be 1-%d characters",
				ErrTagPolicyViolation,
				maxTagKeyLen,
			)
		}

		if len(v) > maxTagValueLen {
			return fmt.Errorf(
				"%w: tag value must be 0-%d characters",
				ErrTagPolicyViolation,
				maxTagValueLen,
			)
		}
	}

	merged := len(existing)
	for k := range newTags {
		if _, exists := existing[k]; !exists {
			merged++
		}
	}

	if merged > maxTagsPerResource {
		return fmt.Errorf(
			"%w: resource cannot have more than %d tags",
			ErrTagPolicyViolation,
			maxTagsPerResource,
		)
	}

	return nil
}

// stateMachineTagActions returns tag-related actions for state machines.
func (h *Handler) stateMachineTagActions() map[string]actionFn {
	return map[string]actionFn{
		"ListTagsForResource": func(b []byte) (any, error) {
			var input sfnListTagsForResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			tagMap := h.getTags(input.ResourceArn)
			tagList := make([]sfnTagEntry, 0, len(tagMap))
			for k, v := range tagMap {
				tagList = append(tagList, sfnTagEntry{Key: k, Value: v})
			}

			return &listTagsForResourceOutput{Tags: tagList}, nil
		},
		"TagResource": func(b []byte) (any, error) {
			var input sfnTagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			var kv map[string]string
			if input.Tags != nil {
				kv = input.Tags.Clone()
			}

			existing := h.getTags(input.ResourceArn)
			if err := validateTags(existing, kv); err != nil {
				return nil, err
			}

			h.setTags(input.ResourceArn, kv)

			return &tagResourceOutput{}, nil
		},
		"UntagResource": func(b []byte) (any, error) {
			var input sfnUntagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			h.removeTags(input.ResourceArn, input.TagKeys)

			return &untagResourceOutput{}, nil
		},
	}
}
