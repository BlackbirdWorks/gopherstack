package stepfunctions

import (
	"encoding/json"
	"fmt"
)

type sfnListTagsForResourceInput struct {
	ResourceArn string `json:"resourceArn"`
}

type sfnTagResourceInput struct {
	ResourceArn string        `json:"resourceArn"`
	Tags        []sfnTagEntry `json:"tags"`
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
			ErrTooManyTags,
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

			kv := make(map[string]string, len(input.Tags))
			for _, t := range input.Tags {
				kv[t.Key] = t.Value
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

// TaggedEntry pairs a resource ARN with its tag map, for cross-service tag
// enumeration by the Resource Groups Tagging API (see cli.go's
// wireTaggingStepFunctions). State machines, activities, and state machine
// aliases share the same ARN-keyed h.tags store, so a single flat walk
// covers every taggable Step Functions resource kind.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every Step Functions resource ARN that currently
// has at least one tag.
func (h *Handler) TaggedResources() []TaggedEntry {
	h.tagsMu.RLock("TaggedResources")
	defer h.tagsMu.RUnlock()

	out := make([]TaggedEntry, 0, len(h.tags))

	for arn, t := range h.tags {
		if t == nil || t.Len() == 0 {
			continue
		}

		out = append(out, TaggedEntry{ARN: arn, Tags: t.Clone()})
	}

	return out
}

// TagResourceByARN adds or updates tags on the Step Functions resource
// identified by ARN, enforcing the same tag-policy constraints as the native
// TagResource action (see stateMachineTagActions above). Used by cli.go's
// wireTaggingStepFunctions to let the Resource Groups Tagging API mutate
// Step Functions tags.
func (h *Handler) TagResourceByARN(arn string, kv map[string]string) error {
	existing := h.getTags(arn)
	if err := validateTags(existing, kv); err != nil {
		return err
	}

	h.setTags(arn, kv)

	return nil
}

// UntagResourceByARN removes the given tag keys from the Step Functions
// resource identified by ARN. Used by cli.go's wireTaggingStepFunctions.
func (h *Handler) UntagResourceByARN(arn string, keys []string) error {
	h.removeTags(arn, keys)

	return nil
}
