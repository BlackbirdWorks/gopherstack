package fis

import (
	"fmt"
	"maps"
	"strings"
)

// ----------------------------------------
// Tag operations
// ----------------------------------------

// ListTagsForResource returns tags for a resource identified by its ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if b.safetyLever != nil && b.safetyLever.Arn == resourceARN {
		return copyStringMap(b.safetyLever.Tags), nil
	}

	if tpl, ok := b.templateByArn(resourceARN); ok {
		return copyStringMap(tpl.Tags), nil
	}

	if exp, ok := b.experimentByArn(resourceARN); ok {
		return copyStringMap(exp.Tags), nil
	}

	return nil, fmt.Errorf("%w: %s", ErrResourceNotFound, resourceARN)
}

// validateTags checks tag key/value constraints per AWS limits.
func validateTags(tags map[string]string) error {
	for k, v := range tags {
		if len(k) == 0 || len(k) > maxTagKeyLen {
			return fmt.Errorf(
				"%w: tag key %q: length must be 1-%d characters",
				ErrValidation, k, maxTagKeyLen,
			)
		}

		if strings.HasPrefix(k, "aws:") {
			return fmt.Errorf("%w: tag key %q: keys with prefix \"aws:\" are reserved", ErrValidation, k)
		}

		if len(v) > maxTagValueLen {
			return fmt.Errorf(
				"%w: tag key %q: value length must be 0-%d characters",
				ErrValidation, k, maxTagValueLen,
			)
		}
	}

	return nil
}

// applyTags merges newTags into dest, enforcing the per-resource quota.
// dest must be non-nil. Returns ErrTooManyTags if the quota would be exceeded.
func applyTags(dest *map[string]string, newTags map[string]string, resourceARN string) error {
	if *dest == nil {
		*dest = make(map[string]string)
	}

	if len(*dest)+len(newTags) > maxTagsPerResource {
		return fmt.Errorf(
			"%w: resource %s already has the maximum of %d tags",
			ErrTooManyTags, resourceARN, maxTagsPerResource,
		)
	}

	maps.Copy(*dest, newTags)

	return nil
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	if err := validateTags(tags); err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	return b.applyTagsLocked(resourceARN, tags)
}

// applyTagsLocked merges tags into the resource identified by resourceARN.
// Must be called with b.mu held for write.
func (b *InMemoryBackend) applyTagsLocked(resourceARN string, tags map[string]string) error {
	if b.safetyLever != nil && b.safetyLever.Arn == resourceARN {
		return applyTags(&b.safetyLever.Tags, tags, resourceARN)
	}

	if tpl, ok := b.templateByArn(resourceARN); ok {
		return applyTags(&tpl.Tags, tags, resourceARN)
	}

	if exp, ok := b.experimentByArn(resourceARN); ok {
		return applyTags(&exp.Tags, tags, resourceARN)
	}

	return fmt.Errorf("%w: %s", ErrResourceNotFound, resourceARN)
}

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every FIS resource ARN (safety lever, experiment templates,
// experiments) that currently has at least one tag applied via TagResource.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	if b.safetyLever != nil && len(b.safetyLever.Tags) > 0 {
		out = append(out, TaggedEntry{ARN: b.safetyLever.Arn, Tags: copyStringMap(b.safetyLever.Tags)})
	}

	b.templates.Range(func(tpl *ExperimentTemplate) bool {
		if len(tpl.Tags) > 0 {
			out = append(out, TaggedEntry{ARN: tpl.Arn, Tags: copyStringMap(tpl.Tags)})
		}

		return true
	})

	b.experiments.Range(func(exp *Experiment) bool {
		if len(exp.Tags) > 0 {
			out = append(out, TaggedEntry{ARN: exp.Arn, Tags: copyStringMap(exp.Tags)})
		}

		return true
	})

	return out
}

// UntagResource removes specific tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if b.safetyLever != nil && b.safetyLever.Arn == resourceARN {
		for _, k := range keys {
			delete(b.safetyLever.Tags, k)
		}

		return nil
	}

	if tpl, ok := b.templateByArn(resourceARN); ok {
		for _, k := range keys {
			delete(tpl.Tags, k)
		}

		return nil
	}

	if exp, ok := b.experimentByArn(resourceARN); ok {
		for _, k := range keys {
			delete(exp.Tags, k)
		}

		return nil
	}

	return fmt.Errorf("%w: %s", ErrResourceNotFound, resourceARN)
}
