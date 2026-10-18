package globalaccelerator

import (
	"maps"
	"strings"
)

func (b *InMemoryBackend) tagsOfLocked(arn string) (*map[string]string, error) {
	switch {
	case strings.Contains(arn, ":accelerator/") && !strings.Contains(arn, "/listener/"):
		a, err := b.acceleratorAnyLocked(arn)
		if err != nil {
			return nil, err
		}

		return &a.Tags, nil
	case strings.Contains(arn, ":attachment/"):
		a, err := b.attachmentLocked(arn)
		if err != nil {
			return nil, err
		}

		return &a.Tags, nil
	default:
		return nil, fail(ErrInvalidArgument, "%q is not a taggable Global Accelerator resource ARN", arn)
	}
}

// TagResource adds or replaces tags on an accelerator or attachment.
func (b *InMemoryBackend) TagResource(arn string, add map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	t, err := b.tagsOfLocked(arn)
	if err != nil {
		return err
	}

	merged := maps.Clone(*t)
	if merged == nil {
		merged = make(map[string]string, len(add))
	}

	maps.Copy(merged, add)

	if len(merged) > maxTags {
		return fail(ErrLimitExceeded, "a resource can have at most %d tags", maxTags)
	}

	*t = merged

	return nil
}

// UntagResource removes tags by key.
func (b *InMemoryBackend) UntagResource(arn string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	t, err := b.tagsOfLocked(arn)
	if err != nil {
		return err
	}

	for _, k := range keys {
		delete(*t, k)
	}

	return nil
}

// ListTagsForResource returns the tags of an accelerator or attachment.
func (b *InMemoryBackend) ListTagsForResource(arn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t, err := b.tagsOfLocked(arn)
	if err != nil {
		return nil, err
	}

	return maps.Clone(*t), nil
}
