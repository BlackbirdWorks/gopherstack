package oam

import (
	"context"
	"maps"
	"strings"
)

const (
	maxTags        = 50
	maxTagKeyLen   = 128
	maxTagValueLen = 256
)

func validateTags(m map[string]string) error {
	if len(m) > maxTags {
		return fail(ErrTooManyTags, "a resource can have at most %d tags", maxTags)
	}

	for k, v := range m {
		if k == "" || len(k) > maxTagKeyLen || len(v) > maxTagValueLen ||
			strings.HasPrefix(strings.ToLower(k), "aws:") {
			return fail(ErrInvalidParameter, "tag %q is invalid", k)
		}
	}

	return nil
}

// tagsOfLocked returns a pointer to the tag map of the sink or link named by resource.
func (b *InMemoryBackend) tagsOfLocked(s scope, resource string) (*map[string]string, error) {
	parts := strings.SplitN(resource, ":", arnFields)
	if len(parts) != arnFields || parts[0] != "arn" || parts[2] != serviceName {
		return nil, fail(ErrInvalidParameter, "%q is not an OAM resource ARN", resource)
	}

	if sink, ok := b.sinks.Get(resource); ok && sink.Account == s.account && sink.Region == s.region {
		return &sink.Tags, nil
	}

	if link, ok := b.links.Get(resource); ok && link.Account == s.account && link.Region == s.region {
		return &link.Tags, nil
	}

	return nil, fail(ErrResourceNotFound, "The resource %q was not found", resource)
}

// TagResource adds or replaces tags on a sink or link.
func (b *InMemoryBackend) TagResource(ctx context.Context, resource string, add map[string]string) error {
	if err := validateTags(add); err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	t, err := b.tagsOfLocked(b.scopeOf(ctx), resource)
	if err != nil {
		return err
	}

	merged := maps.Clone(*t)
	if merged == nil {
		merged = make(map[string]string, len(add))
	}

	maps.Copy(merged, add)

	if len(merged) > maxTags {
		return fail(ErrTooManyTags, "a resource can have at most %d tags", maxTags)
	}

	*t = merged

	return nil
}

// UntagResource removes tags by key.
func (b *InMemoryBackend) UntagResource(ctx context.Context, resource string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	t, err := b.tagsOfLocked(b.scopeOf(ctx), resource)
	if err != nil {
		return err
	}

	for _, k := range keys {
		delete(*t, k)
	}

	return nil
}

// ListTagsForResource returns the tags of a sink or link.
func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resource string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t, err := b.tagsOfLocked(b.scopeOf(ctx), resource)
	if err != nil {
		return nil, err
	}

	out := maps.Clone(*t)
	if out == nil {
		out = map[string]string{}
	}

	return out, nil
}
