package servicequotas

import (
	"context"
	"maps"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	maxTags        = 50
	maxTagKeyLen   = 128
	maxTagValueLen = 256
	arnFields      = 6
)

func validateTags(list []tags.KV) (map[string]string, error) {
	out := make(map[string]string, len(list))

	for _, t := range list {
		if t.Key == "" || len(t.Key) > maxTagKeyLen || len(t.Value) > maxTagValueLen ||
			strings.HasPrefix(strings.ToLower(t.Key), "aws:") {
			return nil, fail(ErrIllegalArgument, "tag %q is invalid", t.Key)
		}

		out[t.Key] = t.Value
	}

	return out, nil
}

func (b *InMemoryBackend) checkQuotaARNLocked(s scope, resource string) error {
	parts := strings.SplitN(resource, ":", arnFields)
	if len(parts) != arnFields || parts[0] != "arn" || parts[2] != serviceName {
		return fail(ErrIllegalArgument, "%q is not a Service Quotas ARN", resource)
	}

	svcCode, quotaCode, ok := strings.Cut(parts[5], "/")
	if !ok || parts[3] != s.region || parts[4] != s.account {
		return fail(ErrNoSuchResource, "The resource %q was not found", resource)
	}

	if _, _, found := b.catalog.Quota(svcCode, quotaCode); !found {
		return fail(ErrNoSuchResource, "The resource %q was not found", resource)
	}

	return nil
}

// TagResource adds or replaces tags on an applied quota.
func (b *InMemoryBackend) TagResource(ctx context.Context, resource string, add map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if err := b.checkQuotaARNLocked(b.scopeOf(ctx), resource); err != nil {
		return err
	}

	rt, ok := b.resourceTags.Get(resource)
	merged := map[string]string{}

	if ok {
		merged = maps.Clone(rt.Tags)
	}

	maps.Copy(merged, add)

	if len(merged) > maxTags {
		return fail(ErrTooManyTags, "a resource can have at most %d tags", maxTags)
	}

	b.resourceTags.Put(&ResourceTags{ARN: resource, Tags: merged})

	return nil
}

// UntagResource removes tags by key.
func (b *InMemoryBackend) UntagResource(ctx context.Context, resource string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if err := b.checkQuotaARNLocked(b.scopeOf(ctx), resource); err != nil {
		return err
	}

	rt, ok := b.resourceTags.Get(resource)
	if !ok {
		return nil
	}

	for _, k := range keys {
		delete(rt.Tags, k)
	}

	if len(rt.Tags) == 0 {
		b.resourceTags.Delete(resource)
	}

	return nil
}

// ListTagsForResource returns the tags of an applied quota.
func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resource string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if err := b.checkQuotaARNLocked(b.scopeOf(ctx), resource); err != nil {
		return nil, err
	}

	if rt, ok := b.resourceTags.Get(resource); ok {
		return maps.Clone(rt.Tags), nil
	}

	return map[string]string{}, nil
}
