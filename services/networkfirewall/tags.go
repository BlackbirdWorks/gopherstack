package networkfirewall

import (
	"context"
	"maps"
	"slices"
	"time"
)

// TagResourceInput is the TagResource request.
type TagResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
	Tags        []Tag  `json:"Tags"`
}

// UntagResourceInput is the UntagResource request.
type UntagResourceInput struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

// ListTagsForResourceInput is the ListTagsForResource request.
type ListTagsForResourceInput struct {
	MaxResults  *int32 `json:"MaxResults"`
	ResourceArn string `json:"ResourceArn"`
	NextToken   string `json:"NextToken"`
}

// ListTagsForResourceOutput is the ListTagsForResource response.
type ListTagsForResourceOutput struct {
	NextToken string `json:"NextToken,omitempty"`
	Tags      []Tag  `json:"Tags"`
}

// tagHandle reads and replaces the tag map of one resource.
type tagHandle struct {
	get func() map[string]string
	set func(map[string]string)
}

func (b *InMemoryBackend) tagHandleLocked(resourceARN string) (tagHandle, error) {
	if r, ok := b.firewalls.Get(resourceARN); ok && !r.expired(time.Now()) {
		return tagHandle{func() map[string]string { return r.Tags }, func(m map[string]string) { r.Tags = m }}, nil
	}

	if r, ok := b.policies.Get(resourceARN); ok {
		return tagHandle{func() map[string]string { return r.Tags }, func(m map[string]string) { r.Tags = m }}, nil
	}

	if r, ok := b.ruleGroups.Get(resourceARN); ok {
		return tagHandle{func() map[string]string { return r.Tags }, func(m map[string]string) { r.Tags = m }}, nil
	}

	if r, ok := b.tlsConfigs.Get(resourceARN); ok {
		return tagHandle{func() map[string]string { return r.Tags }, func(m map[string]string) { r.Tags = m }}, nil
	}

	if h, ok := b.extraTagHandleLocked(resourceARN); ok {
		return h, nil
	}

	return tagHandle{}, notFound("resource", resourceARN)
}

// TagResource adds or overwrites tags on a resource.
func (b *InMemoryBackend) TagResource(_ context.Context, in *TagResourceInput) (*EmptyOutput, error) {
	if len(in.Tags) == 0 {
		return nil, invalid("Tags is required")
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	h, err := b.tagHandleLocked(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	merged := maps.Clone(h.get())
	if merged == nil {
		merged = map[string]string{}
	}

	maps.Copy(merged, tagMap(in.Tags))

	if err = validateTags(merged); err != nil {
		return nil, err
	}

	h.set(merged)

	return &EmptyOutput{}, nil
}

// UntagResource removes tags by key.
func (b *InMemoryBackend) UntagResource(_ context.Context, in *UntagResourceInput) (*EmptyOutput, error) {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	h, err := b.tagHandleLocked(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	kept := maps.Clone(h.get())
	for _, k := range in.TagKeys {
		delete(kept, k)
	}

	h.set(kept)

	return &EmptyOutput{}, nil
}

// ListTagsForResource lists a resource's tags.
func (b *InMemoryBackend) ListTagsForResource(
	_ context.Context, in *ListTagsForResourceInput,
) (*ListTagsForResourceOutput, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	h, err := b.tagHandleLocked(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	all := slices.Clone(tagList(h.get()))

	pg, err := paginate(all, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	if pg.Data == nil {
		pg.Data = []Tag{}
	}

	return &ListTagsForResourceOutput{Tags: pg.Data, NextToken: pg.Next}, nil
}

func newTagHandle[V any](rec *V, get func(*V) *map[string]string) tagHandle {
	p := get(rec)

	return tagHandle{func() map[string]string { return *p }, func(m map[string]string) { *p = m }}
}

func (b *InMemoryBackend) extraTagHandleLocked(resourceARN string) (tagHandle, bool) {
	now := time.Now()

	if r, ok := b.proxies.Get(resourceARN); ok && !r.expired(now) {
		return newTagHandle(r, func(x *proxyRecord) *map[string]string { return &x.Tags }), true
	}

	if r, ok := b.proxyConfigs.Get(resourceARN); ok {
		return newTagHandle(r, func(x *proxyConfigRecord) *map[string]string { return &x.Tags }), true
	}

	if r, ok := b.proxyRuleGroups.Get(resourceARN); ok {
		return newTagHandle(r, func(x *proxyRuleGroupRecord) *map[string]string { return &x.Tags }), true
	}

	if r, ok := b.vpcEndpoints.Get(resourceARN); ok && !r.expired(now) {
		return newTagHandle(r, func(x *vpcEndpointRecord) *map[string]string { return &x.Tags }), true
	}

	if r, ok := b.containers.Get(resourceARN); ok && !r.expired(now) {
		return newTagHandle(r, func(x *containerRecord) *map[string]string { return &x.Tags }), true
	}

	return tagHandle{}, false
}
