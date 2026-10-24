package codegurureviewer

import (
	"context"
	"maps"
	"strings"
)

func (b *InMemoryBackend) taggableLocked(arn string) (*associationRecord, error) {
	a, ok := b.associations.Get(arn)
	if !validAssociationARN(arn) || !ok {
		return nil, resourceNotFoundErr("resource %q does not exist or cannot be tagged", arn)
	}

	return a, nil
}

// TagResource adds or replaces tags on a repository association.
func (b *InMemoryBackend) TagResource(arn string, add map[string]string) error {
	if err := checkTags(add); err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	a, err := b.taggableLocked(arn)
	if err != nil {
		return err
	}

	merged := maps.Clone(a.Tags)
	if merged == nil {
		merged = make(map[string]string, len(add))
	}

	maps.Copy(merged, add)

	if err = checkTags(merged); err != nil {
		return err
	}

	a.Tags = merged

	return nil
}

// UntagResource removes tags by key.
func (b *InMemoryBackend) UntagResource(arn string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	a, err := b.taggableLocked(arn)
	if err != nil {
		return err
	}

	for _, k := range keys {
		delete(a.Tags, k)
	}

	return nil
}

// ListTagsForResource returns a copy of the association's tags.
func (b *InMemoryBackend) ListTagsForResource(arn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	a, err := b.taggableLocked(arn)
	if err != nil {
		return nil, err
	}

	out := maps.Clone(a.Tags)
	if out == nil {
		out = map[string]string{}
	}

	return out, nil
}

// TaggedEntry is one taggable resource, for the Resource Groups Tagging API bridge.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources lists the caller's tagged repository associations.
func (b *InMemoryBackend) TaggedResources(ctx context.Context) []TaggedEntry {
	account, region := b.scope(ctx)
	owner := ":" + region + ":" + account + ":"

	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	for _, a := range b.associations.All() {
		if len(a.Tags) > 0 && strings.Contains(a.ARN, owner) {
			out = append(out, TaggedEntry{ARN: a.ARN, Tags: maps.Clone(a.Tags)})
		}
	}

	return out
}
