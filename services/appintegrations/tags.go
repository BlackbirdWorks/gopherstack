package appintegrations

import (
	"context"
	"maps"
	"strings"
)

// tagsOfLocked returns the live tag map holder for an event integration, data integration or application ARN.
func (b *InMemoryBackend) tagsOfLocked(resourceARN string) (*map[string]string, error) {
	switch {
	case strings.Contains(resourceARN, ":event-integration/"):
		for _, e := range b.events.All() {
			if e.EventIntegrationArn == resourceARN {
				return &e.Tags, nil
			}
		}
	case strings.Contains(resourceARN, ":data-integration/"):
		for _, d := range b.dataIntegs.All() {
			if d.Arn == resourceARN {
				return &d.Tags, nil
			}
		}
	case strings.Contains(resourceARN, ":application/"):
		for _, a := range b.apps.All() {
			if a.Arn == resourceARN {
				return &a.Tags, nil
			}
		}
	}

	return nil, notFoundErr("Resource", resourceARN)
}

// TagResource adds or replaces tags.
func (b *InMemoryBackend) TagResource(resourceARN string, add map[string]string) error {
	if err := checkTags(add); err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	t, err := b.tagsOfLocked(resourceARN)
	if err != nil {
		return err
	}

	merged := maps.Clone(*t)
	if merged == nil {
		merged = make(map[string]string, len(add))
	}

	maps.Copy(merged, add)

	if err = checkTags(merged); err != nil {
		return err
	}

	*t = merged

	return nil
}

// UntagResource removes tags by key.
func (b *InMemoryBackend) UntagResource(resourceARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	t, err := b.tagsOfLocked(resourceARN)
	if err != nil {
		return err
	}

	for _, k := range keys {
		delete(*t, k)
	}

	return nil
}

// ListTagsForResource returns a copy of the tags.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t, err := b.tagsOfLocked(resourceARN)
	if err != nil {
		return nil, err
	}

	out := maps.Clone(*t)
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

// TaggedResources lists the caller's tagged event integrations, data integrations and applications.
func (b *InMemoryBackend) TaggedResources(ctx context.Context) []TaggedEntry {
	account, region := b.scope(ctx)
	suffix := ":" + region + ":" + account + ":"

	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	add := func(arn string, tags map[string]string) {
		if len(tags) > 0 && strings.Contains(arn, suffix) {
			out = append(out, TaggedEntry{ARN: arn, Tags: maps.Clone(tags)})
		}
	}

	for _, e := range b.events.All() {
		add(e.EventIntegrationArn, e.Tags)
	}

	for _, d := range b.dataIntegs.All() {
		add(d.Arn, d.Tags)
	}

	for _, a := range b.apps.All() {
		add(a.Arn, a.Tags)
	}

	return out
}
