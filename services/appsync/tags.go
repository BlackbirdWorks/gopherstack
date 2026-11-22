package appsync

import (
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// TagResource adds or updates tags on a GraphQL API (v1) or Api (v2 Event API) — both
// resource kinds are addressed via "arn:...:appsync:...:apis/{apiId}" and are valid
// TagResource targets on the wire.
func (b *InMemoryBackend) TagResource(apiID string, tagMap map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if api, ok := b.apis.Get(apiID); ok {
		if api.Tags == nil {
			api.Tags = tags.New("appsync.api." + apiID + ".tags")
		}

		for k, v := range tagMap {
			api.Tags.Set(k, v)
		}

		return nil
	}

	if eventAPI, ok := b.eventAPIs.Get(apiID); ok {
		if eventAPI.Tags == nil {
			eventAPI.Tags = make(map[string]string, len(tagMap))
		}

		maps.Copy(eventAPI.Tags, tagMap)

		return nil
	}

	return fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
}

// UntagResource removes tags from a GraphQL API (v1) or Api (v2 Event API).
func (b *InMemoryBackend) UntagResource(apiID string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if api, ok := b.apis.Get(apiID); ok {
		if api.Tags != nil {
			api.Tags.DeleteKeys(tagKeys)
		}

		return nil
	}

	if eventAPI, ok := b.eventAPIs.Get(apiID); ok {
		for _, k := range tagKeys {
			delete(eventAPI.Tags, k)
		}

		return nil
	}

	return fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
}

// ListTagsForResource returns all tags on a GraphQL API (v1) or Api (v2 Event API).
func (b *InMemoryBackend) ListTagsForResource(apiID string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if api, ok := b.apis.Get(apiID); ok {
		if api.Tags == nil {
			return map[string]string{}, nil
		}

		return api.Tags.Clone(), nil
	}

	if eventAPI, ok := b.eventAPIs.Get(apiID); ok {
		out := make(map[string]string, len(eventAPI.Tags))
		maps.Copy(out, eventAPI.Tags)

		return out, nil
	}

	return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
}

// TaggedEntry pairs a resource ARN with its tag set, for the Resource Groups
// Tagging API's GetResources (see cli.go's wireTaggingAppSync).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources lists every GraphQL API and event API that carries at least one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	return b.entries(true)
}

// InventoryResources lists every GraphQL API and event API, tagged or not.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(false)
}

func (b *InMemoryBackend) entries(taggedOnly bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	apis := b.apis.All()
	eventAPIs := b.eventAPIs.All()
	b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(apis)+len(eventAPIs))

	for _, api := range apis {
		empty := api.Tags == nil || api.Tags.Len() == 0
		if taggedOnly && empty {
			continue
		}

		entry := TaggedEntry{ARN: api.ARN}
		if !empty {
			entry.Tags = api.Tags.Clone()
		}

		out = append(out, entry)
	}

	for _, eventAPI := range eventAPIs {
		if !taggedOnly || len(eventAPI.Tags) > 0 {
			out = append(out, TaggedEntry{ARN: eventAPI.ARN, Tags: maps.Clone(eventAPI.Tags)})
		}
	}

	return out
}
