package mediapackage

import (
	"maps"
)

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if _, ok := b.tags[resourceARN]; !ok {
		b.tags[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceARN], tags)

	// Keep resource-level Tags fields in sync so Describe* responses reflect tag updates.
	if ch := b.findChannelByARN(resourceARN); ch != nil {
		if ch.Tags == nil {
			ch.Tags = make(map[string]string)
		}

		maps.Copy(ch.Tags, tags)
	} else if ep := b.findOriginEndpointByARN(resourceARN); ep != nil {
		if ep.Tags == nil {
			ep.Tags = make(map[string]string)
		}

		maps.Copy(ep.Tags, tags)
	}

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if existing, ok := b.tags[resourceARN]; ok {
		for _, k := range keys {
			delete(existing, k)
		}
	}

	// Keep resource-level Tags fields in sync so Describe* responses reflect tag removals.
	if ch := b.findChannelByARN(resourceARN); ch != nil {
		for _, k := range keys {
			delete(ch.Tags, k)
		}
	} else if ep := b.findOriginEndpointByARN(resourceARN); ep != nil {
		for _, k := range keys {
			delete(ep.Tags, k)
		}
	}

	return nil
}

// findChannelByARN returns the channel with the given ARN, or nil. Must be called with lock held.
func (b *InMemoryBackend) findChannelByARN(resourceARN string) *storedChannel {
	var found *storedChannel

	b.channels.Range(func(ch *storedChannel) bool {
		if ch.ARN == resourceARN {
			found = ch

			return false
		}

		return true
	})

	return found
}

// findOriginEndpointByARN returns the origin endpoint with the given ARN, or nil. Must be called with lock held.
func (b *InMemoryBackend) findOriginEndpointByARN(resourceARN string) *storedOriginEndpoint {
	var found *storedOriginEndpoint

	b.originEndpoints.Range(func(ep *storedOriginEndpoint) bool {
		if ep.ARN == resourceARN {
			found = ep

			return false
		}

		return true
	})

	return found
}

// ListTagsForResource returns all tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	result := make(map[string]string)

	if existing, ok := b.tags[resourceARN]; ok {
		maps.Copy(result, existing)
	}

	return result, nil
}
