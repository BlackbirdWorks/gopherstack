package managedblockchain

import "maps"

// ListTagsForResource returns tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	res, ok := b.arnToResource[resourceARN]
	if !ok {
		return nil, ErrResourceNotFound
	}

	switch r := res.(type) {
	case *Network:
		result := make(map[string]string, len(r.Tags))
		maps.Copy(result, r.Tags)

		return result, nil
	case *Member:
		result := make(map[string]string, len(r.Tags))
		maps.Copy(result, r.Tags)

		return result, nil
	case *Node:
		result := make(map[string]string, len(r.Tags))
		maps.Copy(result, r.Tags)

		return result, nil
	case *Accessor:
		result := make(map[string]string, len(r.Tags))
		maps.Copy(result, r.Tags)

		return result, nil
	}

	return nil, ErrResourceNotFound
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	res, ok := b.arnToResource[resourceARN]
	if !ok {
		return ErrResourceNotFound
	}

	switch r := res.(type) {
	case *Network:
		if r.Tags == nil {
			r.Tags = make(map[string]string)
		}

		maps.Copy(r.Tags, tags)

		return nil
	case *Member:
		if r.Tags == nil {
			r.Tags = make(map[string]string)
		}

		maps.Copy(r.Tags, tags)

		return nil
	case *Node:
		if r.Tags == nil {
			r.Tags = make(map[string]string)
		}

		maps.Copy(r.Tags, tags)

		return nil
	case *Accessor:
		if r.Tags == nil {
			r.Tags = make(map[string]string)
		}

		maps.Copy(r.Tags, tags)

		return nil
	}

	return ErrResourceNotFound
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	res, ok := b.arnToResource[resourceARN]
	if !ok {
		return ErrResourceNotFound
	}

	switch r := res.(type) {
	case *Network:
		for _, k := range tagKeys {
			delete(r.Tags, k)
		}

		return nil
	case *Member:
		for _, k := range tagKeys {
			delete(r.Tags, k)
		}

		return nil
	case *Node:
		for _, k := range tagKeys {
			delete(r.Tags, k)
		}

		return nil
	case *Accessor:
		for _, k := range tagKeys {
			delete(r.Tags, k)
		}

		return nil
	}

	return ErrResourceNotFound
}
