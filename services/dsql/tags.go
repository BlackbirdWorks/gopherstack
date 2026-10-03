package dsql

import "maps"

// TagResource adds or replaces tags on a cluster identified by its ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	if err := validateTags(tags); err != nil {
		return err
	}

	identifier, ok := clusterIdentifierFromResourceARN(resourceARN)
	if !ok {
		return ErrValidation
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	c, err := b.resolveClusterLocked(identifier)
	if err != nil {
		return err
	}

	if len(c.Tags)+len(tags) > maxTagsPerResource {
		return ErrValidation
	}

	maps.Copy(c.Tags, tags)

	return nil
}

// UntagResource removes tags from a cluster by key.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	identifier, ok := clusterIdentifierFromResourceARN(resourceARN)
	if !ok {
		return ErrValidation
	}

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	c, err := b.resolveClusterLocked(identifier)
	if err != nil {
		return err
	}

	for _, k := range tagKeys {
		delete(c.Tags, k)
	}

	return nil
}

// ListTagsForResource returns all tags on a cluster.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	identifier, ok := clusterIdentifierFromResourceARN(resourceARN)
	if !ok {
		return nil, ErrValidation
	}

	b.mu.Lock("ListTagsForResource")
	defer b.mu.Unlock()

	c, err := b.resolveClusterLocked(identifier)
	if err != nil {
		return nil, err
	}

	out := make(map[string]string, len(c.Tags))
	maps.Copy(out, c.Tags)

	return out, nil
}
