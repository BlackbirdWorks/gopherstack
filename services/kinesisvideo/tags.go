package kinesisvideo

import "maps"

// TagStream adds or replaces tags on a stream.
func (b *InMemoryBackend) TagStream(name, streamARN string, tags map[string]string) error {
	if err := validateTags(tags); err != nil {
		return err
	}

	b.mu.Lock("TagStream")
	defer b.mu.Unlock()

	s, err := b.resolveStreamLocked(name, streamARN)
	if err != nil {
		return err
	}

	if len(s.Tags)+len(tags) > maxTagsPerStream {
		return ErrValidation
	}

	maps.Copy(s.Tags, tags)

	return nil
}

// UntagStream removes tags from a stream by key.
func (b *InMemoryBackend) UntagStream(name, streamARN string, tagKeys []string) error {
	b.mu.Lock("UntagStream")
	defer b.mu.Unlock()

	s, err := b.resolveStreamLocked(name, streamARN)
	if err != nil {
		return err
	}

	for _, k := range tagKeys {
		delete(s.Tags, k)
	}

	return nil
}

// ListTagsForStream returns all tags on a stream.
func (b *InMemoryBackend) ListTagsForStream(name, streamARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForStream")
	defer b.mu.RUnlock()

	s, err := b.resolveStreamLocked(name, streamARN)
	if err != nil {
		return nil, err
	}

	out := make(map[string]string, len(s.Tags))
	maps.Copy(out, s.Tags)

	return out, nil
}

// TagResource adds or replaces tags on a signaling channel.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	if err := validateTags(tags); err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	c, err := b.resolveChannelLocked("", resourceARN)
	if err != nil {
		return err
	}

	if len(c.Tags)+len(tags) > maxTagsPerStream {
		return ErrValidation
	}

	maps.Copy(c.Tags, tags)

	return nil
}

// UntagResource removes tags from a signaling channel by key.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	c, err := b.resolveChannelLocked("", resourceARN)
	if err != nil {
		return err
	}

	for _, k := range tagKeys {
		delete(c.Tags, k)
	}

	return nil
}

// ListTagsForResource returns all tags on a signaling channel.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	c, err := b.resolveChannelLocked("", resourceARN)
	if err != nil {
		return nil, err
	}

	out := make(map[string]string, len(c.Tags))
	maps.Copy(out, c.Tags)

	return out, nil
}
