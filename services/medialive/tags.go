package medialive

import "maps"

// --- Tag operations ---

// findLiveTags scans items for the one whose ARN matches resourceARN and
// returns its live Tags map (lazily initialized so callers can mutate it in
// place), or ok=false if none matches. Extracted as a generic helper so
// taggableResourceTags -- which tries five different resource tables --
// stays a flat sequence of one-line calls instead of five inlined loops.
func findLiveTags[T any](
	items []*T,
	resourceARN string,
	arnOf func(*T) string,
	tagsOf func(*T) *map[string]string,
) (map[string]string, bool) {
	for _, item := range items {
		if arnOf(item) != resourceARN {
			continue
		}

		tags := tagsOf(item)
		if *tags == nil {
			*tags = make(map[string]string)
		}

		return *tags, true
	}

	return nil, false
}

// taggableResourceTags resolves resourceARN to the live Tags map of the
// underlying resource, for the resource families whose Describe/List
// responses echo tags inline (Channel, Input, InputSecurityGroup,
// Multiplex, InputDevice -- confirmed against the real DescribeXOutput
// shapes). Their Tags field must stay in lockstep with CreateTags/
// DeleteTags/ListTagsForResource instead of drifting behind a second,
// disconnected b.tags[ARN] store (the pre-fix bug: CreateChannel(tags)
// populated ch.Tags but never b.tags, so ListTagsForResource(channelArn)
// always came back empty, and CreateTags(channelArn, ...) never showed up
// in DescribeChannel). The returned map is the actual stored map (not a
// copy) so callers may mutate it directly while holding b.mu. ok is false
// for any ARN that isn't one of these five resource types, in which case
// callers fall back to the legacy per-ARN b.tags store (used by every
// other taggable resource family, e.g. Cluster, SignalMap, Reservation).
func (b *InMemoryBackend) taggableResourceTags(resourceARN string) (map[string]string, bool) {
	if tags, ok := findLiveTags(b.channels.All(), resourceARN,
		func(c *storedChannel) string { return c.ARN },
		func(c *storedChannel) *map[string]string { return &c.Tags }); ok {
		return tags, true
	}

	if tags, ok := findLiveTags(b.inputs.All(), resourceARN,
		func(i *storedInput) string { return i.ARN },
		func(i *storedInput) *map[string]string { return &i.Tags }); ok {
		return tags, true
	}

	if tags, ok := findLiveTags(b.inputSecurityGroups.All(), resourceARN,
		func(g *storedInputSecurityGroup) string { return g.ARN },
		func(g *storedInputSecurityGroup) *map[string]string { return &g.Tags }); ok {
		return tags, true
	}

	if tags, ok := findLiveTags(b.multiplexes.All(), resourceARN,
		func(m *storedMultiplex) string { return m.ARN },
		func(m *storedMultiplex) *map[string]string { return &m.Tags }); ok {
		return tags, true
	}

	if tags, ok := findLiveTags(b.inputDevices.All(), resourceARN,
		func(d *storedInputDevice) string { return d.ARN },
		func(d *storedInputDevice) *map[string]string { return &d.Tags }); ok {
		return tags, true
	}

	return nil, false
}

// CreateTags adds tags to a resource.
func (b *InMemoryBackend) CreateTags(resourceARN string, tags map[string]string) error {
	b.mu.Lock("CreateTags")
	defer b.mu.Unlock()

	if live, ok := b.taggableResourceTags(resourceARN); ok {
		maps.Copy(live, tags)

		return nil
	}

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceARN], tags)

	return nil
}

// DeleteTags removes tag keys from a resource.
func (b *InMemoryBackend) DeleteTags(resourceARN string, tagKeys []string) error {
	b.mu.Lock("DeleteTags")
	defer b.mu.Unlock()

	if live, ok := b.taggableResourceTags(resourceARN); ok {
		for _, k := range tagKeys {
			delete(live, k)
		}

		return nil
	}

	existing := b.tags[resourceARN]
	if existing == nil {
		return nil
	}

	for _, k := range tagKeys {
		delete(existing, k)
	}

	return nil
}

// ListTagsForResource returns all tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if live, ok := b.taggableResourceTags(resourceARN); ok {
		result := make(map[string]string, len(live))
		maps.Copy(result, live)

		return result, nil
	}

	existing := b.tags[resourceARN]
	result := make(map[string]string, len(existing))
	maps.Copy(result, existing)

	return result, nil
}

func copyTags(tags map[string]string) map[string]string {
	if len(tags) == 0 {
		return make(map[string]string)
	}

	result := make(map[string]string, len(tags))
	maps.Copy(result, tags)

	return result
}
