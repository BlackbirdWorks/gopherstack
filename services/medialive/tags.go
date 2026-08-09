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

// findNodeTags scans Nodes, which nest inside each Cluster rather than
// living in their own top-level table, for the one whose ARN matches
// resourceARN.
func (b *InMemoryBackend) findNodeTags(resourceARN string) (map[string]string, bool) {
	for _, c := range b.clusters.All() {
		for _, n := range c.Nodes {
			if n.ARN != resourceARN {
				continue
			}

			if n.Tags == nil {
				n.Tags = make(map[string]string)
			}

			return n.Tags, true
		}
	}

	return nil, false
}

// taggableResourceTags resolves resourceARN to the live Tags map of the
// underlying resource, for every resource family whose Describe/List
// responses echo tags inline (confirmed against the real DescribeXOutput
// shapes). Their Tags field must stay in lockstep with CreateTags/
// DeleteTags/ListTagsForResource instead of drifting behind a second,
// disconnected b.tags[ARN] store (the pre-fix bug: CreateChannel(tags)
// populated ch.Tags but never b.tags, so ListTagsForResource(channelArn)
// always came back empty, and CreateTags(channelArn, ...) never showed up
// in DescribeChannel -- the same shape independently found and fixed for
// nine more resource kinds by gopherstack-2mwl's sweep). The returned map
// is the actual stored map (not a copy) so callers may mutate it directly
// while holding b.mu. ok is false for any ARN that isn't one of these
// resource types, in which case callers fall back to the legacy per-ARN
// b.tags store (used by resource families with no inline Tags field on
// their own Describe shape, e.g. SdiSource, Reservation).
func (b *InMemoryBackend) taggableResourceTags(resourceARN string) (map[string]string, bool) {
	finders := []func() (map[string]string, bool){
		func() (map[string]string, bool) {
			return findLiveTags(b.channels.All(), resourceARN,
				func(c *storedChannel) string { return c.ARN },
				func(c *storedChannel) *map[string]string { return &c.Tags })
		},
		func() (map[string]string, bool) {
			return findLiveTags(b.inputs.All(), resourceARN,
				func(i *storedInput) string { return i.ARN },
				func(i *storedInput) *map[string]string { return &i.Tags })
		},
		func() (map[string]string, bool) {
			return findLiveTags(b.inputSecurityGroups.All(), resourceARN,
				func(g *storedInputSecurityGroup) string { return g.ARN },
				func(g *storedInputSecurityGroup) *map[string]string { return &g.Tags })
		},
		func() (map[string]string, bool) {
			return findLiveTags(b.multiplexes.All(), resourceARN,
				func(m *storedMultiplex) string { return m.ARN },
				func(m *storedMultiplex) *map[string]string { return &m.Tags })
		},
		func() (map[string]string, bool) {
			return findLiveTags(b.inputDevices.All(), resourceARN,
				func(d *storedInputDevice) string { return d.ARN },
				func(d *storedInputDevice) *map[string]string { return &d.Tags })
		},
		func() (map[string]string, bool) {
			return findLiveTags(b.channelPlacementGroups.All(), resourceARN,
				func(g *storedChannelPlacementGroup) string { return g.ARN },
				func(g *storedChannelPlacementGroup) *map[string]string { return &g.Tags })
		},
		func() (map[string]string, bool) {
			return findLiveTags(b.clusters.All(), resourceARN,
				func(c *storedCluster) string { return c.ARN },
				func(c *storedCluster) *map[string]string { return &c.Tags })
		},
		func() (map[string]string, bool) { return b.findNodeTags(resourceARN) },
		func() (map[string]string, bool) {
			return findLiveTags(b.networks.All(), resourceARN,
				func(n *storedNetwork) string { return n.ARN },
				func(n *storedNetwork) *map[string]string { return &n.Tags })
		},
		func() (map[string]string, bool) {
			return findLiveTags(b.signalMaps.All(), resourceARN,
				func(s *storedSignalMap) string { return s.Arn },
				func(s *storedSignalMap) *map[string]string { return &s.Tags })
		},
		func() (map[string]string, bool) {
			return findLiveTags(b.cwAlarmTemplateGroups.All(), resourceARN,
				func(g *storedCloudWatchAlarmTemplateGroup) string { return g.Arn },
				func(g *storedCloudWatchAlarmTemplateGroup) *map[string]string { return &g.Tags })
		},
		func() (map[string]string, bool) {
			return findLiveTags(b.cwAlarmTemplates.All(), resourceARN,
				func(t *storedCloudWatchAlarmTemplate) string { return t.Arn },
				func(t *storedCloudWatchAlarmTemplate) *map[string]string { return &t.Tags })
		},
		func() (map[string]string, bool) {
			return findLiveTags(b.ebRuleTemplateGroups.All(), resourceARN,
				func(g *storedEventBridgeRuleTemplateGroup) string { return g.Arn },
				func(g *storedEventBridgeRuleTemplateGroup) *map[string]string { return &g.Tags })
		},
		func() (map[string]string, bool) {
			return findLiveTags(b.ebRuleTemplates.All(), resourceARN,
				func(t *storedEventBridgeRuleTemplate) string { return t.Arn },
				func(t *storedEventBridgeRuleTemplate) *map[string]string { return &t.Tags })
		},
	}

	for _, find := range finders {
		if tags, ok := find(); ok {
			return tags, true
		}
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
