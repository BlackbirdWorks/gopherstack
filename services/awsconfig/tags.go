package awsconfig

// setResourceTagsLocked assigns tags to arn, overwriting any previous tags
// for that resource. Unlike TagResource, it does not take b.mu itself --
// callers that need to set tags while already holding the write lock (e.g.
// PutConnector/PutThirdPartyServiceLinkedConfigurationRecorder, whose real
// AWS semantics are "tags are added at creation and cannot be updated with
// this operation", so there is never existing tag state to merge) call this
// instead of re-entering TagResource's own lock. A nil/empty tags is a no-op
// so creating a resource with no Tags doesn't leave a spurious empty slice
// in b.resourceTags.
func (b *InMemoryBackend) setResourceTagsLocked(arn string, tags []Tag) {
	if len(tags) == 0 {
		return
	}

	cp := make([]Tag, len(tags))
	copy(cp, tags)
	b.resourceTags[arn] = cp
}

// TagResource adds tags to the resource identified by arn.
func (b *InMemoryBackend) TagResource(arn string, tags []Tag) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	existing := b.resourceTags[arn]
	// Build a map of existing keys for dedup / update.
	idx := make(map[string]int, len(existing))
	for i, t := range existing {
		idx[t.Key] = i
	}

	for _, t := range tags {
		if i, ok := idx[t.Key]; ok {
			existing[i].Value = t.Value
		} else {
			idx[t.Key] = len(existing)
			existing = append(existing, t)
		}
	}

	b.resourceTags[arn] = existing

	return nil
}

// UntagResource removes tags from the resource identified by arn.
func (b *InMemoryBackend) UntagResource(arn string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	remove := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		remove[k] = struct{}{}
	}

	existing := b.resourceTags[arn]
	filtered := existing[:0]

	for _, t := range existing {
		if _, skip := remove[t.Key]; !skip {
			filtered = append(filtered, t)
		}
	}

	b.resourceTags[arn] = filtered

	return nil
}

// ListTagsForResource returns all tags for the resource identified by arn.
func (b *InMemoryBackend) ListTagsForResource(arn string) []Tag {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	tags := b.resourceTags[arn]
	if len(tags) == 0 {
		return []Tag{}
	}

	out := make([]Tag, len(tags))
	copy(out, tags)

	return out
}

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources lists every tag-store ARN that carries at least one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	return b.entries(false)
}

// InventoryResources lists every config rule, aggregator, conformance pack, connector, recorder, organization
// rule or pack, stored query and aggregation authorization plus every tagged ARN of the tag store.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(true)
}

func (b *InMemoryBackend) entries(withUntagged bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.resourceTags))
	seen := make(map[string]struct{}, len(b.resourceTags))

	for key, t := range b.resourceTags {
		if len(t) == 0 {
			continue
		}

		seen[key] = struct{}{}
		out = append(out, TaggedEntry{ARN: key, Tags: tagListToMap(t)})
	}

	if withUntagged {
		for _, key := range b.resourceARNsLocked() {
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				out = append(out, TaggedEntry{ARN: key})
			}
		}
	}

	return out
}

func (b *InMemoryBackend) resourceARNsLocked() []string {
	out := make(
		[]string,
		0,
		b.configRules.Len()+b.aggregators.Len()+b.conformancePacks.Len()+b.connectors.Len()+b.recorders.Len()+
			b.orgConformancePacks.Len()+b.orgConfigRules.Len()+b.storedQueries.Len()+b.aggregationAuths.Len(),
	)

	for _, v := range b.configRules.All() {
		out = append(out, v.ConfigRuleArn)
	}

	for _, v := range b.aggregators.All() {
		out = append(out, v.ConfigurationAggregatorArn)
	}

	for _, v := range b.conformancePacks.All() {
		out = append(out, v.ConformancePackArn)
	}

	for _, v := range b.connectors.All() {
		out = append(out, v.Arn)
	}

	for _, v := range b.recorders.All() {
		out = append(out, v.Arn)
	}

	for _, v := range b.orgConformancePacks.All() {
		out = append(out, v.OrganizationConformancePackArn)
	}

	for _, v := range b.orgConfigRules.All() {
		out = append(out, v.OrganizationConfigRuleArn)
	}

	for _, v := range b.storedQueries.All() {
		out = append(out, v.QueryArn)
	}

	for _, v := range b.aggregationAuths.All() {
		out = append(out, v.AggregationAuthorizationArn)
	}

	return out
}

func tagListToMap(tags []Tag) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}
