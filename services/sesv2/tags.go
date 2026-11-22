package sesv2

import "maps"

func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	b.putResourceTagsLocked(arn, tags)

	return nil
}

// putResourceTagsLocked merges tags into the shared resourceTags store so
// ListTagsForResource sees them. Every Create* op that accepts inline tags
// must call this at creation time -- storing tags only on the resource's own
// domain struct (as CreateTenant/CreateEmailIdentity used to) left
// ListTagsForResource and cross-service GetResources blind to creation-time
// tags. Caller must hold b.mu.
func (b *InMemoryBackend) putResourceTagsLocked(arn string, tags map[string]string) {
	if len(tags) == 0 {
		return
	}

	if b.resourceTags[arn] == nil {
		b.resourceTags[arn] = make(map[string]string)
	}

	maps.Copy(b.resourceTags[arn], tags)
}

func (b *InMemoryBackend) UntagResource(arn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	m := b.resourceTags[arn]
	for _, k := range tagKeys {
		delete(m, k)
	}

	return nil
}

func (b *InMemoryBackend) ListTagsForResource(arn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	out := make(map[string]string, len(b.resourceTags[arn]))
	maps.Copy(out, b.resourceTags[arn])

	return out, nil
}

// liveTagsLocked returns a copy of the live tags for arn from the shared
// resourceTags store, rather than a resource's own creation-time Tags
// snapshot -- callers that echo tags back through a Get op must read this
// live map so tags applied via TagResource/UntagResource after creation are
// visible, matching the fix already applied for Create*'s inline tags (see
// putResourceTagsLocked). Caller must hold at least a read lock.
func (b *InMemoryBackend) liveTagsLocked(arn string) map[string]string {
	if len(b.resourceTags[arn]) == 0 {
		return nil
	}

	out := make(map[string]string, len(b.resourceTags[arn]))
	maps.Copy(out, b.resourceTags[arn])

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

// InventoryResources lists every email identity, configuration set, contact list, email template, dedicated
// IP pool and tenant plus every tagged ARN of the tag store.
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
		out = append(out, TaggedEntry{ARN: key, Tags: maps.Clone(t)})
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
		b.identities.Len()+b.configurationSets.Len()+b.contactLists.Len()+b.emailTemplates.Len()+b.dedicatedIPPools.Len()+len(
			b.tenants,
		),
	)

	for _, v := range b.identities.All() {
		out = append(out, b.identityARN(v.Identity))
	}

	for _, v := range b.configurationSets.All() {
		out = append(out, b.configurationSetARN(v.Name))
	}

	for _, v := range b.contactLists.All() {
		out = append(out, b.contactListARN(v.Name))
	}

	for _, v := range b.emailTemplates.All() {
		out = append(out, b.emailTemplateARN(v.TemplateName))
	}

	for _, v := range b.dedicatedIPPools.All() {
		out = append(out, b.dedicatedIPPoolARN(v.PoolName))
	}

	for name := range b.tenants {
		out = append(out, b.tenantARN(name))
	}

	return out
}
