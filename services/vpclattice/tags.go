package vpclattice

import (
	"maps"
)

// ------- Tagging operations -------

// TagResource adds tags to a resource.
func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if _, ok := b.tags[resourceArn]; !ok {
		b.tags[resourceArn] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceArn], tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceArn string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if t, ok := b.tags[resourceArn]; ok {
		for _, k := range keys {
			delete(t, k)
		}
	}

	return nil
}

// ListTagsForResource returns tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t, ok := b.tags[resourceArn]
	if !ok {
		return make(map[string]string), nil
	}

	result := make(map[string]string, len(t))
	maps.Copy(result, t)

	return result, nil
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

// InventoryResources lists every service network, association, service, listener, rule, target group,
// resource configuration, resource gateway, access log subscription and domain verification plus every tagged
// ARN of the tag store.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(true)
}

func (b *InMemoryBackend) entries(withUntagged bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.tags))
	seen := make(map[string]struct{}, len(b.tags))

	for key, t := range b.tags {
		if len(t) == 0 {
			continue
		}

		seen[key] = struct{}{}
		out = append(out, TaggedEntry{ARN: key, Tags: copyTags(t)})
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
	out := b.resourceARNsPart1Locked()
	out = append(out, b.resourceARNsPart2Locked()...)

	return out
}

func (b *InMemoryBackend) resourceARNsPart1Locked() []string {
	out := make(
		[]string,
		0,
		b.serviceNetworks.Len()+b.snras.Len()+b.snsas.Len()+b.snvas.Len()+b.services.Len()+b.listeners.Len()+
			b.rules.Len()+b.targetGroups.Len(),
	)

	for _, v := range b.serviceNetworks.All() {
		out = append(out, v.ARN)
	}

	for _, v := range b.snras.All() {
		out = append(out, v.ARN)
	}

	for _, v := range b.snsas.All() {
		out = append(out, v.ARN)
	}

	for _, v := range b.snvas.All() {
		out = append(out, v.ARN)
	}

	for _, v := range b.services.All() {
		out = append(out, v.ARN)
	}

	for _, v := range b.listeners.All() {
		out = append(out, v.ARN)
	}

	for _, v := range b.rules.All() {
		out = append(out, v.ARN)
	}

	for _, v := range b.targetGroups.All() {
		out = append(out, v.ARN)
	}

	return out
}

func (b *InMemoryBackend) resourceARNsPart2Locked() []string {
	out := make(
		[]string,
		0,
		b.resourceConfigurations.Len()+b.resourceGateways.Len()+b.alss.Len()+b.domainVerifications.Len(),
	)

	for _, v := range b.resourceConfigurations.All() {
		out = append(out, v.ARN)
	}

	for _, v := range b.resourceGateways.All() {
		out = append(out, v.ARN)
	}

	for _, v := range b.alss.All() {
		out = append(out, v.ARN)
	}

	for _, v := range b.domainVerifications.All() {
		out = append(out, v.ARN)
	}

	return out
}
