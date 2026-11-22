package apprunner

import (
	"fmt"
	"maps"
)

// resourceExists reports whether resourceArn refers to any known resource type.
func (b *InMemoryBackend) resourceExists(resourceArn string) bool {
	if b.services.Has(resourceArn) {
		return true
	}
	if b.autoScalingConfigs.Has(resourceArn) {
		return true
	}
	if b.connections.Has(resourceArn) {
		return true
	}
	if b.observabilityConfigs.Has(resourceArn) {
		return true
	}
	if b.vpcConnectors.Has(resourceArn) {
		return true
	}
	if b.vpcIngressConnections.Has(resourceArn) {
		return true
	}

	return false
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.resourceExists(resourceArn) {
		return fmt.Errorf("resource %s not found: %w", resourceArn, ErrNotFound)
	}

	if b.tags[resourceArn] == nil {
		b.tags[resourceArn] = make(map[string]string)
	}
	maps.Copy(b.tags[resourceArn], tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceArn string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.resourceExists(resourceArn) {
		return fmt.Errorf("resource %s not found: %w", resourceArn, ErrNotFound)
	}

	for _, k := range keys {
		delete(b.tags[resourceArn], k)
	}

	return nil
}

// ListTagsForResource returns all tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if !b.resourceExists(resourceArn) {
		return nil, fmt.Errorf("resource %s not found: %w", resourceArn, ErrNotFound)
	}

	result := make(map[string]string)
	maps.Copy(result, b.tags[resourceArn])

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

// InventoryResources lists every service, auto scaling configuration, connection, observability
// configuration, VPC connector and VPC ingress connection plus every tagged ARN of the tag store.
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
		b.services.Len()+b.autoScalingConfigs.Len()+b.connections.Len()+b.observabilityConfigs.Len()+
			b.vpcConnectors.Len()+b.vpcIngressConnections.Len(),
	)

	for _, s := range b.services.All() {
		out = append(out, s.ServiceArn)
	}

	for _, c := range b.autoScalingConfigs.All() {
		out = append(out, c.AutoScalingConfigurationArn)
	}

	for _, c := range b.connections.All() {
		out = append(out, c.ConnectionArn)
	}

	for _, c := range b.observabilityConfigs.All() {
		out = append(out, c.ObservabilityConfigurationArn)
	}

	for _, c := range b.vpcConnectors.All() {
		out = append(out, c.VpcConnectorArn)
	}

	for _, c := range b.vpcIngressConnections.All() {
		out = append(out, c.VpcIngressConnectionArn)
	}

	return out
}
