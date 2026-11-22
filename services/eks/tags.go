package eks

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// findTagInNodegroupsLocked searches nodegroups for a resource with the given ARN.
func (b *InMemoryBackend) findTagInNodegroupsLocked(resourceARN string) *tags.Tags {
	var found *tags.Tags

	b.nodegroups.Range(func(ng *Nodegroup) bool {
		if ng.ARN == resourceARN {
			found = ng.Tags

			return false
		}

		return true
	})

	return found
}

// findTagInAccessEntriesAndAddonsLocked searches access entries and addons.
func (b *InMemoryBackend) findTagInAccessEntriesAndAddonsLocked(resourceARN string) *tags.Tags {
	var found *tags.Tags

	b.accessEntries.Range(func(e *AccessEntry) bool {
		if e.ARN == resourceARN {
			found = e.Tags

			return false
		}

		return true
	})

	if found != nil {
		return found
	}

	b.addons.Range(func(a *Addon) bool {
		if a.ARN == resourceARN {
			found = a.Tags

			return false
		}

		return true
	})

	return found
}

// findTagInProfilesAndAssocLocked searches fargate profiles, pod identity
// associations, and subscriptions.
func (b *InMemoryBackend) findTagInProfilesAndAssocLocked(resourceARN string) *tags.Tags {
	var found *tags.Tags

	b.fargateProfiles.Range(func(p *FargateProfile) bool {
		if p.ARN == resourceARN {
			found = p.Tags

			return false
		}

		return true
	})

	if found != nil {
		return found
	}

	b.podIdentityAssociations.Range(func(a *PodIdentityAssociation) bool {
		if a.ARN == resourceARN {
			found = a.Tags

			return false
		}

		return true
	})

	if found != nil {
		return found
	}

	b.capabilities.Range(func(capa *Capability) bool {
		if capa.ARN == resourceARN {
			found = capa.Tags

			return false
		}

		return true
	})

	if found != nil {
		return found
	}

	b.subscriptions.Range(func(sub *AnywhereSubscription) bool {
		if sub.ARN == resourceARN {
			found = sub.Tags

			return false
		}

		return true
	})

	return found
}

// findTagsForARNLocked returns a pointer to the tags.Tags for the resource
// identified by resourceARN. Must be called with b.mu held (read or write).
// Returns nil if the resource is not found.
func (b *InMemoryBackend) findTagsForARNLocked(resourceARN string) *tags.Tags {
	var found *tags.Tags

	b.clusters.Range(func(c *Cluster) bool {
		if c.ARN == resourceARN {
			found = c.Tags

			return false
		}

		return true
	})

	if found != nil {
		return found
	}

	if t := b.findTagInNodegroupsLocked(resourceARN); t != nil {
		return t
	}

	if t := b.findTagInAccessEntriesAndAddonsLocked(resourceARN); t != nil {
		return t
	}

	if t := b.findTagInProfilesAndAssocLocked(resourceARN); t != nil {
		return t
	}

	return b.findTagInIdentityProviderConfigsLocked(resourceARN)
}

// findTagInIdentityProviderConfigsLocked searches identity provider configs.
func (b *InMemoryBackend) findTagInIdentityProviderConfigsLocked(resourceARN string) *tags.Tags {
	var found *tags.Tags

	b.identityProviderConfigs.Range(func(cfg *IdentityProviderConfig) bool {
		if cfg.ARN == resourceARN {
			found = cfg.Tags

			return false
		}

		return true
	})

	return found
}

// TagResource adds tags to a resource by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	t := b.findTagsForARNLocked(resourceARN)
	if t == nil {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	t.Merge(kv)

	return nil
}

// UntagResource removes specific tag keys from a resource by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	t := b.findTagsForARNLocked(resourceARN)
	if t == nil {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	t.DeleteKeys(tagKeys)

	return nil
}

// ListTagsForResource returns tags for a resource by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t := b.findTagsForARNLocked(resourceARN)
	if t == nil {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	return t.Clone(), nil
}

// TaggedEntry pairs a resource ARN with its tag map, for cross-service tag
// enumeration by the Resource Groups Tagging API (see cli.go's
// wireTaggingEKS).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

func appendEKSTaggedEntry(entries []TaggedEntry, arn string, t *tags.Tags, taggedOnly bool) []TaggedEntry {
	empty := t == nil || t.Len() == 0
	if taggedOnly && empty {
		return entries
	}

	entry := TaggedEntry{ARN: arn}
	if !empty {
		entry.Tags = t.Clone()
	}

	return append(entries, entry)
}

// TaggedResources lists every EKS resource that carries at least one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	return b.entries(true)
}

// InventoryResources lists every EKS resource, tagged or not.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(false)
}

func (b *InMemoryBackend) entries(taggedOnly bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	b.clusters.Range(func(c *Cluster) bool {
		out = appendEKSTaggedEntry(out, c.ARN, c.Tags, taggedOnly)

		return true
	})
	b.nodegroups.Range(func(ng *Nodegroup) bool {
		out = appendEKSTaggedEntry(out, ng.ARN, ng.Tags, taggedOnly)

		return true
	})
	b.accessEntries.Range(func(e *AccessEntry) bool {
		out = appendEKSTaggedEntry(out, e.ARN, e.Tags, taggedOnly)

		return true
	})
	b.addons.Range(func(a *Addon) bool {
		out = appendEKSTaggedEntry(out, a.ARN, a.Tags, taggedOnly)

		return true
	})
	b.fargateProfiles.Range(func(p *FargateProfile) bool {
		out = appendEKSTaggedEntry(out, p.ARN, p.Tags, taggedOnly)

		return true
	})
	b.podIdentityAssociations.Range(func(a *PodIdentityAssociation) bool {
		out = appendEKSTaggedEntry(out, a.ARN, a.Tags, taggedOnly)

		return true
	})
	b.capabilities.Range(func(capa *Capability) bool {
		out = appendEKSTaggedEntry(out, capa.ARN, capa.Tags, taggedOnly)

		return true
	})
	b.subscriptions.Range(func(sub *AnywhereSubscription) bool {
		out = appendEKSTaggedEntry(out, sub.ARN, sub.Tags, taggedOnly)

		return true
	})
	b.identityProviderConfigs.Range(func(cfg *IdentityProviderConfig) bool {
		out = appendEKSTaggedEntry(out, cfg.ARN, cfg.Tags, taggedOnly)

		return true
	})

	return out
}
