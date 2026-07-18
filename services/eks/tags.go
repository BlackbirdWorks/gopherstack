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

	return b.findTagInProfilesAndAssocLocked(resourceARN)
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
