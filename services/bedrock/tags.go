package bedrock

import (
	"fmt"
	"sort"
)

// copyTags returns a new slice with a deep copy of tags.
func copyTags(src []Tag) []Tag {
	if len(src) == 0 {
		return nil
	}

	dst := make([]Tag, len(src))
	copy(dst, src)

	return dst
}

// mergeTags merges new tags into existing ones and returns a sorted result.
// Existing tags with the same key are overwritten by the new values.
func mergeTags(existing, newTags []Tag) []Tag {
	tagMap := make(map[string]string, len(existing)+len(newTags))
	for _, t := range existing {
		tagMap[t.Key] = t.Value
	}

	for _, t := range newTags {
		tagMap[t.Key] = t.Value
	}

	merged := make([]Tag, 0, len(tagMap))
	for k, v := range tagMap {
		merged = append(merged, Tag{Key: k, Value: v})
	}

	sort.Slice(merged, func(i, j int) bool { return merged[i].Key < merged[j].Key })

	return merged
}

// filterTags returns a new slice with the specified keys removed.
func filterTags(existing []Tag, removeKeys map[string]bool) []Tag {
	result := make([]Tag, 0, len(existing))
	for _, t := range existing {
		if !removeKeys[t.Key] {
			result = append(result, t)
		}
	}

	return result
}

// taggableResolvers lists, one closure per taggable resource family, how to
// find that family's Tags field by ARN. Data-driven (rather than one
// hand-written if-chain per operation) so TagResource/UntagResource/
// ListTagsForResource all share a single registry and adding a family means
// adding one entry here, not three near-identical branches (which is exactly
// how CreateModelCopyJob et al. went missing from this registry despite the
// backend correctly storing tags on the resource itself -- gopherstack-2mwl).
//
//nolint:gochecknoglobals // registration table, analogous to arnCollectorFuncs elsewhere
var taggableResolvers = []func(*InMemoryBackend, string) (*[]Tag, bool){
	func(b *InMemoryBackend, resourceARN string) (*[]Tag, bool) {
		id, ok := b.guardrailsByARN[resourceARN]
		if !ok {
			return nil, false
		}

		g, _ := b.guardrails.Get(id)

		return &g.Tags, true
	},
	func(b *InMemoryBackend, resourceARN string) (*[]Tag, bool) {
		if v, ok := b.provisionedModelThroughputs.Get(resourceARN); ok {
			return &v.Tags, true
		}

		return nil, false
	},
	func(b *InMemoryBackend, resourceARN string) (*[]Tag, bool) {
		if v, ok := b.evaluationJobs.Get(resourceARN); ok {
			return &v.Tags, true
		}

		return nil, false
	},
	func(b *InMemoryBackend, resourceARN string) (*[]Tag, bool) {
		if v, ok := b.automatedReasoningPolicies.Get(resourceARN); ok {
			return &v.Tags, true
		}

		return nil, false
	},
	func(b *InMemoryBackend, resourceARN string) (*[]Tag, bool) {
		if v, ok := b.customModels.Get(resourceARN); ok {
			return &v.Tags, true
		}

		return nil, false
	},
	func(b *InMemoryBackend, resourceARN string) (*[]Tag, bool) {
		if v, ok := b.customModelDeployments.Get(resourceARN); ok {
			return &v.Tags, true
		}

		return nil, false
	},
	func(b *InMemoryBackend, resourceARN string) (*[]Tag, bool) {
		if v, ok := b.advancedPromptOptimizationJobs.Get(resourceARN); ok {
			return &v.Tags, true
		}

		return nil, false
	},
	func(b *InMemoryBackend, resourceARN string) (*[]Tag, bool) {
		if v, ok := b.inferenceProfiles.Get(resourceARN); ok {
			return &v.Tags, true
		}

		return nil, false
	},
	func(b *InMemoryBackend, resourceARN string) (*[]Tag, bool) {
		if v, ok := b.marketplaceEndpoints.Get(resourceARN); ok {
			return &v.Tags, true
		}

		return nil, false
	},
	func(b *InMemoryBackend, resourceARN string) (*[]Tag, bool) {
		if v, ok := b.modelCopyJobs.Get(resourceARN); ok {
			return &v.Tags, true
		}

		return nil, false
	},
	func(b *InMemoryBackend, resourceARN string) (*[]Tag, bool) {
		if v, ok := b.modelImportJobs.Get(resourceARN); ok {
			return &v.Tags, true
		}

		return nil, false
	},
	func(b *InMemoryBackend, resourceARN string) (*[]Tag, bool) {
		if v, ok := b.promptRouters.Get(resourceARN); ok {
			return &v.Tags, true
		}

		return nil, false
	},
	func(b *InMemoryBackend, resourceARN string) (*[]Tag, bool) {
		if v, ok := b.modelCustomizationJobs.Get(resourceARN); ok {
			return &v.Tags, true
		}

		return nil, false
	},
	func(b *InMemoryBackend, resourceARN string) (*[]Tag, bool) {
		if v, ok := b.findARPVersionByARN(resourceARN); ok {
			return &v.Tags, true
		}

		return nil, false
	},
}

// findTaggableResource returns a pointer to the Tags field of the resource
// identified by resourceARN, searching every family in taggableResolvers.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findTaggableResource(resourceARN string) (*[]Tag, bool) {
	for _, resolve := range taggableResolvers {
		if tags, ok := resolve(b, resourceARN); ok {
			return tags, true
		}
	}

	return nil, false
}

// ListTagsForResource returns tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) ([]Tag, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	tags, ok := b.findTaggableResource(resourceARN)
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	return copyTags(*tags), nil
}

// TagResource adds or updates tags on a resource identified by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags []Tag) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	existing, ok := b.findTaggableResource(resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	*existing = mergeTags(*existing, tags)

	return nil
}

// UntagResource removes tags from a resource identified by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	existing, ok := b.findTaggableResource(resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	removeSet := make(map[string]bool, len(tagKeys))
	for _, k := range tagKeys {
		removeSet[k] = true
	}

	*existing = filterTags(*existing, removeSet)

	return nil
}

// findARPVersionByARN looks up an AutomatedReasoningPolicyVersion by its own
// externally-visible ARN (policyARN + "/version/" + versionNum), which
// differs from the "policyARN:versionNum" key arpVersions is stored under
// (see arpVersionsKeyFn) -- a linear scan over the small per-account version
// set is simplest here rather than maintaining a second index.
func (b *InMemoryBackend) findARPVersionByARN(resourceARN string) (*AutomatedReasoningPolicyVersion, bool) {
	for _, v := range b.arpVersions.All() {
		if v.PolicyArn == resourceARN {
			return v, true
		}
	}

	return nil, false
}
