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

// ListTagsForResource returns tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) ([]Tag, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	tags, ok := b.findTagsByARN(resourceARN)
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	return copyTags(tags), nil
}

// TagResource adds or updates tags on a resource identified by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags []Tag) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if id, ok := b.guardrailsByARN[resourceARN]; ok {
		g, _ := b.guardrails.Get(id)
		g.Tags = mergeTags(g.Tags, tags)

		return nil
	}

	if pmt, ok := b.provisionedModelThroughputs.Get(resourceARN); ok {
		pmt.Tags = mergeTags(pmt.Tags, tags)

		return nil
	}

	if job, ok := b.evaluationJobs.Get(resourceARN); ok {
		job.Tags = mergeTags(job.Tags, tags)

		return nil
	}

	if policy, ok := b.automatedReasoningPolicies.Get(resourceARN); ok {
		policy.Tags = mergeTags(policy.Tags, tags)

		return nil
	}

	if model, ok := b.customModels.Get(resourceARN); ok {
		model.Tags = mergeTags(model.Tags, tags)

		return nil
	}

	if deployment, ok := b.customModelDeployments.Get(resourceARN); ok {
		deployment.Tags = mergeTags(deployment.Tags, tags)

		return nil
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// UntagResource removes tags from a resource identified by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	removeSet := make(map[string]bool, len(tagKeys))
	for _, k := range tagKeys {
		removeSet[k] = true
	}

	if id, ok := b.guardrailsByARN[resourceARN]; ok {
		g, _ := b.guardrails.Get(id)
		g.Tags = filterTags(g.Tags, removeSet)

		return nil
	}

	if pmt, ok := b.provisionedModelThroughputs.Get(resourceARN); ok {
		pmt.Tags = filterTags(pmt.Tags, removeSet)

		return nil
	}

	if job, ok := b.evaluationJobs.Get(resourceARN); ok {
		job.Tags = filterTags(job.Tags, removeSet)

		return nil
	}

	if policy, ok := b.automatedReasoningPolicies.Get(resourceARN); ok {
		policy.Tags = filterTags(policy.Tags, removeSet)

		return nil
	}

	if model, ok := b.customModels.Get(resourceARN); ok {
		model.Tags = filterTags(model.Tags, removeSet)

		return nil
	}

	if deployment, ok := b.customModelDeployments.Get(resourceARN); ok {
		deployment.Tags = filterTags(deployment.Tags, removeSet)

		return nil
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// findTagsByARN returns the tags slice pointer for a resource by ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findTagsByARN(resourceARN string) ([]Tag, bool) {
	if id, ok := b.guardrailsByARN[resourceARN]; ok {
		g, _ := b.guardrails.Get(id)

		return g.Tags, true
	}

	if pmt, ok := b.provisionedModelThroughputs.Get(resourceARN); ok {
		return pmt.Tags, true
	}

	if job, ok := b.evaluationJobs.Get(resourceARN); ok {
		return job.Tags, true
	}

	if policy, ok := b.automatedReasoningPolicies.Get(resourceARN); ok {
		return policy.Tags, true
	}

	if model, ok := b.customModels.Get(resourceARN); ok {
		return model.Tags, true
	}

	if deployment, ok := b.customModelDeployments.Get(resourceARN); ok {
		return deployment.Tags, true
	}

	return nil, false
}
