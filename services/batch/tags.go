package batch

import (
	"context"
	"fmt"
	"maps"
)

// ListTagsForResource returns the tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resourceARN string) (map[string]string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if tags, ok := b.findTagsByARN(region, resourceARN); ok {
		out := make(map[string]string, len(tags))
		maps.Copy(out, tags)

		return out, nil
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// TagResource adds or updates tags on a resource identified by ARN.
func (b *InMemoryBackend) TagResource(ctx context.Context, resourceARN string, tags map[string]string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	existing, ok := b.findTagsByARN(region, resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	if existing == nil {
		b.initTagsByARN(region, resourceARN)
		existing, _ = b.findTagsByARN(region, resourceARN)
	}

	// Validate combined tag count (new keys only).
	newKeys := 0
	for k := range tags {
		if _, alreadyPresent := existing[k]; !alreadyPresent {
			newKeys++
		}
	}

	if len(existing)+newKeys > maxTagCount {
		return fmt.Errorf("%w: resource would exceed max tag count of %d", ErrValidation, maxTagCount)
	}

	if err := validateTags(tags); err != nil {
		return err
	}

	maps.Copy(existing, tags)

	return nil
}

// UntagResource removes tags from a resource identified by ARN.
func (b *InMemoryBackend) UntagResource(ctx context.Context, resourceARN string, tagKeys []string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	existing, ok := b.findTagsByARN(region, resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	for _, k := range tagKeys {
		delete(existing, k)
	}

	return nil
}

// findTagsByARN looks up the tags map for a resource by ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findTagsByARN(region, resourceARN string) (map[string]string, bool) {
	if tags, ok := b.findTagsInCoreResources(region, resourceARN); ok {
		return tags, true
	}

	return b.findTagsInPolicyResources(region, resourceARN)
}

func (b *InMemoryBackend) findTagsInCoreResources(region, resourceARN string) (map[string]string, bool) {
	for _, ce := range b.computeEnvironmentsByRegion.Get(region) {
		if ce.ComputeEnvironmentArn == resourceARN {
			return ce.Tags, true
		}
	}

	for _, jq := range b.jobQueuesByRegion.Get(region) {
		if jq.JobQueueArn == resourceARN {
			return jq.Tags, true
		}
	}

	if jd, ok := b.jobDefinitions.Get(regionKey(region, resourceARN)); ok {
		return jd.Tags, true
	}

	for _, j := range b.jobsByRegion.Get(region) {
		if j.JobARN == resourceARN {
			return j.Tags, true
		}
	}

	for _, cr := range b.consumableResourcesByRegion.Get(region) {
		if cr.ConsumableResourceArn == resourceARN {
			return cr.Tags, true
		}
	}

	return nil, false
}

func (b *InMemoryBackend) findTagsInPolicyResources(region, resourceARN string) (map[string]string, bool) {
	for _, sp := range b.schedulingPoliciesByRegion.Get(region) {
		if sp.Arn == resourceARN {
			return sp.Tags, true
		}
	}

	for _, se := range b.serviceEnvironmentsByRegion.Get(region) {
		if se.ServiceEnvironmentArn == resourceARN {
			return se.Tags, true
		}
	}

	for _, sj := range b.serviceJobsByRegion.Get(region) {
		if sj.JobArn == resourceARN {
			return sj.Tags, true
		}
	}

	return nil, false
}

// initTagsByARN ensures a resource has an initialised tags map.
// Caller must hold the write lock.
func (b *InMemoryBackend) initTagsByARN(region, resourceARN string) {
	if b.initTagsInCoreResources(region, resourceARN) {
		return
	}

	b.initTagsInPolicyResources(region, resourceARN)
}

func (b *InMemoryBackend) initTagsInCoreResources(region, resourceARN string) bool {
	for _, ce := range b.computeEnvironmentsByRegion.Get(region) {
		if ce.ComputeEnvironmentArn == resourceARN {
			ce.Tags = make(map[string]string)

			return true
		}
	}

	for _, jq := range b.jobQueuesByRegion.Get(region) {
		if jq.JobQueueArn == resourceARN {
			jq.Tags = make(map[string]string)

			return true
		}
	}

	if jd, ok := b.jobDefinitions.Get(regionKey(region, resourceARN)); ok {
		jd.Tags = make(map[string]string)

		return true
	}

	for _, j := range b.jobsByRegion.Get(region) {
		if j.JobARN == resourceARN {
			j.Tags = make(map[string]string)

			return true
		}
	}

	for _, cr := range b.consumableResourcesByRegion.Get(region) {
		if cr.ConsumableResourceArn == resourceARN {
			cr.Tags = make(map[string]string)

			return true
		}
	}

	return false
}

// TaggedEntry pairs a resource ARN with its tag map, for cross-service tag
// enumeration by the Resource Groups Tagging API (see cli.go's
// wireTaggingBatch).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// appendBatchTaggedEntry appends a TaggedEntry for arn/tagMap to entries when
// tagMap holds at least one tag.
func appendBatchTaggedEntry(entries []TaggedEntry, arn string, tagMap map[string]string) []TaggedEntry {
	if len(tagMap) == 0 {
		return entries
	}

	out := make(map[string]string, len(tagMap))
	maps.Copy(out, tagMap)

	return append(entries, TaggedEntry{ARN: arn, Tags: out})
}

// TaggedResources returns every Batch resource ARN that currently has at
// least one tag, across every taggable Batch resource kind (compute
// environments, job queues, job definitions, jobs, consumable resources,
// scheduling policies, service environments, service jobs).
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	for _, ce := range b.computeEnvironments.All() {
		out = appendBatchTaggedEntry(out, ce.ComputeEnvironmentArn, ce.Tags)
	}

	for _, jq := range b.jobQueues.All() {
		out = appendBatchTaggedEntry(out, jq.JobQueueArn, jq.Tags)
	}

	for _, jd := range b.jobDefinitions.All() {
		out = appendBatchTaggedEntry(out, jd.JobDefinitionArn, jd.Tags)
	}

	for _, j := range b.jobs.All() {
		out = appendBatchTaggedEntry(out, j.JobARN, j.Tags)
	}

	for _, cr := range b.consumableResources.All() {
		out = appendBatchTaggedEntry(out, cr.ConsumableResourceArn, cr.Tags)
	}

	for _, sp := range b.schedulingPolicies.All() {
		out = appendBatchTaggedEntry(out, sp.Arn, sp.Tags)
	}

	for _, se := range b.serviceEnvironments.All() {
		out = appendBatchTaggedEntry(out, se.ServiceEnvironmentArn, se.Tags)
	}

	for _, sj := range b.serviceJobs.All() {
		out = appendBatchTaggedEntry(out, sj.JobArn, sj.Tags)
	}

	return out
}

func (b *InMemoryBackend) initTagsInPolicyResources(region, resourceARN string) {
	for _, sp := range b.schedulingPoliciesByRegion.Get(region) {
		if sp.Arn == resourceARN {
			sp.Tags = make(map[string]string)

			return
		}
	}

	for _, se := range b.serviceEnvironmentsByRegion.Get(region) {
		if se.ServiceEnvironmentArn == resourceARN {
			se.Tags = make(map[string]string)

			return
		}
	}

	for _, sj := range b.serviceJobsByRegion.Get(region) {
		if sj.JobArn == resourceARN {
			sj.Tags = make(map[string]string)

			return
		}
	}
}
