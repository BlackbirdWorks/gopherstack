package emrserverless

import (
	"fmt"
	"maps"
)

// ListTagsForResource returns tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	tags, ok := b.findTagsByARN(resourceARN)
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	out := make(map[string]string, len(tags))
	maps.Copy(out, tags)

	return out, nil
}

// TagResource adds or updates tags on a resource identified by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	existing, ok := b.findTagsByARN(resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	maps.Copy(existing, tags)

	return nil
}

// UntagResource removes tags from a resource identified by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	existing, ok := b.findTagsByARN(resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	for _, k := range tagKeys {
		delete(existing, k)
	}

	return nil
}

// TaggedEntry pairs a resource ARN with its tag set, for the Resource Groups
// Tagging API's GetResources (see cli.go's wireTaggingEmrServerless).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every application and job run that carries at
// least one tag, keyed by ARN. Sessions are excluded: real EMR Serverless
// only supports applications and job runs as TagResource/ListTagsForResource
// targets (botocore 1.43.56, emr-serverless/2021-07-13/service-2.json.gz,
// TagResourceRequest.resourceArn: "the supported resources are Amazon EMR
// Serverless applications and job runs"), unlike findTagsByARN above which is
// more permissive for the backend's own direct tag calls.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	apps := b.applications.All()
	jobRuns := b.jobRuns.All()
	b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(apps)+len(jobRuns))

	for _, app := range apps {
		if len(app.Tags) == 0 {
			continue
		}

		out = append(out, TaggedEntry{ARN: app.Arn, Tags: maps.Clone(app.Tags)})
	}

	for _, jr := range jobRuns {
		if len(jr.Tags) == 0 {
			continue
		}

		out = append(out, TaggedEntry{ARN: jr.Arn, Tags: maps.Clone(jr.Tags)})
	}

	return out
}

// findTagsByARN looks up the tags map for a resource by ARN using O(1) index lookups.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findTagsByARN(resourceARN string) (map[string]string, bool) {
	if list := b.applicationsByARN.Get(resourceARN); len(list) > 0 {
		return list[0].Tags, true
	}

	if list := b.jobRunsByARN.Get(resourceARN); len(list) > 0 {
		return list[0].Tags, true
	}

	if list := b.sessionsByARN.Get(resourceARN); len(list) > 0 {
		return list[0].Tags, true
	}

	return nil, false
}
