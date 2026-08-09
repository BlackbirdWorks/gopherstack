package redshift

import (
	"fmt"
	"maps"
)

// ---------------------------------------------------------------------------
// Serverless resource tags (TagResource / UntagResource / ListTagsForResource)
// ---------------------------------------------------------------------------

// slResourceExistsLocked reports whether resourceArn belongs to a known
// taggable Redshift Serverless resource. Only Namespace/Workgroup/Snapshot
// accept a "tags" list on their Create* request (confirmed against
// CreateNamespace/CreateWorkgroup/CreateSnapshot's Request shapes in
// service-2.json -- CreateUsageLimitRequest/CreateScheduledActionRequest have
// no such field), so those are the three ARN spaces checked here. Caller must
// hold b.mu.
func (b *InMemoryBackend) slResourceExistsLocked(resourceArn string) bool {
	for _, ns := range b.slNamespaces.All() {
		if ns.NamespaceArn == resourceArn {
			return true
		}
	}

	for _, wg := range b.slWorkgroups.All() {
		if wg.WorkgroupArn == resourceArn {
			return true
		}
	}

	for _, sn := range b.slSnapshots.All() {
		if sn.SnapshotArn == resourceArn {
			return true
		}
	}

	return false
}

// putServerlessTagsLocked merges newTags into resourceArn's tag set without
// validating the resource exists. Callers must already hold b.mu for writing.
// Used both by TagServerlessResource (after its own existence check) and by
// CreateNamespace/CreateWorkgroup/CreateServerlessSnapshot's creation-time
// Tags/tags parameter, which cannot re-acquire the non-reentrant b.mu.
func (b *InMemoryBackend) putServerlessTagsLocked(resourceArn string, newTags map[string]string) {
	if len(newTags) == 0 {
		return
	}

	set, ok := b.slResourceTags.Get(resourceArn)
	if !ok {
		set = &slResourceTagSet{ResourceArn: resourceArn, Tags: make(map[string]string, len(newTags))}
	}

	maps.Copy(set.Tags, newTags)

	b.slResourceTags.Put(set)
}

// TagServerlessResource assigns tags to a taggable Redshift Serverless resource.
func (b *InMemoryBackend) TagServerlessResource(resourceArn string, newTags map[string]string) error {
	b.mu.Lock("TagServerlessResource")
	defer b.mu.Unlock()

	if !b.slResourceExistsLocked(resourceArn) {
		return fmt.Errorf("%w: resource %q not found", ErrServerlessResourceNotFound, resourceArn)
	}

	b.putServerlessTagsLocked(resourceArn, newTags)

	return nil
}

// UntagServerlessResource removes the given tag keys from a resource.
func (b *InMemoryBackend) UntagServerlessResource(resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagServerlessResource")
	defer b.mu.Unlock()

	if !b.slResourceExistsLocked(resourceArn) {
		return fmt.Errorf("%w: resource %q not found", ErrServerlessResourceNotFound, resourceArn)
	}

	set, ok := b.slResourceTags.Get(resourceArn)
	if !ok {
		return nil
	}

	for _, k := range tagKeys {
		delete(set.Tags, k)
	}

	return nil
}

// ListServerlessResourceTags returns a resource's current tags.
func (b *InMemoryBackend) ListServerlessResourceTags(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListServerlessResourceTags")
	defer b.mu.RUnlock()

	if !b.slResourceExistsLocked(resourceArn) {
		return nil, fmt.Errorf("%w: resource %q not found", ErrServerlessResourceNotFound, resourceArn)
	}

	set, ok := b.slResourceTags.Get(resourceArn)
	if !ok {
		return map[string]string{}, nil
	}

	cp := make(map[string]string, len(set.Tags))
	maps.Copy(cp, set.Tags)

	return cp, nil
}
