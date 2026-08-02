package grafana

import "context"

// TaggedEntry is one tagged resource, used by TaggedResources for the
// resourcegroupstaggingapi integration (cli.go's wireTaggingGrafana).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TagResource adds or updates tags on the workspace identified by
// resourceArn. Only workspaces are taggable ("Currently, the only resource
// that can be tagged is workspaces" -- TagResourceInput's doc comment).
func (b *InMemoryBackend) TagResource(_ context.Context, resourceArn string, newTags map[string]string) error {
	id, ok := workspaceIDFromARN(resourceArn)
	if !ok {
		return validationError("malformed resourceArn: " + resourceArn)
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	w, ok := b.workspaces.Get(id)
	if !ok {
		return notFoundError(resourceTypeWorkspace, id)
	}

	w.Tags.Merge(newTags)

	return nil
}

// UntagResource removes tags from the workspace identified by resourceArn.
func (b *InMemoryBackend) UntagResource(_ context.Context, resourceArn string, tagKeys []string) error {
	id, ok := workspaceIDFromARN(resourceArn)
	if !ok {
		return validationError("malformed resourceArn: " + resourceArn)
	}

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	w, ok := b.workspaces.Get(id)
	if !ok {
		return notFoundError(resourceTypeWorkspace, id)
	}

	w.Tags.DeleteKeys(tagKeys)

	return nil
}

// ListTagsForResource returns the tags for the workspace identified by
// resourceArn.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	id, ok := workspaceIDFromARN(resourceArn)
	if !ok {
		return nil, validationError("malformed resourceArn: " + resourceArn)
	}

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	w, ok := b.workspaces.Get(id)
	if !ok {
		return nil, notFoundError(resourceTypeWorkspace, id)
	}

	return w.Tags.Clone(), nil
}

// TaggedResources returns every tagged workspace, for the
// resourcegroupstaggingapi integration.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	all := b.workspaces.Snapshot()
	out := make([]TaggedEntry, 0, len(all))

	for _, w := range all {
		if w.Tags == nil || w.Tags.Len() == 0 {
			continue
		}

		out = append(out, TaggedEntry{ARN: w.ARN, Tags: w.Tags.Clone()})
	}

	return out
}
