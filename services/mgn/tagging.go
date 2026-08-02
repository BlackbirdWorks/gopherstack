package mgn

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// This file backs family L (3 ops): TagResource, UntagResource,
// ListTagsForResource -- the only 3 ops sharing the /tags/{resourceArn}
// path, and a distinct, newer-generation error set (AccessDenied/
// InternalServer/ResourceNotFound/Throttling/Validation, never
// UninitializedAccount) from every other legacy op (PARITY.md).
//
// 12 taggable resource kinds share the "mgn" ARN namespace -- richer than
// outposts' 2 or directconnect's 5 -- so resolveTaggableLocked needs
// resourceTypeFromARN-style multi-kind dispatch, matching
// wireTaggingDirectConnect's pattern in cli.go.

// TaggedEntry is one tagged resource, used by TaggedResources for the
// resourcegroupstaggingapi integration (cli.go's wireTaggingMGN).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// resolveTaggableLocked resolves resourceArn to the tags.Tags instance of
// whichever of the 12 taggable resource kinds it names -- split across
// itself plus resolveTaggableGroup2Locked/resolveTaggableGroup3Locked
// purely to keep each function's branch count low (decomposition, not
// suppression -- see .claude/memories/parity-principles.md's ban on
// cyclop/funlen/gocyclo/gocognit suppressions). Callers must hold b.mu
// (either lock).
func (b *InMemoryBackend) resolveTaggableLocked(resourceArn string) (*tags.Tags, bool) {
	if id, ok := resourceIDFromARN(resourceArn, ":source-server/"); ok {
		if s, found := b.sourceServers.Get(id); found {
			return s.Tags, true
		}
	}

	if id, ok := resourceIDFromARN(resourceArn, ":job/"); ok {
		if j, found := b.jobs.Get(id); found {
			return j.Tags, true
		}
	}

	if id, ok := resourceIDFromARN(resourceArn, ":application/"); ok {
		if a, found := b.applications.Get(id); found {
			return a.Tags, true
		}
	}

	if id, ok := resourceIDFromARN(resourceArn, ":wave/"); ok {
		if w, found := b.waves.Get(id); found {
			return w.Tags, true
		}
	}

	if t, ok := b.resolveTaggableGroup2Locked(resourceArn); ok {
		return t, true
	}

	return b.resolveTaggableGroup3Locked(resourceArn)
}

// resolveTaggableGroup2Locked covers Connector/VcenterClient/
// LaunchConfigurationTemplate/ReplicationConfigurationTemplate -- split out
// of resolveTaggableLocked purely to keep that function's branch count low
// (decomposition, not suppression).
func (b *InMemoryBackend) resolveTaggableGroup2Locked(resourceArn string) (*tags.Tags, bool) {
	if id, ok := resourceIDFromARN(resourceArn, ":connector/"); ok {
		if c, found := b.connectors.Get(id); found {
			return c.Tags, true
		}
	}

	if id, ok := resourceIDFromARN(resourceArn, ":vcenter-client/"); ok {
		if v, found := b.vcenterClients.Get(id); found {
			return v.Tags, true
		}
	}

	if id, ok := resourceIDFromARN(resourceArn, ":launch-configuration-template/"); ok {
		if t, found := b.launchTemplates.Get(id); found {
			return t.Tags, true
		}
	}

	if id, ok := resourceIDFromARN(resourceArn, ":replication-configuration-template/"); ok {
		if t, found := b.replicationTemplates.Get(id); found {
			return t.Tags, true
		}
	}

	return nil, false
}

// resolveTaggableGroup3Locked covers ExportTask/ImportTask/
// NetworkMigrationDefinition/NetworkMigrationExecution -- see
// resolveTaggableGroup2Locked's doc comment for why this is split out.
func (b *InMemoryBackend) resolveTaggableGroup3Locked(resourceArn string) (*tags.Tags, bool) {
	if id, ok := resourceIDFromARN(resourceArn, ":export-task/"); ok {
		if t, found := b.exportTasks.Get(id); found {
			return t.Tags, true
		}
	}

	if id, ok := resourceIDFromARN(resourceArn, ":import-task/"); ok {
		if t, found := b.importTasks.Get(id); found {
			return t.Tags, true
		}
	}

	if id, ok := resourceIDFromARN(resourceArn, ":network-migration-definition/"); ok {
		if d, found := b.nmDefinitions.Get(id); found {
			return d.Tags, true
		}
	}

	if id, ok := resourceIDFromARN(resourceArn, ":network-migration-execution/"); ok {
		for _, e := range b.nmExecutions.All() {
			if e.NetworkMigrationExecutionID == id {
				return e.Tags, true
			}
		}
	}

	return nil, false
}

// TagResource adds or updates tags on the named resource.
func (b *InMemoryBackend) TagResource(_ context.Context, resourceArn string, newTags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	t, ok := b.resolveTaggableLocked(resourceArn)
	if !ok {
		return notFoundError(resourceTaggable, resourceArn)
	}

	t.Merge(newTags)

	return nil
}

// UntagResource removes tags from the named resource.
func (b *InMemoryBackend) UntagResource(_ context.Context, resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	t, ok := b.resolveTaggableLocked(resourceArn)
	if !ok {
		return notFoundError(resourceTaggable, resourceArn)
	}

	t.DeleteKeys(tagKeys)

	return nil
}

// ListTagsForResource returns the tags on the named resource.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t, ok := b.resolveTaggableLocked(resourceArn)
	if !ok {
		return nil, notFoundError(resourceTaggable, resourceArn)
	}

	return t.Clone(), nil
}

// appendIfTagged appends a TaggedEntry for (arn, t) to out iff t carries at
// least one tag -- matching services/directconnect's identical helper.
func appendIfTagged(out []TaggedEntry, arn string, t *tags.Tags) []TaggedEntry {
	if t == nil || t.Len() == 0 {
		return out
	}

	return append(out, TaggedEntry{ARN: arn, Tags: t.Clone()})
}

// TaggedResources returns every tagged resource across all 12 taggable
// kinds, for the resourcegroupstaggingapi integration.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	out = b.taggedResourcesGroup1Locked(out)
	out = b.taggedResourcesGroup2Locked(out)

	return out
}

// own ARN-builder/Tags field; see taggedResourcesGroup2Locked's sibling below -- the shared shape
// is the point (a uniform tagging seam across 12 kinds), not accidental duplication.
//
//nolint:dupl // each block below builds a TaggedEntry from a genuinely distinct resource kind's
func (b *InMemoryBackend) taggedResourcesGroup1Locked(out []TaggedEntry) []TaggedEntry {
	for _, s := range b.sourceServers.Snapshot() {
		out = appendIfTagged(out, b.sourceServerARN(s.SourceServerID), s.Tags)
	}

	for _, j := range b.jobs.Snapshot() {
		out = appendIfTagged(out, b.jobARN(j.JobID), j.Tags)
	}

	for _, a := range b.applications.Snapshot() {
		out = appendIfTagged(out, b.applicationARN(a.ApplicationID), a.Tags)
	}

	for _, w := range b.waves.Snapshot() {
		out = appendIfTagged(out, b.waveARN(w.WaveID), w.Tags)
	}

	for _, c := range b.connectors.Snapshot() {
		out = appendIfTagged(out, b.connectorARN(c.ConnectorID), c.Tags)
	}

	for _, v := range b.vcenterClients.Snapshot() {
		out = appendIfTagged(out, b.vcenterClientARN(v.VcenterClientID), v.Tags)
	}

	return out
}

//nolint:dupl // see taggedResourcesGroup1Locked's sibling doc comment above
func (b *InMemoryBackend) taggedResourcesGroup2Locked(out []TaggedEntry) []TaggedEntry {
	for _, t := range b.launchTemplates.Snapshot() {
		out = appendIfTagged(out, b.launchTemplateARN(t.LaunchConfigurationTemplateID), t.Tags)
	}

	for _, t := range b.replicationTemplates.Snapshot() {
		out = appendIfTagged(out, b.replicationTemplateARN(t.ReplicationConfigurationTemplateID), t.Tags)
	}

	for _, e := range b.exportTasks.Snapshot() {
		out = appendIfTagged(out, b.exportTaskARN(e.ExportID), e.Tags)
	}

	for _, i := range b.importTasks.Snapshot() {
		out = appendIfTagged(out, b.importTaskARN(i.ImportID), i.Tags)
	}

	for _, d := range b.nmDefinitions.Snapshot() {
		out = appendIfTagged(out, b.nmDefinitionARN(d.NetworkMigrationDefinitionID), d.Tags)
	}

	for _, e := range b.nmExecutions.Snapshot() {
		out = appendIfTagged(out, b.nmExecutionARN(e.NetworkMigrationExecutionID), e.Tags)
	}

	return out
}
