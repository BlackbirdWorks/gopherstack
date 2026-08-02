package elasticache

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// tagEntry holds the tags pointer and the metric name used to initialise tags when nil.
type tagEntry struct {
	ptr      **tags.Tags
	initName string
}

// tagCandidate bundles an ARN with the tagEntry to return when it matches.
type tagCandidate struct {
	arn   string
	entry tagEntry
}

// collectTagCandidatesLocked builds a flat list of all taggable resources for ARN lookup.
// It iterates over all regions so that ARN-addressed operations find the correct resource.
func (b *InMemoryBackend) collectTagCandidatesLocked() []tagCandidate {
	candidates := make([]tagCandidate, 0, tagCandidateInitCap)
	candidates = b.appendClusterTagCandidates(candidates)
	candidates = b.appendNetworkTagCandidates(candidates)
	candidates = b.appendServerlessTagCandidates(candidates)
	candidates = b.appendUserTagCandidates(candidates)

	return candidates
}

func (b *InMemoryBackend) appendClusterTagCandidates(candidates []tagCandidate) []tagCandidate {
	for _, regionClusters := range b.clusters {
		for _, c := range regionClusters.All() {
			candidates = append(candidates,
				tagCandidate{c.ARN, tagEntry{&c.Tags, "elasticache.cluster." + c.ClusterID + ".tags"}})
		}
	}
	for _, regionRGs := range b.replicationGroups {
		for _, rg := range regionRGs.All() {
			candidates = append(candidates,
				tagCandidate{rg.ARN, tagEntry{&rg.Tags, "elasticache.rg." + rg.ReplicationGroupID + ".tags"}})
		}
	}
	for _, regionPGs := range b.parameterGroups {
		for _, pg := range regionPGs.All() {
			candidates = append(candidates,
				tagCandidate{pg.ARN, tagEntry{&pg.Tags, "elasticache.pg." + pg.Name + ".tags"}})
		}
	}
	for _, regionSnaps := range b.snapshots {
		for _, snap := range regionSnaps.All() {
			candidates = append(candidates,
				tagCandidate{snap.ARN, tagEntry{&snap.Tags, "elasticache.snapshot." + snap.SnapshotName + ".tags"}})
		}
	}
	for _, regionCSGs := range b.cacheSecurityGroups {
		for _, sg := range regionCSGs.All() {
			candidates = append(candidates,
				tagCandidate{sg.ARN, tagEntry{&sg.Tags, "elasticache.sg." + sg.Name + ".tags"}})
		}
	}
	for _, grg := range b.globalReplicationGroups.All() {
		candidates = append(candidates,
			tagCandidate{grg.ARN, tagEntry{&grg.Tags, "elasticache.grg." + grg.GlobalReplicationGroupID + ".tags"}})
	}

	return candidates
}

func (b *InMemoryBackend) appendNetworkTagCandidates(candidates []tagCandidate) []tagCandidate {
	for _, regionSGs := range b.subnetGroups {
		for _, sg := range regionSGs.All() {
			candidates = append(candidates,
				tagCandidate{sg.ARN, tagEntry{&sg.Tags, "elasticache.sg." + sg.Name + ".tags"}})
		}
	}

	return candidates
}

func (b *InMemoryBackend) appendServerlessTagCandidates(candidates []tagCandidate) []tagCandidate {
	for _, regionSCs := range b.serverlessCaches {
		for _, sc := range regionSCs.All() {
			candidates = append(candidates,
				tagCandidate{sc.ARN, tagEntry{&sc.Tags, "elasticache.serverless." + sc.Name + ".tags"}})
		}
	}
	for _, regionScSnaps := range b.serverlessCacheSnapshots {
		for _, snap := range regionScSnaps.All() {
			candidates = append(candidates,
				tagCandidate{snap.ARN, tagEntry{&snap.Tags, "elasticache.serverlesssnap." + snap.Name + ".tags"}})
		}
	}

	return candidates
}

func (b *InMemoryBackend) appendUserTagCandidates(candidates []tagCandidate) []tagCandidate {
	for _, regionUsers := range b.users {
		for _, u := range regionUsers.All() {
			candidates = append(candidates,
				tagCandidate{u.ARN, tagEntry{&u.Tags, "elasticache.user." + u.UserID + ".tags"}})
		}
	}
	for _, regionUGs := range b.userGroups {
		for _, ug := range regionUGs.All() {
			candidates = append(candidates,
				tagCandidate{ug.ARN, tagEntry{&ug.Tags, "elasticache.usergroup." + ug.UserGroupID + ".tags"}})
		}
	}

	return candidates
}

// findTagsByARNLocked returns the tagEntry for the resource with the given ARN, or nil if not found.
func (b *InMemoryBackend) findTagsByARNLocked(arn string) *tagEntry {
	for _, c := range b.collectTagCandidatesLocked() {
		if c.arn == arn {
			entry := c.entry

			return &entry
		}
	}

	return nil
}

// ListTagsForResource returns tags for the given ARN.
func (b *InMemoryBackend) ListTagsForResource(_ context.Context, arn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	entry := b.findTagsByARNLocked(arn)
	if entry == nil {
		return nil, fmt.Errorf("resource with ARN %s: %w", arn, ErrResourceNotFound)
	}

	if *entry.ptr == nil {
		return map[string]string{}, nil
	}

	return (*entry.ptr).Clone(), nil
}

// AddTagsToResource adds or updates tags on the resource identified by resourceARN.
func (b *InMemoryBackend) AddTagsToResource(_ context.Context, resourceARN string, newTags map[string]string) error {
	b.mu.Lock("AddTagsToResource")
	defer b.mu.Unlock()

	entry := b.findTagsByARNLocked(resourceARN)
	if entry == nil {
		return fmt.Errorf("resource with ARN %s: %w", resourceARN, ErrResourceNotFound)
	}

	if *entry.ptr == nil {
		*entry.ptr = tags.FromMap(entry.initName, newTags)
	} else {
		(*entry.ptr).Merge(newTags)
	}

	return nil
}

// RemoveTagsFromResource removes the specified tag keys from the resource identified by resourceARN.
func (b *InMemoryBackend) RemoveTagsFromResource(_ context.Context, resourceARN string, tagKeys []string) error {
	b.mu.Lock("RemoveTagsFromResource")
	defer b.mu.Unlock()

	entry := b.findTagsByARNLocked(resourceARN)
	if entry == nil {
		return fmt.Errorf("resource with ARN %s: %w", resourceARN, ErrResourceNotFound)
	}

	if *entry.ptr != nil {
		(*entry.ptr).DeleteKeys(tagKeys)
	}

	return nil
}

// TaggedEntry pairs a resource ARN with its tag map, for cross-service tag
// enumeration by the Resource Groups Tagging API (see cli.go's
// wireTaggingElastiCache).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every ElastiCache resource ARN that currently has
// at least one tag, across every taggable resource kind (see
// collectTagCandidatesLocked).
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	for _, c := range b.collectTagCandidatesLocked() {
		t := *c.entry.ptr
		if t == nil || t.Len() == 0 {
			continue
		}

		out = append(out, TaggedEntry{ARN: c.arn, Tags: t.Clone()})
	}

	return out
}
