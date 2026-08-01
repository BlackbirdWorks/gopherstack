package docdb

import (
	"context"
	"sort"
	"strings"
)

func (b *InMemoryBackend) AddTagsToResource(ctx context.Context, arnStr string, tags []Tag) error {
	if err := validateTagList(tags); err != nil {
		return err
	}
	region := regionFromARN(arnStr, getRegion(ctx, b.region))
	b.mu.Lock("AddTagsToResource")
	defer b.mu.Unlock()
	tagStore := b.tagsStore(region)
	current := tagStore[arnStr]
	idx := make(map[string]int, len(current))
	for i, t := range current {
		idx[t.Key] = i
	}
	for _, t := range tags {
		if i, ok := idx[t.Key]; ok {
			current[i].Value = t.Value
		} else {
			idx[t.Key] = len(current)
			current = append(current, t)
		}
	}
	tagStore[arnStr] = current

	return nil
}

func (b *InMemoryBackend) RemoveTagsFromResource(ctx context.Context, arnStr string, keys []string) {
	region := regionFromARN(arnStr, getRegion(ctx, b.region))
	b.mu.Lock("RemoveTagsFromResource")
	defer b.mu.Unlock()
	tagStore := b.tagsStore(region)
	remove := make(map[string]bool, len(keys))
	for _, k := range keys {
		remove[k] = true
	}
	current := tagStore[arnStr]
	kept := make([]Tag, 0, len(current))
	for _, t := range current {
		if !remove[t.Key] {
			kept = append(kept, t)
		}
	}
	tagStore[arnStr] = kept
}

func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, arnStr string) []Tag {
	region := regionFromARN(arnStr, getRegion(ctx, b.region))
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()
	src := b.tagsStoreRO(region)[arnStr]
	cp := make([]Tag, len(src))
	copy(cp, src)
	sort.Slice(cp, func(i, j int) bool {
		return cp[i].Key < cp[j].Key
	})

	return cp
}

// TaggedEntry pairs a resource ARN with its tag map, for cross-service tag
// enumeration by the Resource Groups Tagging API (see cli.go's
// wireTaggingDocDB). DocDB keeps tags in a region-nested, flat ARN-keyed map
// (b.tags[region][arn]) covering every resource kind it supports, so this is
// a walk of that map across every observed region rather than one per-kind
// loop.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every DocDB resource ARN, across all regions, that
// currently has at least one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	for _, regionTags := range b.tags {
		for arnStr, tagList := range regionTags {
			if len(tagList) == 0 {
				continue
			}

			kv := make(map[string]string, len(tagList))
			for _, t := range tagList {
				kv[t.Key] = t.Value
			}

			out = append(out, TaggedEntry{ARN: arnStr, Tags: kv})
		}
	}

	return out
}

// arnResourcePartCount is the minimum number of colon-delimited fields in a
// well-formed DocDB resource ARN: "arn:partition:service:region:account:type:id".
const arnResourcePartCount = 7

// HasTaggableResource reports whether arnStr refers to a resource DocDB
// actually stores, resolving the region from the ARN (falling back to ctx)
// exactly as AddTagsToResource does. AddTagsToResource/RemoveTagsFromResource
// above do not themselves validate that an ARN corresponds to a real
// resource -- they accept and store tags for any ARN, matching this
// backend's existing permissive semantics -- so this exists solely for
// cross-service tagging (resourcegroupstaggingapi, wired in cli.go's
// wireTaggingDocDB): every DocumentDB resource kind builds its ARN under the
// "rds" ARN service (clusterARN, instanceARN, subnetGroupARN,
// clusterParameterGroupARN, clusterSnapshotARN, globalClusterARN, and
// eventSubscriptionARN in store.go:232-266), the same service segment the
// separate RDS backend claims, so ownership must be checked explicitly
// before a cross-service TagResources call is allowed to claim an ARN that
// might really belong to RDS.
func (b *InMemoryBackend) HasTaggableResource(ctx context.Context, arnStr string) bool {
	parts := strings.SplitN(arnStr, ":", arnResourcePartCount)
	if len(parts) != arnResourcePartCount {
		return false
	}

	region := regionFromARN(arnStr, getRegion(ctx, b.region))
	resType, resID := parts[5], parts[6]

	b.mu.RLock("HasTaggableResource")
	defer b.mu.RUnlock()

	switch resType {
	case "cluster":
		return b.clusterHas(region, resID)
	case "db":
		return b.instanceHas(region, resID)
	case "subgrp":
		return b.subnetGroupHas(region, resID)
	case "cluster-pg":
		return b.clusterParameterGroupHas(region, resID)
	case "cluster-snapshot":
		return b.clusterSnapshotHas(region, resID)
	case "es":
		return b.eventSubscriptionHas(region, resID)
	case "global-cluster":
		return b.globalClusters.Has(resID)
	default:
		return false
	}
}
