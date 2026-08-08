package neptune

import (
	"context"
	"fmt"
	"strings"
)

func (b *InMemoryBackend) tagsStore(region string) map[string][]Tag {
	if b.tags[region] == nil {
		b.tags[region] = make(map[string][]Tag)
	}

	return b.tags[region]
}

// tagsStoreRO returns the region-scoped tags map for region without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty map
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) tagsStoreRO(region string) map[string][]Tag {
	if v := b.tags[region]; v != nil {
		return v
	}

	return map[string][]Tag{}
}

// taggableResourceKind describes one ARN resource-type segment that
// AddTagsToResource/RemoveTagsFromResource/ListTagsForResource accept,
// keeping validateResourceARN a flat lookup instead of a growing switch.
type taggableResourceKind struct {
	has      func(b *InMemoryBackend, region, resID string) bool
	notFound error
	label    string
}

// neptuneTaggableKinds maps every ARN resource-type segment Neptune stores
// tags for to its existence check. A kind missing here is exactly the
// memorydb-shaped bug (gopherstack-2mwl): TagResource/CreateXxx's tags are
// silently accepted by AddTagsToResource's caller but ListTagsForResource
// (which also runs through validateResourceARN) then errors "unsupported
// resource type", so the caller-visible failure mode is a loud 400, not a
// silent drop -- still wrong, and still the same missing-case root cause.
//
//nolint:gochecknoglobals // read-only lookup table initialized once at startup
var neptuneTaggableKinds = map[string]taggableResourceKind{
	"cluster": {
		has:      (*InMemoryBackend).clusterHas,
		notFound: ErrClusterNotFound,
		label:    "cluster",
	},
	"db": {
		has:      (*InMemoryBackend).instanceHas,
		notFound: ErrInstanceNotFound,
		label:    "instance",
	},
	"cluster-snapshot": {
		has:      (*InMemoryBackend).clusterSnapshotHas,
		notFound: ErrClusterSnapshotNotFound,
		label:    "cluster snapshot",
	},
	"subgrp": {
		has:      (*InMemoryBackend).subnetGroupHas,
		notFound: ErrSubnetGroupNotFound,
		label:    "subnet group",
	},
	"cluster-pg": {
		has:      (*InMemoryBackend).clusterParameterGroupHas,
		notFound: ErrClusterParameterGroupNotFound,
		label:    "cluster parameter group",
	},
	"cluster-endpoint": {
		has:      (*InMemoryBackend).clusterEndpointHas,
		notFound: ErrClusterEndpointNotFound,
		label:    "cluster endpoint",
	},
	"pg": {
		has:      (*InMemoryBackend).parameterGroupHas,
		notFound: ErrParameterGroupNotFound,
		label:    "parameter group",
	},
	"es": {
		has:      (*InMemoryBackend).eventSubscriptionHas,
		notFound: ErrSubscriptionNotFound,
		label:    "event subscription",
	},
	"global-cluster": {
		has:      func(b *InMemoryBackend, _, resID string) bool { return b.globalClusters.Has(resID) },
		notFound: ErrGlobalClusterNotFound,
		label:    "global cluster",
	},
}

// validateResourceARN checks whether an ARN refers to a known Neptune resource in the given region.
// Must be called while holding at least a read lock.
func (b *InMemoryBackend) validateResourceARN(region, arnStr string) error {
	// ARN format: arn:partition:service:region:account:type:id
	parts := strings.SplitN(arnStr, ":", arnPartCount)
	if len(parts) < arnPartCount {
		return fmt.Errorf("%w: invalid ARN format: %s", ErrInvalidParameter, arnStr)
	}
	resType, resID := parts[5], parts[6]
	kind, ok := neptuneTaggableKinds[resType]
	if !ok {
		return fmt.Errorf("%w: unsupported resource type in ARN: %s", ErrInvalidParameter, arnStr)
	}
	if !kind.has(b, region, resID) {
		return fmt.Errorf("%w: %s %s not found", kind.notFound, kind.label, resID)
	}

	return nil
}

// AddTagsToResource adds or updates tags on a Neptune resource.
// The resource's region is resolved from the ARN, falling back to the ctx region.
func (b *InMemoryBackend) AddTagsToResource(ctx context.Context, arnStr string, tags []Tag) error {
	region := regionFromARN(arnStr, getRegion(ctx, b.region))
	b.mu.Lock("AddTagsToResource")
	defer b.mu.Unlock()
	if err := b.validateResourceARN(region, arnStr); err != nil {
		return err
	}
	for _, t := range tags {
		if len(t.Key) == 0 || len(t.Key) > maxTagKeyLen {
			return fmt.Errorf(
				"%w: tag key must be 1-%d characters",
				ErrInvalidParameter,
				maxTagKeyLen,
			)
		}
		if len(t.Value) > maxTagValueLen {
			return fmt.Errorf(
				"%w: tag value must be 0-%d characters",
				ErrInvalidParameter,
				maxTagValueLen,
			)
		}
	}
	tagStore := b.tagsStore(region)
	current := tagStore[arnStr]
	idx := make(map[string]int, len(current))
	for i, t := range current {
		idx[t.Key] = i
	}
	newCount := len(current)
	for _, t := range tags {
		if _, exists := idx[t.Key]; !exists {
			newCount++
		}
	}
	if newCount > maxTagsPerResource {
		return fmt.Errorf(
			"%w: resource cannot have more than %d tags",
			ErrInvalidParameter,
			maxTagsPerResource,
		)
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

// RemoveTagsFromResource removes tags from a Neptune resource.
func (b *InMemoryBackend) RemoveTagsFromResource(
	ctx context.Context,
	arnStr string,
	keys []string,
) error {
	region := regionFromARN(arnStr, getRegion(ctx, b.region))
	b.mu.Lock("RemoveTagsFromResource")
	defer b.mu.Unlock()
	if err := b.validateResourceARN(region, arnStr); err != nil {
		return err
	}
	remove := make(map[string]bool, len(keys))
	for _, k := range keys {
		remove[k] = true
	}
	tagStore := b.tagsStore(region)
	current := tagStore[arnStr]
	kept := make([]Tag, 0, len(current))
	for _, t := range current {
		if !remove[t.Key] {
			kept = append(kept, t)
		}
	}
	tagStore[arnStr] = kept

	return nil
}

// ListTagsForResource returns the tags for a Neptune resource.
func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, arnStr string) ([]Tag, error) {
	region := regionFromARN(arnStr, getRegion(ctx, b.region))
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()
	if err := b.validateResourceARN(region, arnStr); err != nil {
		return nil, err
	}
	src := b.tagsStoreRO(region)[arnStr]
	cp := make([]Tag, len(src))
	copy(cp, src)

	return cp, nil
}

// TaggedEntry pairs a resource ARN with its tag map, for cross-service tag
// enumeration by the Resource Groups Tagging API (see cli.go's
// wireTaggingNeptune). Neptune keeps tags in a region-nested, flat ARN-keyed
// map (b.tags[region][arn]) spanning every taggable resource kind, so this
// is a walk of that map across every observed region rather than one
// per-kind loop.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every Neptune resource ARN, across all regions,
// that currently has at least one tag.
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

// HasTaggableResource reports whether arnStr refers to a Neptune-owned
// resource of a kind AddTagsToResource actually recognizes (cluster, db,
// cluster-snapshot, subgrp, cluster-pg -- see validateResourceARN),
// resolving the region exactly as AddTagsToResource does. Used exclusively
// by cross-service tagging (resourcegroupstaggingapi, wired in cli.go's
// wireTaggingNeptune) to decide whether a "rds"-namespaced ARN belongs to
// Neptune before claiming it: DB clusters and instances use the "neptune"
// ARN service exclusively (db_clusters.go:70, db_instances.go:32), but
// parameter groups, subnet groups, and cluster snapshots share the "rds" ARN
// service (cluster_parameter_groups.go:47, subnet_groups.go:41,
// cluster_snapshots.go:44) with the separate RDS and DocumentDB backends, so
// ownership can't be assumed from the service segment alone.
func (b *InMemoryBackend) HasTaggableResource(ctx context.Context, arnStr string) bool {
	region := regionFromARN(arnStr, getRegion(ctx, b.region))

	b.mu.RLock("HasTaggableResource")
	defer b.mu.RUnlock()

	return b.validateResourceARN(region, arnStr) == nil
}
