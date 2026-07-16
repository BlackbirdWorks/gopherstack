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

// validateResourceARN checks whether an ARN refers to a known Neptune resource in the given region.
// Must be called while holding at least a read lock.
func (b *InMemoryBackend) validateResourceARN(region, arnStr string) error {
	// ARN format: arn:partition:service:region:account:type:id
	parts := strings.SplitN(arnStr, ":", arnPartCount)
	if len(parts) < arnPartCount {
		return fmt.Errorf("%w: invalid ARN format: %s", ErrInvalidParameter, arnStr)
	}
	resType, resID := parts[5], parts[6]
	switch resType {
	case "cluster":
		if !b.clusterHas(region, resID) {
			return fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, resID)
		}
	case "db":
		if !b.instanceHas(region, resID) {
			return fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, resID)
		}
	case "cluster-snapshot":
		if !b.clusterSnapshotHas(region, resID) {
			return fmt.Errorf(
				"%w: cluster snapshot %s not found",
				ErrClusterSnapshotNotFound,
				resID,
			)
		}
	case "subgrp":
		if !b.subnetGroupHas(region, resID) {
			return fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, resID)
		}
	case "cluster-pg":
		if !b.clusterParameterGroupHas(region, resID) {
			return fmt.Errorf(
				"%w: cluster parameter group %s not found",
				ErrClusterParameterGroupNotFound,
				resID,
			)
		}
	default:
		return fmt.Errorf("%w: unsupported resource type in ARN: %s", ErrInvalidParameter, arnStr)
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
	src := b.tagsStore(region)[arnStr]
	cp := make([]Tag, len(src))
	copy(cp, src)

	return cp, nil
}
