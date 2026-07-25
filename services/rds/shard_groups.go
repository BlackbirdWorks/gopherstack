package rds

import (
	"fmt"
	"slices"
)

// CreateDBShardGroup creates a new Aurora Limitless DB shard group.
func (b *InMemoryBackend) CreateDBShardGroup(
	id, clusterID string,
	maxACU float64,
	minACU float64,
	computeRedundancy int,
	publiclyAccessible bool,
) (*DBShardGroup, error) {
	b.mu.Lock("CreateDBShardGroup")
	defer b.mu.Unlock()

	if id == "" {
		return nil, fmt.Errorf("%w: DBShardGroupIdentifier is required", ErrInvalidParameter)
	}
	if _, exists := b.shardGroups.Get(id); exists {
		return nil, fmt.Errorf("%w: %s", ErrDBShardGroupAlreadyExists, id)
	}
	if clusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}

	sg := &DBShardGroup{
		DBShardGroupIdentifier: id,
		DBClusterIdentifier:    clusterID,
		DBShardGroupArn:        b.rdsARN("shard-group", id),
		DBShardGroupResourceID: "shardgroup-" + id,
		MaxACU:                 maxACU,
		MinACU:                 minACU,
		ComputeRedundancy:      computeRedundancy,
		PubliclyAccessible:     publiclyAccessible,
		Status:                 shardGroupStatusAvailableInternal,
		Endpoint:               id + ".limitless." + clusterID + ".rds.amazonaws.com",
	}
	b.shardGroups.Put(sg)
	cp := *sg

	return &cp, nil
}

// DeleteDBShardGroup deletes a DB shard group.
func (b *InMemoryBackend) DeleteDBShardGroup(id string) (*DBShardGroup, error) {
	b.mu.Lock("DeleteDBShardGroup")
	defer b.mu.Unlock()

	sg, exists := b.shardGroups.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrDBShardGroupNotFound, id)
	}

	cp := *sg
	cp.Status = shardGroupStatusDeletingInternal
	b.shardGroups.Delete(id)

	return &cp, nil
}

// DescribeDBShardGroups returns DB shard groups, optionally filtered by ID.
func (b *InMemoryBackend) DescribeDBShardGroups(id string) ([]DBShardGroup, error) {
	b.mu.RLock("DescribeDBShardGroups")
	defer b.mu.RUnlock()

	result := make([]DBShardGroup, 0, b.shardGroups.Len())
	for _, sg := range b.shardGroups.All() {
		if id != "" && sg.DBShardGroupIdentifier != id {
			continue
		}
		result = append(result, *sg)
	}

	if id != "" && len(result) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrDBShardGroupNotFound, id)
	}

	slices.SortFunc(result, func(a, b DBShardGroup) int {
		if a.DBShardGroupIdentifier < b.DBShardGroupIdentifier {
			return -1
		}
		if a.DBShardGroupIdentifier > b.DBShardGroupIdentifier {
			return 1
		}

		return 0
	})

	return result, nil
}

// ModifyDBShardGroup modifies a DB shard group's settings.
func (b *InMemoryBackend) ModifyDBShardGroup(
	id string,
	maxACU float64,
	computeRedundancy int,
) (*DBShardGroup, error) {
	b.mu.Lock("ModifyDBShardGroup")
	defer b.mu.Unlock()

	sg, exists := b.shardGroups.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrDBShardGroupNotFound, id)
	}

	if maxACU > 0 {
		sg.MaxACU = maxACU
	}
	if computeRedundancy >= 0 {
		sg.ComputeRedundancy = computeRedundancy
	}
	cp := *sg

	return &cp, nil
}

// RebootDBShardGroup reboots a DB shard group.
func (b *InMemoryBackend) RebootDBShardGroup(id string) (*DBShardGroup, error) {
	b.mu.Lock("RebootDBShardGroup")
	defer b.mu.Unlock()

	sg, exists := b.shardGroups.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrDBShardGroupNotFound, id)
	}

	sg.Status = shardGroupStatusRebootingInternal
	cp := *sg
	sg.Status = shardGroupStatusAvailableInternal

	return &cp, nil
}

const (
	shardGroupStatusAvailableInternal = instanceStatusAvailable
	shardGroupStatusDeletingInternal  = instanceStatusDeleting
	shardGroupStatusRebootingInternal = "rebooting"
)
