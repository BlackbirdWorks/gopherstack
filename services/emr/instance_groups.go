package emr

import (
	"context"
	"fmt"
)

// buildInstanceGroups converts input specs to InstanceGroup records.
func (b *InMemoryBackend) buildInstanceGroups(specs []InstanceGroupSpec) []InstanceGroup {
	groups := make([]InstanceGroup, 0, len(specs))

	for _, spec := range specs {
		market := spec.Market
		if market == "" {
			market = "ON_DEMAND"
		}

		groups = append(groups, InstanceGroup{
			ID:                     fmt.Sprintf("ig-%013d", b.counter.Add(1)),
			Name:                   spec.Name,
			Market:                 market,
			BidPrice:               spec.BidPrice,
			InstanceGroupType:      spec.InstanceRole,
			InstanceType:           spec.InstanceType,
			Configurations:         cloneConfigurations(spec.Configurations),
			RequestedInstanceCount: spec.InstanceCount,
			RunningInstanceCount:   spec.InstanceCount,
			Status:                 InstanceGroupStatus{State: instanceGroupStateRunning},
		})
	}

	return groups
}

// ListInstanceGroups returns the instance groups for a cluster by its ID.
func (b *InMemoryBackend) ListInstanceGroups(ctx context.Context, clusterID string) ([]InstanceGroup, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListInstanceGroups")
	defer b.mu.RUnlock()

	cluster, ok := b.clusterGet(region, clusterID)
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	groups := make([]InstanceGroup, len(cluster.instanceGroups))
	copy(groups, cluster.instanceGroups)

	return groups, nil
}

// AddInstanceGroups adds new instance groups to an existing cluster.
func (b *InMemoryBackend) AddInstanceGroups(
	ctx context.Context,
	clusterID string,
	specs []InstanceGroupSpec,
) ([]string, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("AddInstanceGroups")
	defer b.mu.Unlock()

	cluster, ok := b.clusterGet(region, clusterID)
	if !ok {
		return nil, "", fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	groupIDs := make([]string, 0, len(specs))

	for _, spec := range specs {
		market := spec.Market
		if market == "" {
			market = "ON_DEMAND"
		}

		grpID := fmt.Sprintf("ig-%013d", b.counter.Add(1))
		group := InstanceGroup{
			ID:                     grpID,
			Name:                   spec.Name,
			Market:                 market,
			InstanceGroupType:      spec.InstanceRole,
			InstanceType:           spec.InstanceType,
			RequestedInstanceCount: spec.InstanceCount,
			RunningInstanceCount:   spec.InstanceCount,
			Status:                 InstanceGroupStatus{State: instanceGroupStateRunning},
		}

		cluster.instanceGroups = append(cluster.instanceGroups, group)
		groupIDs = append(groupIDs, grpID)
	}

	return groupIDs, cluster.ARN, nil
}

// ModifyInstanceGroups updates instance counts for the specified groups.
func (b *InMemoryBackend) ModifyInstanceGroups(
	ctx context.Context,
	clusterID string,
	mods []InstanceGroupModification,
) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("ModifyInstanceGroups")
	defer b.mu.Unlock()

	cluster, ok := b.clusterGet(region, clusterID)
	if !ok {
		return fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	for _, mod := range mods {
		applyInstanceGroupMod(cluster, mod)
	}

	return nil
}

func applyInstanceGroupMod(cluster *Cluster, mod InstanceGroupModification) {
	for i := range cluster.instanceGroups {
		if cluster.instanceGroups[i].ID == mod.InstanceGroupID {
			cluster.instanceGroups[i].RequestedInstanceCount = mod.InstanceCount
			cluster.instanceGroups[i].RunningInstanceCount = mod.InstanceCount

			return
		}
	}
}
