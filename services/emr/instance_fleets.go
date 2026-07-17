package emr

import (
	"context"
	"fmt"
)

// AddInstanceFleet adds an instance fleet to an existing cluster.
func (b *InMemoryBackend) AddInstanceFleet(
	ctx context.Context,
	clusterID string,
	spec InstanceFleetSpec,
) (*InstanceFleet, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("AddInstanceFleet")
	defer b.mu.Unlock()

	cluster, ok := b.clusterGet(region, clusterID)
	if !ok {
		return nil, "", fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	fleet := InstanceFleet{
		ID:                          b.nextFleetID(),
		Name:                        spec.Name,
		InstanceFleetType:           spec.InstanceFleetType,
		TargetOnDemandCapacity:      spec.TargetOnDemandCapacity,
		TargetSpotCapacity:          spec.TargetSpotCapacity,
		ProvisionedOnDemandCapacity: spec.TargetOnDemandCapacity,
		ProvisionedSpotCapacity:     spec.TargetSpotCapacity,
		Status:                      InstanceFleetStatus{State: instanceGroupStateRunning},
	}

	cluster.instanceFleets = append(cluster.instanceFleets, fleet)

	return &fleet, cluster.ARN, nil
}

// ModifyInstanceFleet updates target capacities on an instance fleet.
func (b *InMemoryBackend) ModifyInstanceFleet(
	ctx context.Context,
	clusterID string,
	mod InstanceFleetModification,
) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("ModifyInstanceFleet")
	defer b.mu.Unlock()

	cluster, ok := b.clusterGet(region, clusterID)
	if !ok {
		return fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	for i := range cluster.instanceFleets {
		if cluster.instanceFleets[i].ID == mod.InstanceFleetID {
			applyInstanceFleetMod(&cluster.instanceFleets[i], mod)

			return nil
		}
	}

	return fmt.Errorf("%w: instance fleet %s not found", ErrNotFound, mod.InstanceFleetID)
}

// applyInstanceFleetMod updates fleet's target (and, since gopherstack
// provisions instantly, provisioned) capacities from mod. A zero value in
// either field means "not specified" (the JSON field is plain int, not a
// pointer, so unset and explicit-zero are indistinguishable on the wire) --
// matching the real API, where either capacity may be omitted to leave it
// unchanged.
func applyInstanceFleetMod(fleet *InstanceFleet, mod InstanceFleetModification) {
	if mod.TargetOnDemandCapacity > 0 {
		fleet.TargetOnDemandCapacity = mod.TargetOnDemandCapacity
		fleet.ProvisionedOnDemandCapacity = mod.TargetOnDemandCapacity
	}

	if mod.TargetSpotCapacity > 0 {
		fleet.TargetSpotCapacity = mod.TargetSpotCapacity
		fleet.ProvisionedSpotCapacity = mod.TargetSpotCapacity
	}
}

// ListInstanceFleets returns the instance fleets for a cluster by its ID.
func (b *InMemoryBackend) ListInstanceFleets(ctx context.Context, clusterID string) ([]InstanceFleet, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListInstanceFleets")
	defer b.mu.RUnlock()

	cluster, ok := b.clusterGet(region, clusterID)
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	fleets := make([]InstanceFleet, len(cluster.instanceFleets))
	copy(fleets, cluster.instanceFleets)

	return fleets, nil
}
