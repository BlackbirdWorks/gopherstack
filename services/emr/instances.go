package emr

import (
	"context"
	"fmt"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// ListInstances synthesizes per-group instances for a cluster.
func (b *InMemoryBackend) ListInstances(
	ctx context.Context,
	clusterID string,
	params ListInstancesParams,
) ([]ClusterInstance, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListInstances")
	defer b.mu.RUnlock()

	cluster, ok := b.clusterGet(region, clusterID)
	if !ok {
		return []ClusterInstance{}, ""
	}

	instances := buildInstanceList(cluster, params)

	p := page.New(instances, params.Marker, listInstancesPageSize, listInstancesPageSize)

	return p.Data, p.Next
}

// fleetFilterSet reports whether params names a fleet specifically -- when
// it does, group instances are excluded from the result (a client asking
// for a given fleet, or fleets of a given node type, is not expecting
// instance-group instances back), and vice versa for group filters.
func fleetFilterSet(params ListInstancesParams) bool {
	return params.InstanceFleetID != "" || params.InstanceFleetType != ""
}

func groupFilterSet(params ListInstancesParams) bool {
	return params.InstanceGroupID != "" || len(params.InstanceGroupTypes) > 0
}

func buildInstanceList(cluster *Cluster, params ListInstancesParams) []ClusterInstance {
	var instances []ClusterInstance

	idx := 0

	if !fleetFilterSet(params) {
		instances = appendGroupInstances(instances, cluster, params, &idx)
	}

	if !groupFilterSet(params) {
		instances = appendFleetInstances(instances, cluster, params, &idx)
	}

	return instances
}

func appendGroupInstances(
	instances []ClusterInstance, cluster *Cluster, params ListInstancesParams, idx *int,
) []ClusterInstance {
	for _, grp := range cluster.instanceGroups {
		if !instanceGroupMatchesParams(grp, params) {
			continue
		}

		for range grp.RunningInstanceCount {
			inst := synthesizeGroupInstance(cluster.ID, grp, *idx)
			if instanceStateMatches(inst.Status.State, params.InstanceStates) {
				instances = append(instances, inst)
			}

			*idx++
		}
	}

	return instances
}

func appendFleetInstances(
	instances []ClusterInstance, cluster *Cluster, params ListInstancesParams, idx *int,
) []ClusterInstance {
	for _, fleet := range cluster.instanceFleets {
		if !instanceFleetMatchesParams(fleet, params) {
			continue
		}

		instances = appendFleetMarketInstances(
			instances, cluster.ID, fleet, fleet.ProvisionedOnDemandCapacity, "ON_DEMAND", params, idx)
		instances = appendFleetMarketInstances(
			instances, cluster.ID, fleet, fleet.ProvisionedSpotCapacity, "SPOT", params, idx)
	}

	return instances
}

func appendFleetMarketInstances(
	instances []ClusterInstance,
	clusterID string,
	fleet InstanceFleet,
	count int,
	market string,
	params ListInstancesParams,
	idx *int,
) []ClusterInstance {
	for range count {
		inst := synthesizeFleetInstance(clusterID, fleet, *idx, market)
		if instanceStateMatches(inst.Status.State, params.InstanceStates) {
			instances = append(instances, inst)
		}

		*idx++
	}

	return instances
}

func instanceGroupMatchesParams(grp InstanceGroup, params ListInstancesParams) bool {
	if params.InstanceGroupID != "" && grp.ID != params.InstanceGroupID {
		return false
	}

	if len(params.InstanceGroupTypes) > 0 &&
		!slices.Contains(params.InstanceGroupTypes, grp.InstanceGroupType) {
		return false
	}

	return true
}

func instanceFleetMatchesParams(fleet InstanceFleet, params ListInstancesParams) bool {
	if params.InstanceFleetID != "" && fleet.ID != params.InstanceFleetID {
		return false
	}

	if params.InstanceFleetType != "" && fleet.InstanceFleetType != params.InstanceFleetType {
		return false
	}

	return true
}

func instanceStateMatches(state string, wanted []string) bool {
	return len(wanted) == 0 || slices.Contains(wanted, state)
}

func synthesizeGroupInstance(clusterID string, grp InstanceGroup, idx int) ClusterInstance {
	id := fmt.Sprintf("ci-%s-%d", clusterID, idx)
	ec2ID := fmt.Sprintf("i-%016x", idx+1)
	privateDNS := fmt.Sprintf("ip-10-0-0-%d.ec2.internal", idx+1)

	return ClusterInstance{
		ID:              id,
		Ec2InstanceID:   ec2ID,
		PrivateDNSName:  privateDNS,
		Market:          grp.Market,
		InstanceType:    grp.InstanceType,
		InstanceGroupID: grp.ID,
		Status:          ClusterInstanceStatus{State: grp.Status.State},
	}
}

// synthesizeFleetInstance builds a ClusterInstance for an instance fleet
// member. InstanceType is left blank: unlike InstanceGroup, InstanceFleet
// only tracks aggregate target/provisioned capacity, not a per-instance-type
// breakdown (InstanceTypeConfigs is accepted on AddInstanceFleet's wire
// input but not modeled), so there is no real per-instance type to report.
func synthesizeFleetInstance(clusterID string, fleet InstanceFleet, idx int, market string) ClusterInstance {
	id := fmt.Sprintf("ci-%s-%d", clusterID, idx)
	ec2ID := fmt.Sprintf("i-%016x", idx+1)
	privateDNS := fmt.Sprintf("ip-10-0-0-%d.ec2.internal", idx+1)

	return ClusterInstance{
		ID:              id,
		Ec2InstanceID:   ec2ID,
		PrivateDNSName:  privateDNS,
		Market:          market,
		InstanceFleetID: fleet.ID,
		Status:          ClusterInstanceStatus{State: fleet.Status.State},
	}
}
