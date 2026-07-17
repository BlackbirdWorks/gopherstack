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

func buildInstanceList(cluster *Cluster, params ListInstancesParams) []ClusterInstance {
	var instances []ClusterInstance
	idx := 0

	for _, grp := range cluster.instanceGroups {
		if !instanceGroupMatchesParams(grp, params) {
			continue
		}

		for range grp.RunningInstanceCount {
			instances = append(instances, synthesizeInstance(cluster.ID, grp, idx))
			idx++
		}
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

func synthesizeInstance(clusterID string, grp InstanceGroup, idx int) ClusterInstance {
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
