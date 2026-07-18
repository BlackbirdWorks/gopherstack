package emr

import (
	"context"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// DescribeJobFlows translates clusters into the legacy JobFlow format.
func (b *InMemoryBackend) DescribeJobFlows(
	ctx context.Context,
	ids, states []string,
	createdAfter, createdBefore *time.Time,
) []JobFlow {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeJobFlows")
	defer b.mu.RUnlock()

	idSet := buildStringSet(ids)
	stateSet := buildStateSet(states)

	flows := make([]JobFlow, 0)

	for _, c := range b.clustersInRegion(region) {
		if !jobFlowMatchesFilter(c, idSet, stateSet, createdAfter, createdBefore) {
			continue
		}

		flows = append(flows, clusterToJobFlow(c))
	}

	sort.Slice(flows, func(i, j int) bool {
		return flows[i].JobFlowID < flows[j].JobFlowID
	})

	return flows
}

func jobFlowMatchesFilter(
	c *Cluster,
	idSet, stateSet map[string]bool,
	createdAfter, createdBefore *time.Time,
) bool {
	if idSet != nil && !idSet[c.ID] {
		return false
	}

	if stateSet != nil && !stateSet[c.Status.State] {
		return false
	}

	creationSeconds := clusterCreationSecondsFromCluster(c)
	if createdAfter != nil && creationSeconds < awstime.Epoch(*createdAfter) {
		return false
	}

	if createdBefore != nil && creationSeconds > awstime.Epoch(*createdBefore) {
		return false
	}

	return true
}

func clusterToJobFlow(c *Cluster) JobFlow {
	creationSeconds := timelineSeconds(c.Status.Timeline, timelineKeyCreation)
	endSeconds := timelineSeconds(c.Status.Timeline, timelineKeyEnd)

	stateChangeMsg := ""
	if m, ok := c.Status.StateChangeReason["Message"]; ok {
		stateChangeMsg, _ = m.(string)
	}

	totalInstances := 0
	masterType := ""
	slaveType := ""

	for _, grp := range c.instanceGroups {
		totalInstances += grp.RunningInstanceCount
		switch grp.InstanceGroupType {
		case "MASTER":
			masterType = grp.InstanceType
		case "CORE", "TASK":
			if slaveType == "" {
				slaveType = grp.InstanceType
			}
		}
	}

	return JobFlow{
		JobFlowID:    c.ID,
		Name:         c.Name,
		ReleaseLabel: c.ReleaseLabel,
		LogURI:       c.LogURI,
		ServiceRole:  c.ServiceRole,
		ExecutionStatusDetail: JobFlowExecutionStatusDetail{
			State:             c.Status.State,
			CreationDateTime:  creationSeconds,
			EndDateTime:       endSeconds,
			StateChangeReason: stateChangeMsg,
		},
		Instances: JobFlowInstancesDetail{
			MasterInstanceType: masterType,
			SlaveInstanceType:  slaveType,
			InstanceCount:      totalInstances,
		},
	}
}
