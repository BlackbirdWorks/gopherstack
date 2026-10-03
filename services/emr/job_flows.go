package emr

import (
	"context"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	jobFlowRetention       = 60 * 24 * time.Hour
	jobFlowRecentCompleted = 14 * 24 * time.Hour
)

// jobFlowState maps a cluster state to its JobFlowExecutionState.
func jobFlowState(clusterState string) string {
	switch clusterState {
	case StateTerminated:
		return "COMPLETED"
	case StateTerminatedWithErrors:
		return "FAILED"
	case "TERMINATING":
		return "SHUTTING_DOWN"
	default:
		return clusterState
	}
}

func jobFlowIsActive(state string) bool {
	switch state {
	case "RUNNING", "WAITING", "SHUTTING_DOWN", "STARTING":
		return true
	default:
		return false
	}
}

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
		noParams := idSet == nil && stateSet == nil && createdAfter == nil && createdBefore == nil
		if !jobFlowMatchesFilter(c, idSet, stateSet, createdAfter, createdBefore) ||
			!jobFlowInDefaultWindow(c, noParams, time.Now()) {
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

	if stateSet != nil && !stateSet[jobFlowState(c.Status.State)] {
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

// jobFlowInDefaultWindow applies the retention in api_op_DescribeJobFlows.go:17-28:
// two months always, and with no parameters only recent completed or active flows.
func jobFlowInDefaultWindow(c *Cluster, noParams bool, now time.Time) bool {
	created := epochSecondsToTime(clusterCreationSecondsFromCluster(c))
	if created.Before(now.Add(-jobFlowRetention)) {
		return false
	}

	if !noParams {
		return true
	}

	state := jobFlowState(c.Status.State)

	completed := state == "COMPLETED" || state == "FAILED"

	return jobFlowIsActive(state) || (completed && !created.Before(now.Add(-jobFlowRecentCompleted)))
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
			State:             jobFlowState(c.Status.State),
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
