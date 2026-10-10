package redshift

import (
	"fmt"
	"time"
)

const (
	minClusterNodeCount = 1
	maxClusterNodeCount = 100
	minMultiNodeCount   = 2
)

const nodeTypeRA3XLPlus = "ra3.xlplus"

var validNodeTypes = map[string]struct{}{ //nolint:gochecknoglobals // lookup table
	"dc1.large": {}, "dc1.8xlarge": {},
	"dc2.large": {}, "dc2.8xlarge": {},
	"ds1.xlarge": {}, "ds1.8xlarge": {},
	"ds2.xlarge": {}, "ds2.8xlarge": {},
	"ra3.large": {}, nodeTypeRA3XLPlus: {}, "ra3.4xlarge": {}, "ra3.16xlarge": {},
}

func validateNodeType(nodeType string) error {
	if _, ok := validNodeTypes[nodeType]; !ok {
		return fmt.Errorf("%w: Invalid node type %q", ErrInvalidParameter, nodeType)
	}

	return nil
}

func validateNodeCount(n int) error {
	if n < minClusterNodeCount || n > maxClusterNodeCount {
		return fmt.Errorf(
			"%w: NumberOfNodes must be between %d and %d",
			ErrInvalidParameter,
			minClusterNodeCount,
			maxClusterNodeCount,
		)
	}

	return nil
}

func validateClusterTypeName(clusterType string) error {
	if clusterType != "" && clusterType != clusterTypeSingleNode && clusterType != clusterTypeMultiNode {
		return fmt.Errorf(
			"%w: ClusterType must be %s or %s",
			ErrInvalidParameter,
			clusterTypeSingleNode,
			clusterTypeMultiNode,
		)
	}

	return nil
}

func checkTopology(clusterType string, nodes int) error {
	switch {
	case clusterType == clusterTypeSingleNode && nodes != 1:
		return fmt.Errorf(
			"%w: Number of nodes for cluster type %s must be 1",
			ErrInvalidParameterCombination,
			clusterType,
		)
	case clusterType == clusterTypeMultiNode && nodes < minMultiNodeCount:
		return fmt.Errorf(
			"%w: Number of nodes for cluster type %s must be greater than or equal to %d",
			ErrInvalidParameterCombination, clusterType, minMultiNodeCount,
		)
	}

	return nil
}

func topologyType(clusterType string, nodes int) string {
	if clusterType != "" {
		return clusterType
	}

	if nodes > 1 {
		return clusterTypeMultiNode
	}

	return clusterTypeSingleNode
}

// resolveClusterTopology applies CreateCluster's ClusterType/NumberOfNodes rules; nodes==0 means unspecified.
func resolveClusterTopology(clusterType string, nodes int) (string, int, error) {
	if err := validateClusterTypeName(clusterType); err != nil {
		return "", 0, err
	}

	if nodes == 0 {
		nodes = minClusterNodeCount
	}

	if err := validateNodeCount(nodes); err != nil {
		return "", 0, err
	}

	if err := checkTopology(clusterType, nodes); err != nil {
		return "", 0, err
	}

	return topologyType(clusterType, nodes), nodes, nil
}

func resolveResizeTopology(cluster *Cluster, clusterType string, nodes int) (string, int, error) {
	if err := validateClusterTypeName(clusterType); err != nil {
		return "", 0, err
	}

	if nodes == 0 {
		nodes = cluster.NumberOfNodes
	}

	if err := validateNodeCount(nodes); err != nil {
		return "", 0, err
	}

	if err := checkTopology(clusterType, nodes); err != nil {
		return "", 0, err
	}

	return topologyType(clusterType, nodes), nodes, nil
}

// beginClusterTransitionLocked parks the cluster in interim for the activation delay, or lands on final at once.
func (b *InMemoryBackend) beginClusterTransitionLocked(cluster *Cluster, interim, final string) Cluster {
	if b.clusterActivationDelay <= 0 {
		cluster.Status = final

		return cloneCluster(cluster)
	}

	cluster.Status = interim
	b.scheduleClusterTransitionLocked(cluster.ClusterIdentifier, &clusterTransition{
		effectiveAt: time.Now().Add(b.clusterActivationDelay),
		status:      final,
	})

	return cloneCluster(cluster)
}
