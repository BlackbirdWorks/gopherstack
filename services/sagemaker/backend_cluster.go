package sagemaker

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// ---------------------------------------------------------------------------
// SageMaker HyperPod Cluster CRUD
// ---------------------------------------------------------------------------

// resolveClusterLocked looks up a cluster by name or ARN (must be called with
// b.mu held, read or write).
func (b *InMemoryBackend) resolveClusterLocked(region, nameOrArn string) (*Cluster, error) {
	store := b.clustersStoreRO(region)

	if c, ok := store.Get(nameOrArn); ok {
		return c, nil
	}

	if name, ok := b.clusterARNIndexStoreRO(region)[nameOrArn]; ok {
		if c, found := store.Get(name); found {
			return c, nil
		}
	}

	return nil, fmt.Errorf("%w: cluster %q not found", ErrClusterNotFound, nameOrArn)
}

// newClusterNode builds a running node for the given instance group, assigning
// it the next sequential node ID within the cluster.
func newClusterNode(c *Cluster, ig ClusterInstanceGroup) *ClusterNode {
	nodeID := fmt.Sprintf("node-%d", len(c.Nodes)+1)
	for c.Nodes[nodeID] != nil {
		nodeID = fmt.Sprintf("node-%d", len(c.Nodes)+2) //nolint:mnd // simple collision bump
	}

	return &ClusterNode{
		NodeID:            nodeID,
		InstanceType:      ig.InstanceType,
		InstanceGroupName: ig.InstanceGroupName,
		NodeStatus:        statusRunning,
	}
}

// CreateCluster creates a new SageMaker HyperPod cluster and auto-provisions
// InstanceCount running nodes for each requested instance group.
func (b *InMemoryBackend) CreateCluster(
	ctx context.Context,
	name string,
	instanceGroups []ClusterInstanceGroup,
	nodeRecovery string,
	tags map[string]string,
) (*Cluster, error) {
	b.mu.Lock("CreateCluster")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: ClusterName is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)
	store := b.clustersStore(region)

	if _, ok := store.Get(name); ok {
		return nil, fmt.Errorf("%w: cluster %q already exists", ErrClusterAlreadyExists, name)
	}

	clusterARN := arn.Build("sagemaker", region, b.accountID, "cluster/"+name)

	c := &Cluster{
		ClusterName:    name,
		ClusterArn:     clusterARN,
		ClusterStatus:  clusterStatusInService,
		NodeRecovery:   nodeRecovery,
		InstanceGroups: append([]ClusterInstanceGroup(nil), instanceGroups...),
		Tags:           mergeTags(nil, tags),
		CreationTime:   time.Now(),
		Nodes:          make(map[string]*ClusterNode),
	}

	for i, ig := range instanceGroups {
		count := ig.InstanceCount
		if count <= 0 {
			count = 1
		}

		c.InstanceGroups[i].InstanceCount = count

		for range count {
			node := newClusterNode(c, ig)
			c.Nodes[node.NodeID] = node
		}
	}

	store.Put(c)
	b.clusterARNIndexStore(region)[clusterARN] = name

	return cloneCluster(c), nil
}

// DescribeCluster returns a cluster by name or ARN.
func (b *InMemoryBackend) DescribeCluster(ctx context.Context, nameOrArn string) (*Cluster, error) {
	b.mu.RLock("DescribeCluster")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	c, err := b.resolveClusterLocked(region, nameOrArn)
	if err != nil {
		return nil, err
	}

	return cloneCluster(c), nil
}

// ListClusters returns all clusters, optionally filtered by a NameContains substring.
func (b *InMemoryBackend) ListClusters(
	ctx context.Context,
	nextToken, nameContains string,
) ([]*Cluster, string) {
	b.mu.RLock("ListClusters")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	all := b.clustersStoreRO(region)

	filtered := make(map[string]*Cluster, all.Len())

	for _, c := range all.All() {
		if nameContains == "" || strings.Contains(c.ClusterName, nameContains) {
			filtered[c.ClusterName] = c
		}
	}

	return sagemakerListPagedMap(filtered, nextToken, cloneCluster,
		func(a, b *Cluster) bool { return a.ClusterName < b.ClusterName })
}

// DeleteCluster removes a cluster by name or ARN, returning its ARN.
func (b *InMemoryBackend) DeleteCluster(ctx context.Context, nameOrArn string) (string, error) {
	b.mu.Lock("DeleteCluster")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	c, err := b.resolveClusterLocked(region, nameOrArn)
	if err != nil {
		return "", err
	}

	b.clustersStore(region).Delete(c.ClusterName)
	delete(b.clusterARNIndexStore(region), c.ClusterArn)

	return c.ClusterArn, nil
}

// countNodeIDsInGroupLocked returns the sorted node IDs currently assigned to
// the given instance group.
func countNodeIDsInGroupLocked(c *Cluster, groupName string) []string {
	ids := make([]string, 0, len(c.Nodes))

	for id, n := range c.Nodes {
		if n.InstanceGroupName == groupName {
			ids = append(ids, id)
		}
	}

	sort.Strings(ids)

	return ids
}

// resizeInstanceGroupNodesLocked adds or removes nodes so the instance group
// has exactly ig.InstanceCount nodes.
func resizeInstanceGroupNodesLocked(c *Cluster, ig ClusterInstanceGroup) {
	ids := countNodeIDsInGroupLocked(c, ig.InstanceGroupName)
	current := int32(len(ids)) //nolint:gosec // bounded by cluster node counts, never near MaxInt32

	switch {
	case current < ig.InstanceCount:
		for range ig.InstanceCount - current {
			node := newClusterNode(c, ig)
			c.Nodes[node.NodeID] = node
		}
	case current > ig.InstanceCount:
		for _, id := range ids[ig.InstanceCount:] {
			delete(c.Nodes, id)
		}
	}
}

// removeInstanceGroupNodesLocked deletes every node belonging to groupName.
func removeInstanceGroupNodesLocked(c *Cluster, groupName string) {
	for id, n := range c.Nodes {
		if n.InstanceGroupName == groupName {
			delete(c.Nodes, id)
		}
	}
}

// upsertInstanceGroupLocked creates or updates an instance group on c,
// resizing its node pool to match the requested InstanceCount.
func upsertInstanceGroupLocked(c *Cluster, ig ClusterInstanceGroup) {
	for i := range c.InstanceGroups {
		if c.InstanceGroups[i].InstanceGroupName != ig.InstanceGroupName {
			continue
		}

		if ig.InstanceType != "" {
			c.InstanceGroups[i].InstanceType = ig.InstanceType
		}

		if ig.ExecutionRole != "" {
			c.InstanceGroups[i].ExecutionRole = ig.ExecutionRole
		}

		if ig.InstanceCount > 0 {
			c.InstanceGroups[i].InstanceCount = ig.InstanceCount
			resizeInstanceGroupNodesLocked(c, c.InstanceGroups[i])
		}

		return
	}

	if ig.InstanceCount <= 0 {
		ig.InstanceCount = 1
	}

	c.InstanceGroups = append(c.InstanceGroups, ig)
	resizeInstanceGroupNodesLocked(c, ig)
}

// UpdateCluster updates instance groups (adding, resizing, or removing them)
// and the node-recovery mode of an existing cluster.
func (b *InMemoryBackend) UpdateCluster(
	ctx context.Context,
	nameOrArn string,
	instanceGroups []ClusterInstanceGroup,
	instanceGroupsToDelete []string,
	nodeRecovery string,
) (*Cluster, error) {
	b.mu.Lock("UpdateCluster")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	c, err := b.resolveClusterLocked(region, nameOrArn)
	if err != nil {
		return nil, err
	}

	if nodeRecovery != "" {
		c.NodeRecovery = nodeRecovery
	}

	if len(instanceGroupsToDelete) > 0 {
		toDelete := make(map[string]bool, len(instanceGroupsToDelete))
		for _, n := range instanceGroupsToDelete {
			toDelete[n] = true
		}

		kept := make([]ClusterInstanceGroup, 0, len(c.InstanceGroups))

		for _, ig := range c.InstanceGroups {
			if toDelete[ig.InstanceGroupName] {
				removeInstanceGroupNodesLocked(c, ig.InstanceGroupName)

				continue
			}

			kept = append(kept, ig)
		}

		c.InstanceGroups = kept
	}

	for _, ig := range instanceGroups {
		upsertInstanceGroupLocked(c, ig)
	}

	return cloneCluster(c), nil
}

// UpdateClusterSoftware validates the cluster exists and returns its ARN.
// Real AWS asynchronously patches node AMIs; this emulator applies the
// request immediately with no observable software-version state to update.
func (b *InMemoryBackend) UpdateClusterSoftware(ctx context.Context, nameOrArn string) (string, error) {
	b.mu.Lock("UpdateClusterSoftware")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	c, err := b.resolveClusterLocked(region, nameOrArn)
	if err != nil {
		return "", err
	}

	return c.ClusterArn, nil
}

// DescribeClusterNode returns a single node of a cluster by NodeId.
func (b *InMemoryBackend) DescribeClusterNode(
	ctx context.Context,
	clusterNameOrArn, nodeID string,
) (*ClusterNode, error) {
	b.mu.RLock("DescribeClusterNode")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	c, err := b.resolveClusterLocked(region, clusterNameOrArn)
	if err != nil {
		return nil, err
	}

	n, ok := c.Nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: node %q not found in cluster %q", ErrClusterNotFound, nodeID, clusterNameOrArn,
		)
	}

	cp := *n

	return &cp, nil
}

// ListClusterNodes returns a paginated list of nodes belonging to a cluster.
func (b *InMemoryBackend) ListClusterNodes(
	ctx context.Context,
	clusterNameOrArn, nextToken string,
) ([]*ClusterNode, string, error) {
	b.mu.RLock("ListClusterNodes")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	c, err := b.resolveClusterLocked(region, clusterNameOrArn)
	if err != nil {
		return nil, "", err
	}

	nodes, next := sagemakerListKeyPagedMap(c.Nodes, nextToken, func(n *ClusterNode) *ClusterNode {
		cp := *n

		return &cp
	})

	return nodes, next, nil
}

// DescribeClusterEvent looks up a single cluster event by ID. This emulator
// never generates cluster events (they require the Continuous provisioning
// mode with an EKS/Slurm orchestrator), so any lookup on an existing cluster
// correctly reports that the event does not exist.
func (b *InMemoryBackend) DescribeClusterEvent(ctx context.Context, clusterNameOrArn, eventID string) error {
	b.mu.RLock("DescribeClusterEvent")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	if _, err := b.resolveClusterLocked(region, clusterNameOrArn); err != nil {
		return err
	}

	return fmt.Errorf("%w: event %q not found", ErrClusterNotFound, eventID)
}

// ListClusterEvents validates the cluster exists and returns an empty event
// list (see DescribeClusterEvent for why events are never populated).
func (b *InMemoryBackend) ListClusterEvents(ctx context.Context, clusterNameOrArn string) error {
	b.mu.RLock("ListClusterEvents")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	_, err := b.resolveClusterLocked(region, clusterNameOrArn)

	return err
}

// DetachedVolume describes the result of detaching an EBS volume from a
// cluster node.
type DetachedVolume struct {
	AttachTime time.Time
	ClusterArn string
	NodeID     string
	VolumeID   string
	DeviceName string
	Status     string
}

// DetachClusterNodeVolume detaches an EBS volume from a cluster node. Volumes
// are matched by the VolumeName supplied to AttachClusterNodeVolume, since
// this emulator does not mint a separate immutable VolumeId at attach time.
func (b *InMemoryBackend) DetachClusterNodeVolume(
	ctx context.Context,
	clusterArn, nodeID, volumeID string,
) (*DetachedVolume, error) {
	b.mu.Lock("DetachClusterNodeVolume")
	defer b.mu.Unlock()

	if clusterArn == "" {
		return nil, fmt.Errorf("%w: ClusterArn is required", ErrValidation)
	}

	if nodeID == "" {
		return nil, fmt.Errorf("%w: NodeId is required", ErrValidation)
	}

	if volumeID == "" {
		return nil, fmt.Errorf("%w: VolumeId is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)

	c, err := b.resolveClusterLocked(region, clusterArn)
	if err != nil {
		return nil, err
	}

	node, ok := c.Nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: node %q not found in cluster %q", ErrClusterNotFound, nodeID, clusterArn,
		)
	}

	idx := -1

	for i, v := range node.Volumes {
		if v.VolumeName == volumeID {
			idx = i

			break
		}
	}

	if idx == -1 {
		return nil, fmt.Errorf(
			"%w: volume %q not found on node %q", ErrClusterNotFound, volumeID, nodeID,
		)
	}

	node.Volumes = append(node.Volumes[:idx], node.Volumes[idx+1:]...)

	return &DetachedVolume{
		ClusterArn: c.ClusterArn,
		NodeID:     nodeID,
		VolumeID:   volumeID,
		DeviceName: "/dev/xvdf",
		Status:     "detached",
		AttachTime: time.Now(),
	}, nil
}
