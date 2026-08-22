package sagemaker

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrClusterNotFound is returned when a cluster does not exist.
	ErrClusterNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrClusterAlreadyExists is returned when a cluster with the same name already exists.
	ErrClusterAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
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
		CreationTime:      time.Now(),
	}
}

// CreateClusterOptions holds the parameters CreateCluster accepts.
type CreateClusterOptions struct {
	VpcConfig            *VpcConfig
	AutoScaling          *ClusterAutoScalingConfig
	Orchestrator         *ClusterOrchestrator
	TieredStorageConfig  *ClusterTieredStorageConfig
	Tags                 map[string]string
	ClusterName          string
	NodeRecovery         string
	ClusterRole          string
	NodeProvisioningMode string
	InstanceGroups       []ClusterInstanceGroup
}

// validateClusterOrchestratorLocked enforces the real CreateClusterInput /
// UpdateClusterInput business rule: "If you specify the Orchestrator field,
// you must provide exactly one orchestrator configuration: either Eks or
// Slurm. Specifying both or providing an empty configuration returns a
// validation error." (api_op_CreateCluster.go:76-78, sagemaker@v1.263.2).
func validateClusterOrchestratorLocked(o *ClusterOrchestrator) error {
	if o == nil {
		return nil
	}

	if (o.Eks != nil) == (o.Slurm != nil) {
		return fmt.Errorf("%w: Orchestrator requires exactly one of Eks or Slurm", ErrValidation)
	}

	return nil
}

// CreateCluster creates a new SageMaker HyperPod cluster and auto-provisions
// InstanceCount running nodes for each requested instance group.
func (b *InMemoryBackend) CreateCluster(
	ctx context.Context,
	opts CreateClusterOptions,
) (*Cluster, error) {
	b.mu.Lock("CreateCluster")
	defer b.mu.Unlock()

	if opts.ClusterName == "" {
		return nil, fmt.Errorf("%w: ClusterName is required", ErrValidation)
	}

	if err := validateClusterOrchestratorLocked(opts.Orchestrator); err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.region)
	store := b.clustersStore(region)

	if _, ok := store.Get(opts.ClusterName); ok {
		return nil, fmt.Errorf("%w: cluster %q already exists", ErrClusterAlreadyExists, opts.ClusterName)
	}

	clusterARN := arn.Build("sagemaker", region, b.accountID, "cluster/"+opts.ClusterName)

	c := &Cluster{
		ClusterName:          opts.ClusterName,
		ClusterArn:           clusterARN,
		ClusterStatus:        clusterStatusInService,
		NodeRecovery:         opts.NodeRecovery,
		ClusterRole:          opts.ClusterRole,
		NodeProvisioningMode: opts.NodeProvisioningMode,
		VpcConfig:            opts.VpcConfig,
		AutoScaling:          opts.AutoScaling,
		Orchestrator:         opts.Orchestrator,
		TieredStorageConfig:  opts.TieredStorageConfig,
		InstanceGroups:       append([]ClusterInstanceGroup(nil), opts.InstanceGroups...),
		Tags:                 mergeTags(nil, opts.Tags),
		CreationTime:         time.Now(),
		Nodes:                make(map[string]*ClusterNode),
	}

	for i, ig := range opts.InstanceGroups {
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
	b.clusterARNIndexStore(region)[clusterARN] = opts.ClusterName

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

// ListClustersParams bundles ListClusters' filter/sort/pagination criteria
// (api_op_ListClusters.go:30-71, sagemaker@v1.263.2). TrainingPlanArn is
// decoded by the handler for wire-shape fidelity but is a disclosed no-op
// here: CreateClusterInput has no TrainingPlanArn field at all (nor does
// Cluster/ClusterInstanceGroupSpecification), so no cluster ever has a
// training-plan association to filter by.
type ListClustersParams struct {
	CreationTimeAfter  *time.Time
	CreationTimeBefore *time.Time
	NameContains       string
	NextToken          string
	SortBy             string
	SortOrder          string
	MaxResults         int32
}

// ListClusters returns clusters matching params, sorted by params.SortBy
// (default CreationTime) / params.SortOrder (default Ascending, per
// api_op_ListClusters.go's documented defaults), capped at params.MaxResults.
func (b *InMemoryBackend) ListClusters(ctx context.Context, params ListClustersParams) ([]*Cluster, string) {
	b.mu.RLock("ListClusters")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	tbl := b.clustersStoreRO(region)

	list := make([]*Cluster, 0, tbl.Len())

	for _, c := range tbl.All() {
		if !matchesClusterListParams(c, params) {
			continue
		}

		list = append(list, cloneCluster(c))
	}

	asc := !strings.EqualFold(params.SortOrder, sortOrderDescending)
	sort.Slice(list, func(i, j int) bool {
		less := clusterSortLess(list[i], list[j], params.SortBy)
		if asc {
			return less
		}

		return !less
	})

	return paginateSlice(list, params.NextToken, params.MaxResults)
}

// matchesClusterListParams reports whether c satisfies every filter in params.
func matchesClusterListParams(c *Cluster, p ListClustersParams) bool {
	if p.NameContains != "" && !strings.Contains(c.ClusterName, p.NameContains) {
		return false
	}

	if p.CreationTimeAfter != nil && !c.CreationTime.After(*p.CreationTimeAfter) {
		return false
	}

	if p.CreationTimeBefore != nil && !c.CreationTime.Before(*p.CreationTimeBefore) {
		return false
	}

	return true
}

// clusterSortLess orders two clusters by sortBy -- one of ClusterSortBy's
// real values (CREATION_TIME/NAME, types/enums.go:2775-2776).
func clusterSortLess(a, b *Cluster, sortBy string) bool {
	switch sortBy {
	case sortByName:
		if a.ClusterName != b.ClusterName {
			return a.ClusterName < b.ClusterName
		}
	default:
		if !a.CreationTime.Equal(b.CreationTime) {
			return a.CreationTime.Before(b.CreationTime)
		}
	}

	return a.ClusterName < b.ClusterName
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

// StartClusterHealthCheck validates that the cluster (by name or ARN) exists
// and that at least one deep-health-check configuration was supplied,
// returning the cluster's ARN. Real AWS asynchronously runs the requested
// deep health checks against the given instance groups/instances; this
// backend does not synthesize per-node pass/fail health-check results (no
// fabricated telemetry a client could mistake for a measured outcome) —
// consistent with this service's existing ClusterNode model, which only
// ever reports a static "Running" NodeStatus rather than invented health
// signals.
func (b *InMemoryBackend) StartClusterHealthCheck(
	ctx context.Context,
	nameOrArn string,
	hasDeepHealthCheckConfigurations bool,
) (string, error) {
	b.mu.Lock("StartClusterHealthCheck")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	c, err := b.resolveClusterLocked(region, nameOrArn)
	if err != nil {
		return "", err
	}

	if !hasDeepHealthCheckConfigurations {
		return "", fmt.Errorf("%w: DeepHealthCheckConfigurations is required", ErrValidation)
	}

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

// UpdateClusterOptions holds the parameters UpdateCluster accepts.
type UpdateClusterOptions struct {
	AutoScaling            *ClusterAutoScalingConfig
	Orchestrator           *ClusterOrchestrator
	TieredStorageConfig    *ClusterTieredStorageConfig
	NameOrArn              string
	NodeRecovery           string
	NodeProvisioningMode   string
	InstanceGroups         []ClusterInstanceGroup
	InstanceGroupsToDelete []string
}

// UpdateCluster updates instance groups (adding, resizing, or removing them)
// and the node-recovery/autoscaling/orchestrator/tiered-storage configuration
// of an existing cluster.
func (b *InMemoryBackend) UpdateCluster(ctx context.Context, opts UpdateClusterOptions) (*Cluster, error) {
	b.mu.Lock("UpdateCluster")
	defer b.mu.Unlock()

	if err := validateClusterOrchestratorLocked(opts.Orchestrator); err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.region)

	c, err := b.resolveClusterLocked(region, opts.NameOrArn)
	if err != nil {
		return nil, err
	}

	if opts.NodeRecovery != "" {
		c.NodeRecovery = opts.NodeRecovery
	}

	if opts.NodeProvisioningMode != "" {
		c.NodeProvisioningMode = opts.NodeProvisioningMode
	}

	if opts.AutoScaling != nil {
		c.AutoScaling = opts.AutoScaling
	}

	if opts.Orchestrator != nil {
		c.Orchestrator = opts.Orchestrator
	}

	if opts.TieredStorageConfig != nil {
		c.TieredStorageConfig = opts.TieredStorageConfig
	}

	if len(opts.InstanceGroupsToDelete) > 0 {
		toDelete := make(map[string]bool, len(opts.InstanceGroupsToDelete))
		for _, n := range opts.InstanceGroupsToDelete {
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

	for _, ig := range opts.InstanceGroups {
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

// ListClusterNodesParams bundles ListClusterNodes' filter/sort/pagination
// criteria (api_op_ListClusterNodes.go:30-70). IncludeNodeLogicalIds is
// decoded by the handler for wire-shape fidelity but is a disclosed no-op
// here: every node in this backend gets its NodeId assigned synchronously at
// creation (see newClusterNode) -- there is no still-provisioning,
// logical-id-only node state for the flag to include or exclude.
type ListClusterNodesParams struct {
	CreationTimeAfter         *time.Time
	CreationTimeBefore        *time.Time
	InstanceGroupNameContains string
	NextToken                 string
	SortBy                    string
	SortOrder                 string
	MaxResults                int32
}

// ListClusterNodes returns nodes belonging to a cluster matching params,
// sorted by params.SortBy (default CreationTime) / params.SortOrder (default
// Ascending, per api_op_ListClusterNodes.go's documented defaults), capped
// at params.MaxResults.
func (b *InMemoryBackend) ListClusterNodes(
	ctx context.Context,
	clusterNameOrArn string,
	params ListClusterNodesParams,
) ([]*ClusterNode, string, error) {
	b.mu.RLock("ListClusterNodes")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	c, err := b.resolveClusterLocked(region, clusterNameOrArn)
	if err != nil {
		return nil, "", err
	}

	list := make([]*ClusterNode, 0, len(c.Nodes))

	for _, n := range c.Nodes {
		if !matchesClusterNodeListParams(n, params) {
			continue
		}

		cp := *n
		list = append(list, &cp)
	}

	asc := !strings.EqualFold(params.SortOrder, sortOrderDescending)
	sort.Slice(list, func(i, j int) bool {
		less := clusterNodeSortLess(list[i], list[j], params.SortBy)
		if asc {
			return less
		}

		return !less
	})

	nodes, next := paginateSlice(list, params.NextToken, params.MaxResults)

	return nodes, next, nil
}

// matchesClusterNodeListParams reports whether n satisfies every filter in params.
func matchesClusterNodeListParams(n *ClusterNode, p ListClusterNodesParams) bool {
	if p.InstanceGroupNameContains != "" && !strings.Contains(n.InstanceGroupName, p.InstanceGroupNameContains) {
		return false
	}

	if p.CreationTimeAfter != nil && !n.CreationTime.After(*p.CreationTimeAfter) {
		return false
	}

	if p.CreationTimeBefore != nil && !n.CreationTime.Before(*p.CreationTimeBefore) {
		return false
	}

	return true
}

// clusterNodeSortLess orders two nodes by sortBy -- one of ClusterSortBy's
// real values (CREATION_TIME/NAME, types/enums.go:2775-2776), reused
// unchanged from ListClusters even though a node has no Name of its own; NAME
// is interpreted as its InstanceGroupName, the closest analogous field a node
// carries.
func clusterNodeSortLess(a, b *ClusterNode, sortBy string) bool {
	switch sortBy {
	case sortByName:
		if a.InstanceGroupName != b.InstanceGroupName {
			return a.InstanceGroupName < b.InstanceGroupName
		}
	default:
		if !a.CreationTime.Equal(b.CreationTime) {
			return a.CreationTime.Before(b.CreationTime)
		}
	}

	return a.NodeID < b.NodeID
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

// ensureClusterLocked looks up a cluster by name (must be called with lock held).
func (b *InMemoryBackend) ensureClusterLocked(ctx context.Context, clusterName string) (*Cluster, error) {
	region := getRegion(ctx, b.region)

	c, ok := b.clustersStore(region).Get(clusterName)
	if !ok {
		return nil, fmt.Errorf("%w: cluster %q not found", ErrClusterNotFound, clusterName)
	}

	return c, nil
}

// AddClusterInternal adds a cluster directly for seeding tests.
func (b *InMemoryBackend) AddClusterInternal(ctx context.Context, clusterName string) *Cluster {
	b.mu.Lock("AddClusterInternal")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	clusterARN := arn.Build("sagemaker", region, b.accountID, "cluster/"+clusterName)
	c := &Cluster{
		ClusterName:   clusterName,
		ClusterArn:    clusterARN,
		ClusterStatus: clusterStatusInService,
		Nodes:         make(map[string]*ClusterNode),
		CreationTime:  time.Now(),
	}
	b.clustersStore(region).Put(c)
	b.clusterARNIndexStore(region)[clusterARN] = clusterName

	return cloneCluster(c)
}

// AttachClusterNodeVolume attaches a volume to a cluster node.
func (b *InMemoryBackend) AttachClusterNodeVolume(
	ctx context.Context,
	clusterName, nodeID string,
	volume ClusterNodeVolume,
) (string, string, error) {
	b.mu.Lock("AttachClusterNodeVolume")
	defer b.mu.Unlock()

	c, err := b.ensureClusterLocked(ctx, clusterName)
	if err != nil {
		return "", "", err
	}

	if nodeID == "" {
		return "", "", fmt.Errorf("%w: NodeId is required", ErrValidation)
	}

	node, ok := c.Nodes[nodeID]
	if !ok {
		node = &ClusterNode{
			NodeID:     nodeID,
			NodeStatus: statusRunning,
		}
		c.Nodes[nodeID] = node
	}

	node.Volumes = append(node.Volumes, volume)

	return c.ClusterArn, nodeID, nil
}

// BatchAddClusterNodes adds multiple nodes to a cluster.
// Returns clusterArn, the nodes that were added, and the node configs that
// failed (duplicate node ID).
func (b *InMemoryBackend) BatchAddClusterNodes(
	ctx context.Context,
	clusterName string,
	nodeConfigs []ClusterNode,
) (string, []ClusterNode, []ClusterNode, error) {
	b.mu.Lock("BatchAddClusterNodes")
	defer b.mu.Unlock()

	c, err := b.ensureClusterLocked(ctx, clusterName)
	if err != nil {
		return "", nil, nil, err
	}

	var successful, failed []ClusterNode

	for i := range nodeConfigs {
		node := &nodeConfigs[i]
		if node.NodeID == "" {
			node.NodeID = fmt.Sprintf("node-%d", len(c.Nodes)+1)
		}

		if node.NodeStatus == "" {
			node.NodeStatus = statusRunning
		}

		if _, exists := c.Nodes[node.NodeID]; exists {
			failed = append(failed, *node)

			continue
		}

		nodeCopy := *node
		c.Nodes[node.NodeID] = &nodeCopy
		successful = append(successful, nodeCopy)
	}

	return c.ClusterArn, successful, failed, nil
}

// BatchDeleteClusterNodes removes multiple nodes from a cluster.
// Returns clusterArn, a slice of nodeIDs with errors, and a slice of successfully deleted nodeIDs.
func (b *InMemoryBackend) BatchDeleteClusterNodes(
	ctx context.Context,
	clusterName string,
	nodeIDs []string,
) (string, []string, []string, error) {
	b.mu.Lock("BatchDeleteClusterNodes")
	defer b.mu.Unlock()

	c, err := b.ensureClusterLocked(ctx, clusterName)
	if err != nil {
		return "", nil, nil, err
	}

	var errored, successful []string

	for _, nodeID := range nodeIDs {
		if _, ok := c.Nodes[nodeID]; !ok {
			errored = append(errored, nodeID)

			continue
		}

		delete(c.Nodes, nodeID)
		successful = append(successful, nodeID)
	}

	return c.ClusterArn, errored, successful, nil
}

// BatchRebootClusterNodes reboots multiple nodes in a cluster.
// Returns clusterArn, a slice of failed nodeIDs, and successful nodeIDs.
func (b *InMemoryBackend) BatchRebootClusterNodes(
	ctx context.Context,
	clusterName string,
	nodeIDs []string,
) (string, []string, []string, error) {
	b.mu.Lock("BatchRebootClusterNodes")
	defer b.mu.Unlock()

	c, err := b.ensureClusterLocked(ctx, clusterName)
	if err != nil {
		return "", nil, nil, err
	}

	var failures, successful []string

	for _, nodeID := range nodeIDs {
		if _, ok := c.Nodes[nodeID]; !ok {
			failures = append(failures, nodeID)

			continue
		}

		successful = append(successful, nodeID)
	}

	return c.ClusterArn, failures, successful, nil
}

// BatchReplaceClusterNodes replaces multiple nodes in a cluster.
// Returns clusterArn, the nodeIDs that failed to replace, and the nodeIDs
// that were replaced successfully.
func (b *InMemoryBackend) BatchReplaceClusterNodes(
	ctx context.Context,
	clusterName string,
	nodes []ClusterNode,
) (string, []string, []string, error) {
	b.mu.Lock("BatchReplaceClusterNodes")
	defer b.mu.Unlock()

	c, err := b.ensureClusterLocked(ctx, clusterName)
	if err != nil {
		return "", nil, nil, err
	}

	var failed, successful []string

	for i := range nodes {
		node := &nodes[i]
		if node.NodeID == "" {
			failed = append(failed, "")

			continue
		}

		if _, ok := c.Nodes[node.NodeID]; !ok {
			failed = append(failed, node.NodeID)

			continue
		}

		nodeCopy := *node
		nodeCopy.NodeStatus = statusRunning
		c.Nodes[node.NodeID] = &nodeCopy
		successful = append(successful, node.NodeID)
	}

	return c.ClusterArn, failed, successful, nil
}
