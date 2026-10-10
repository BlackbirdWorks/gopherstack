package elasticache

import (
	"context"
	"fmt"
	"slices"
	"strconv"
)

// ReplicaChange is one ConfigureShard entry of Increase/DecreaseReplicaCount.
type ReplicaChange struct {
	NodeGroupID     string
	AZs             []string
	OutpostArns     []string
	NewReplicaCount int32
}

// ReplicaCountRequest carries Increase/DecreaseReplicaCount's members.
type ReplicaCountRequest struct {
	Config             []ReplicaChange
	ReplicasToRemove   []string
	NewReplicaCount    int32
	HasNewReplicaCount bool
	ApplyImmediately   bool
}

func findNodeGroup(rg *ReplicationGroup, id string) int {
	return slices.IndexFunc(rg.NodeGroups, func(ng NodeGroup) bool { return ng.NodeGroupID == id })
}

// promoteReplica makes ng.Replicas[idx] the primary and demotes the old primary into its slot.
func promoteReplica(ng *NodeGroup, idx int) {
	next := ng.Replicas[idx]
	next.CurrentRole = rolePrimary

	if ng.PrimaryNode != nil {
		old := *ng.PrimaryNode
		old.CurrentRole = roleReplica
		ng.Replicas[idx] = old
	} else {
		ng.Replicas = slices.Delete(ng.Replicas, idx, idx+1)
	}

	ng.PrimaryNode = &next
}

// groupEngineLocked returns a member cluster of ng that serves its data plane.
func (b *InMemoryBackend) groupPrimaryClusterLocked(region string, ng NodeGroup) *Cluster {
	if ng.PrimaryNode == nil {
		return nil
	}

	c, _ := b.clustersStore(region).Get(ng.PrimaryNode.CacheClusterID)

	return c
}

// nextMemberNumber is one past the highest node number used in ng.
func nextMemberNumber(ng NodeGroup) int {
	highest := 0
	if ng.PrimaryNode != nil {
		highest = nodeNumber(ng.PrimaryNode.CacheClusterID)
	}

	for _, r := range ng.Replicas {
		highest = max(highest, nodeNumber(r.CacheClusterID))
	}

	return highest + 1
}

// addReplicaLocked adds one replica cluster to ng sharing the primary's data plane.
func (b *InMemoryBackend) addReplicaLocked(
	region string, rg *ReplicationGroup, ng *NodeGroup, np nodePlan,
) error {
	primary := b.groupPrimaryClusterLocked(region, *ng)
	if primary == nil {
		return fmt.Errorf("%w: node group %s has no primary", ErrReplicationGroupNotAvailable, ng.NodeGroupID)
	}

	id := memberClusterID(rg.ReplicationGroupID, ng.NodeGroupID, nextMemberNumber(*ng), rg.ClusterModeEnabled)
	if _, taken := b.clustersStore(region).Get(id); taken {
		return fmt.Errorf("%w: member cluster %s", ErrClusterAlreadyExists, id)
	}

	replica := b.insertClusterLocked(region, id, primary.Engine, primary.NodeType, primary.CacheParameterGroupName,
		primary.PreferredMaintenanceWindow, primary.SnapshotWindow, 1,
		&clusterEngine{port: primary.Port, connectAddr: primary.ConnectAddress})
	copyMemberSettings(replica, primary)
	replica.PreferredAvailabilityZone = np.az
	replica.PreferredOutpostArn = np.outpost
	initClusterNodes(replica, region)

	ng.Replicas = append(ng.Replicas, nodeFor(replica, roleReplica, np.az))

	return nil
}

// copyMemberSettings copies the group-wide settings of src onto dst.
func copyMemberSettings(dst, src *Cluster) {
	dst.EngineVersion = src.EngineVersion
	dst.ReplicationGroupID = src.ReplicationGroupID
	dst.SubnetGroupName = src.SubnetGroupName
	dst.SecurityGroupIDs = slices.Clone(src.SecurityGroupIDs)
	dst.CacheSecurityGroupNames = slices.Clone(src.CacheSecurityGroupNames)
	dst.SnapshotRetentionLimit = src.SnapshotRetentionLimit
	dst.TransitEncryptionEnabled = src.TransitEncryptionEnabled
	dst.TransitEncryptionMode = src.TransitEncryptionMode
	dst.AtRestEncryptionEnabled = src.AtRestEncryptionEnabled
	dst.AuthTokenEnabled = src.AuthTokenEnabled
	dst.KmsKeyID = src.KmsKeyID
	dst.NetworkType = src.NetworkType
	dst.IPDiscovery = src.IPDiscovery
	dst.AutoMinorVersionUpgrade = src.AutoMinorVersionUpgrade
	dst.LogDeliveryConfigurations = slices.Clone(src.LogDeliveryConfigurations)
	dst.NotificationTopicArn = src.NotificationTopicArn
	dst.NotificationTopicStatus = src.NotificationTopicStatus
}

// defaultReplicaAZ picks a zone for the n-th (0-based) replica of ng.
func defaultReplicaAZ(region string, rg *ReplicationGroup, ng NodeGroup, n int) string {
	if rg.MultiAZEnabled {
		return zoneFor(region, n+1)
	}

	if ng.PrimaryNode != nil {
		return ng.PrimaryNode.PreferredAvailabilityZone
	}

	return zoneFor(region, 0)
}

// FailoverReplicationGroup promotes a replica of the node group to primary.
func (b *InMemoryBackend) FailoverReplicationGroup(
	ctx context.Context,
	id, nodeGroupID string,
) (*ReplicationGroup, error) {
	b.mu.Lock("FailoverReplicationGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	rg, exists := b.replicationGroupsStore(region).Get(id)
	if !exists {
		return nil, ErrReplicationGroupNotFound
	}

	if err := b.requireAvailableLocked(
		rg.Status, rg.PendingStatus, rg.AvailableAt, ErrReplicationGroupNotAvailable,
	); err != nil {
		return nil, err
	}

	if len(rg.NodeGroups) > 0 {
		if err := failoverNodeGroup(rg, nodeGroupID); err != nil {
			return nil, err
		}
	}

	rg.Status = statusAvailable
	b.markTransitionLocked(&rg.PendingStatus, &rg.AvailableAt, statusFailingOver)
	b.appendEventLocked(id, "replication-group", "failover completed")

	return b.replicationGroupView(rg), nil
}

func failoverNodeGroup(rg *ReplicationGroup, nodeGroupID string) error {
	idx := 0

	if nodeGroupID != "" {
		if idx = findNodeGroup(rg, nodeGroupID); idx < 0 {
			return fmt.Errorf("%w: node group %s", ErrNodeGroupNotFound, nodeGroupID)
		}
	} else if len(rg.NodeGroups) > 1 {
		return fmt.Errorf("%w: NodeGroupId is required for a replication group with several node groups",
			ErrInvalidParameterValue)
	}

	groups := cloneNodeGroups(rg.NodeGroups)

	if len(groups[idx].Replicas) == 0 {
		return fmt.Errorf("%w: node group %s has no replica to promote", ErrReplicationGroupNotAvailable,
			groups[idx].NodeGroupID)
	}

	promoteReplica(&groups[idx], 0)
	rg.NodeGroups = groups

	return nil
}

// promoteClusterLocked makes clusterID the primary of its node group.
func promoteClusterLocked(rg *ReplicationGroup, clusterID string) error {
	if rg.ClusterModeEnabled {
		return fmt.Errorf("%w: PrimaryClusterId applies to cluster mode disabled groups only",
			ErrInvalidParameterCombination)
	}

	groups := cloneNodeGroups(rg.NodeGroups)

	for gi := range groups {
		if groups[gi].PrimaryNode != nil && groups[gi].PrimaryNode.CacheClusterID == clusterID {
			return nil
		}

		for ri, r := range groups[gi].Replicas {
			if r.CacheClusterID == clusterID {
				promoteReplica(&groups[gi], ri)
				rg.NodeGroups = groups

				return nil
			}
		}
	}

	return fmt.Errorf("%w: cluster %s is not a member of replication group %s", ErrInvalidParameterValue, clusterID,
		rg.ReplicationGroupID)
}

// IncreaseReplicaCount adds replicas so every node group holds newReplicaCount.
func (b *InMemoryBackend) IncreaseReplicaCount(
	ctx context.Context, id string, newReplicaCount int32, applyImmediately bool,
) (*ReplicationGroup, error) {
	return b.IncreaseReplicaCountFull(ctx, id, ReplicaCountRequest{
		NewReplicaCount: newReplicaCount, HasNewReplicaCount: newReplicaCount > 0, ApplyImmediately: applyImmediately,
	})
}

// DecreaseReplicaCount removes replicas so every node group holds newReplicaCount.
func (b *InMemoryBackend) DecreaseReplicaCount(
	ctx context.Context, id string, newReplicaCount int32, applyImmediately bool,
) (*ReplicationGroup, error) {
	return b.DecreaseReplicaCountFull(ctx, id, ReplicaCountRequest{
		NewReplicaCount: newReplicaCount, HasNewReplicaCount: newReplicaCount >= 0, ApplyImmediately: applyImmediately,
	})
}

// replicaTargets resolves a request to node-group-index -> target replica count.
func replicaTargets(rg *ReplicationGroup, req ReplicaCountRequest) (map[int]ReplicaChange, error) {
	if req.HasNewReplicaCount && len(req.Config) > 0 {
		return nil, fmt.Errorf("%w: NewReplicaCount and ReplicaConfiguration are mutually exclusive",
			ErrInvalidParameterCombination)
	}

	out := make(map[int]ReplicaChange)

	if req.HasNewReplicaCount {
		for i, ng := range rg.NodeGroups {
			out[i] = ReplicaChange{NodeGroupID: ng.NodeGroupID, NewReplicaCount: req.NewReplicaCount}
		}

		return out, nil
	}

	for _, cfg := range req.Config {
		id := cfg.NodeGroupID
		if id == "" && len(rg.NodeGroups) == 1 {
			id = rg.NodeGroups[0].NodeGroupID
		}

		idx := findNodeGroup(rg, id)
		if idx < 0 {
			return nil, fmt.Errorf("%w: node group %q", ErrNodeGroupNotFound, id)
		}

		cfg.NodeGroupID = id
		out[idx] = cfg
	}

	return out, nil
}

// IncreaseReplicaCountFull is IncreaseReplicaCount with ReplicaConfiguration support.
func (b *InMemoryBackend) IncreaseReplicaCountFull(
	ctx context.Context, id string, req ReplicaCountRequest,
) (*ReplicationGroup, error) {
	if !req.ApplyImmediately {
		return nil, ErrApplyImmediatelyRequired
	}

	b.mu.Lock("IncreaseReplicaCount")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	rg, ok := b.replicationGroupsStore(region).Get(id)
	if !ok {
		return nil, ErrReplicationGroupNotFound
	}

	if err := b.requireAvailableLocked(
		rg.Status, rg.PendingStatus, rg.AvailableAt, ErrReplicationGroupNotAvailable,
	); err != nil {
		return nil, err
	}

	if len(rg.NodeGroups) == 0 {
		if req.NewReplicaCount > 0 {
			rg.ReplicaCount = req.NewReplicaCount
		}

		b.appendEventLocked(id, "replication-group", "replica count increased")

		return b.replicationGroupView(rg), nil
	}

	targets, err := replicaTargets(rg, req)
	if err != nil {
		return nil, err
	}

	if len(targets) == 0 {
		return nil, fmt.Errorf(
			"%w: NewReplicaCount or ReplicaConfiguration is required",
			ErrInvalidParameterCombination,
		)
	}

	groups := cloneNodeGroups(rg.NodeGroups)

	if err = b.growGroupsLocked(region, rg, groups, targets); err != nil {
		return nil, err
	}

	rg.NodeGroups = groups
	rg.ReplicaCount = count32(len(groups[0].Replicas))
	b.markTransitionLocked(&rg.PendingStatus, &rg.AvailableAt, statusModifying)
	b.appendEventLocked(id, "replication-group", "replica count increased")

	return b.replicationGroupView(rg), nil
}

// growGroupsLocked validates every target and adds the replicas it asks for. The
// clusters it creates are removed again if a later node group fails.
func (b *InMemoryBackend) growGroupsLocked(
	region string, rg *ReplicationGroup, groups []NodeGroup, targets map[int]ReplicaChange,
) error {
	changed := false

	for idx, tgt := range targets {
		current := len(groups[idx].Replicas)

		switch {
		case tgt.NewReplicaCount < 0 || tgt.NewReplicaCount > maxReplicasPerNodeGroup:
			return fmt.Errorf("%w: NewReplicaCount must be between 0 and %d", ErrInvalidParameterValue,
				maxReplicasPerNodeGroup)
		case int(tgt.NewReplicaCount) < current:
			return fmt.Errorf("%w: node group %s already has %d replicas", ErrInvalidParameterValue,
				groups[idx].NodeGroupID, current)
		case int(tgt.NewReplicaCount) > current:
			changed = true
		}

		added := int(tgt.NewReplicaCount) - current
		if len(tgt.AZs) > 0 && len(tgt.AZs) != added {
			return fmt.Errorf("%w: PreferredAvailabilityZones must list exactly %d zones",
				ErrInvalidParameterCombination, added)
		}
	}

	if !changed {
		return fmt.Errorf("%w: replica count is already at the requested value", ErrNoOperation)
	}

	for idx, tgt := range targets {
		added := int(tgt.NewReplicaCount) - len(groups[idx].Replicas)

		for n := range added {
			np := nodePlan{az: defaultReplicaAZ(region, rg, groups[idx], len(groups[idx].Replicas))}
			if n < len(tgt.AZs) {
				np.az = tgt.AZs[n]
			}

			if n < len(tgt.OutpostArns) {
				np.outpost = tgt.OutpostArns[n]
			}

			if err := b.addReplicaLocked(region, rg, &groups[idx], np); err != nil {
				return err
			}
		}
	}

	return nil
}

// DecreaseReplicaCountFull is DecreaseReplicaCount with ReplicaConfiguration and ReplicasToRemove support.
func (b *InMemoryBackend) DecreaseReplicaCountFull(
	ctx context.Context, id string, req ReplicaCountRequest,
) (*ReplicationGroup, error) {
	if !req.ApplyImmediately {
		return nil, ErrApplyImmediatelyRequired
	}

	b.mu.Lock("DecreaseReplicaCount")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	rg, ok := b.replicationGroupsStore(region).Get(id)
	if !ok {
		return nil, ErrReplicationGroupNotFound
	}

	if err := b.requireAvailableLocked(
		rg.Status, rg.PendingStatus, rg.AvailableAt, ErrReplicationGroupNotAvailable,
	); err != nil {
		return nil, err
	}

	if len(rg.NodeGroups) == 0 {
		if req.NewReplicaCount >= 0 {
			rg.ReplicaCount = req.NewReplicaCount
		}

		b.appendEventLocked(id, "replication-group", "replica count decreased")

		return b.replicationGroupView(rg), nil
	}

	groups := cloneNodeGroups(rg.NodeGroups)

	if err := b.shrinkGroupsLocked(region, rg, groups, req); err != nil {
		return nil, err
	}

	rg.NodeGroups = groups
	rg.ReplicaCount = count32(len(groups[0].Replicas))
	b.markTransitionLocked(&rg.PendingStatus, &rg.AvailableAt, statusModifying)
	b.appendEventLocked(id, "replication-group", "replica count decreased")

	return b.replicationGroupView(rg), nil
}

func (b *InMemoryBackend) shrinkGroupsLocked(
	region string, rg *ReplicationGroup, groups []NodeGroup, req ReplicaCountRequest,
) error {
	if len(req.ReplicasToRemove) > 0 {
		if req.HasNewReplicaCount || len(req.Config) > 0 {
			return fmt.Errorf("%w: ReplicasToRemove cannot be combined with a replica count",
				ErrInvalidParameterCombination)
		}

		return b.removeNamedReplicasLocked(region, rg, groups, req.ReplicasToRemove)
	}

	targets, err := replicaTargets(rg, req)
	if err != nil {
		return err
	}

	if len(targets) == 0 {
		return fmt.Errorf("%w: NewReplicaCount, ReplicaConfiguration or ReplicasToRemove is required",
			ErrInvalidParameterCombination)
	}

	floor := 0
	if rg.MultiAZEnabled && !rg.ClusterModeEnabled {
		floor = 1
	}

	changed := false

	for idx, tgt := range targets {
		current := len(groups[idx].Replicas)

		switch {
		case int(tgt.NewReplicaCount) < floor:
			return fmt.Errorf(
				"%w: a Multi-AZ replication group keeps at least %d replica",
				ErrInvalidParameterValue,
				floor,
			)
		case int(tgt.NewReplicaCount) > current:
			return fmt.Errorf("%w: node group %s only has %d replicas", ErrInvalidParameterValue,
				groups[idx].NodeGroupID, current)
		case int(tgt.NewReplicaCount) < current:
			changed = true
		}
	}

	if !changed {
		return fmt.Errorf("%w: replica count is already at the requested value", ErrNoOperation)
	}

	clusters := b.clustersStore(region)

	for idx, tgt := range targets {
		for len(groups[idx].Replicas) > int(tgt.NewReplicaCount) {
			last := len(groups[idx].Replicas) - 1
			b.removeMemberLocked(clusters, groups[idx].Replicas[last].CacheClusterID)
			groups[idx].Replicas = groups[idx].Replicas[:last]
		}
	}

	return nil
}

func (b *InMemoryBackend) removeNamedReplicasLocked(
	region string, rg *ReplicationGroup, groups []NodeGroup, ids []string,
) error {
	floor := 0
	if rg.MultiAZEnabled && !rg.ClusterModeEnabled {
		floor = 1
	}

	for _, id := range ids {
		if !slices.ContainsFunc(groups, func(ng NodeGroup) bool {
			return slices.ContainsFunc(ng.Replicas, func(n NodeGroupNode) bool { return n.CacheClusterID == id })
		}) {
			return fmt.Errorf("%w: %s is not a replica of %s", ErrInvalidParameterValue, id, rg.ReplicationGroupID)
		}
	}

	clusters := b.clustersStore(region)

	for gi := range groups {
		kept := make([]NodeGroupNode, 0, len(groups[gi].Replicas))
		removed := 0

		for _, r := range groups[gi].Replicas {
			if slices.Contains(ids, r.CacheClusterID) {
				removed++

				continue
			}

			kept = append(kept, r)
		}

		if removed > 0 && len(kept) < floor {
			return fmt.Errorf(
				"%w: a Multi-AZ replication group keeps at least %d replica",
				ErrInvalidParameterValue,
				floor,
			)
		}

		for _, r := range groups[gi].Replicas {
			if slices.Contains(ids, r.CacheClusterID) {
				b.removeMemberLocked(clusters, r.CacheClusterID)
			}
		}

		groups[gi].Replicas = kept
	}

	return nil
}

// attachReplicaLocked adds an existing cluster to rg's single node group as a replica,
// switching it onto the primary's data plane.
func (b *InMemoryBackend) attachReplicaLocked(region string, rg *ReplicationGroup, c *Cluster) error {
	if len(rg.NodeGroups) == 0 {
		c.ReplicationGroupID = rg.ReplicationGroupID

		return nil
	}

	if rg.ClusterModeEnabled {
		return fmt.Errorf("%w: a cluster cannot join a cluster mode enabled replication group",
			ErrInvalidParameterCombination)
	}

	groups := cloneNodeGroups(rg.NodeGroups)
	ng := &groups[0]

	if len(ng.Replicas) >= maxReplicasPerNodeGroup {
		return fmt.Errorf("%w: a node group holds at most %d replicas", ErrNodeQuotaForClusterExceeded,
			maxReplicasPerNodeGroup)
	}

	primary := b.groupPrimaryClusterLocked(region, *ng)
	if primary == nil || primary.Engine != c.Engine {
		return fmt.Errorf("%w: cluster engine must match the replication group", ErrInvalidParameterCombination)
	}

	if c.mini != nil {
		c.mini.Close()
		c.mini = nil
	}

	if b.allocator != nil && c.AllocatedPort > 0 {
		_ = b.allocator.Release(c.AllocatedPort)
	}

	c.AllocatedPort = 0
	c.Port, c.ConnectAddress = primary.Port, primary.ConnectAddress
	c.ReplicationGroupID = rg.ReplicationGroupID
	b.registerClusterDNSLocked(c)

	az := c.PreferredAvailabilityZone
	if az == "" {
		az = defaultReplicaAZ(region, rg, *ng, len(ng.Replicas))
	}

	ng.Replicas = append(ng.Replicas, nodeFor(c, roleReplica, az))
	rg.NodeGroups = groups
	rg.ReplicaCount = count32(len(ng.Replicas))

	return nil
}

// detachMemberLocked removes cluster c from rg's node groups (a replica only).
// It reports whether c may be deleted.
func detachMemberLocked(rg *ReplicationGroup, c *Cluster) error {
	if len(rg.NodeGroups) == 0 {
		return nil
	}

	if rg.MultiAZEnabled || rg.ClusterModeEnabled {
		return ErrClusterInReplicationGroup
	}

	groups := cloneNodeGroups(rg.NodeGroups)

	for gi := range groups {
		if groups[gi].PrimaryNode != nil && groups[gi].PrimaryNode.CacheClusterID == c.ClusterID {
			return ErrClusterInReplicationGroup
		}

		for ri, r := range groups[gi].Replicas {
			if r.CacheClusterID == c.ClusterID {
				groups[gi].Replicas = slices.Delete(groups[gi].Replicas, ri, ri+1)
				rg.NodeGroups = groups
				rg.ReplicaCount = count32(len(groups[0].Replicas))

				return nil
			}
		}
	}

	return nil
}

// ShardConfigRequest carries ModifyReplicationGroupShardConfiguration's members.
type ShardConfigRequest struct {
	Resharding         []ReshardingConfig
	NodeGroupsToRemove []string
	NodeGroupsToRetain []string
	NodeGroupCount     int32
	ApplyImmediately   bool
}

// ModifyReplicationGroupShardConfiguration resizes a cluster mode enabled group to nodeGroupCount shards.
func (b *InMemoryBackend) ModifyReplicationGroupShardConfiguration(
	ctx context.Context, id string, nodeGroupCount int32, applyImmediately bool, resharding []ReshardingConfig,
) (*ReplicationGroup, error) {
	return b.ModifyReplicationGroupShardConfigurationFull(ctx, id, ShardConfigRequest{
		NodeGroupCount: nodeGroupCount, ApplyImmediately: applyImmediately, Resharding: resharding,
	})
}

// shardPlanFor checks a shard-count change against the group and returns how many shards to add.
func shardPlanFor(rg *ReplicationGroup, req ShardConfigRequest) (int, []string, error) {
	current := len(rg.NodeGroups)
	target := int(req.NodeGroupCount)

	if target < 1 || target > maxNodeGroupsPerGroup {
		return 0, nil, fmt.Errorf("%w: NodeGroupCount must be between 1 and %d", ErrInvalidParameterValue,
			maxNodeGroupsPerGroup)
	}

	switch {
	case target == current:
		return 0, nil, fmt.Errorf("%w: the replication group already has %d node groups", ErrNoOperation, current)
	case target > current:
		if len(req.NodeGroupsToRemove) > 0 || len(req.NodeGroupsToRetain) > 0 {
			return 0, nil, fmt.Errorf("%w: NodeGroupsToRemove and NodeGroupsToRetain only apply when shrinking",
				ErrInvalidParameterCombination)
		}

		return target - current, nil, nil
	}

	if len(req.Resharding) > 0 {
		return 0, nil, fmt.Errorf("%w: ReshardingConfiguration only applies when adding node groups",
			ErrInvalidParameterCombination)
	}

	remove, err := shardsToRemove(rg, req, current-target)

	return 0, remove, err
}

func shardsToRemove(rg *ReplicationGroup, req ShardConfigRequest, count int) ([]string, error) {
	if (len(req.NodeGroupsToRemove) > 0) == (len(req.NodeGroupsToRetain) > 0) {
		return nil, fmt.Errorf("%w: specify exactly one of NodeGroupsToRemove and NodeGroupsToRetain",
			ErrInvalidParameterCombination)
	}

	for _, id := range slices.Concat(req.NodeGroupsToRemove, req.NodeGroupsToRetain) {
		if findNodeGroup(rg, id) < 0 {
			return nil, fmt.Errorf("%w: node group %s", ErrNodeGroupNotFound, id)
		}
	}

	var remove []string

	if len(req.NodeGroupsToRemove) > 0 {
		remove = slices.Compact(slices.Sorted(slices.Values(req.NodeGroupsToRemove)))
	} else {
		for _, ng := range rg.NodeGroups {
			if !slices.Contains(req.NodeGroupsToRetain, ng.NodeGroupID) {
				remove = append(remove, ng.NodeGroupID)
			}
		}
	}

	if len(remove) != count {
		return nil, fmt.Errorf("%w: the node groups to remove or retain do not match NodeGroupCount",
			ErrInvalidParameterValue)
	}

	return remove, nil
}

// ModifyReplicationGroupShardConfigurationFull resizes the shard set, creating or removing member clusters.
func (b *InMemoryBackend) ModifyReplicationGroupShardConfigurationFull(
	ctx context.Context, id string, req ShardConfigRequest,
) (*ReplicationGroup, error) {
	if !req.ApplyImmediately {
		return nil, ErrApplyImmediatelyRequired
	}

	region := getRegion(ctx, b.region)

	var (
		adds    int
		remove  []string
		current int
		planErr error
	)

	func() {
		b.mu.Lock("ModifyReplicationGroupShardConfiguration.plan")
		defer b.mu.Unlock()

		rg, err := b.shardableGroupLocked(region, id)
		if err != nil {
			planErr = err

			return
		}

		current = len(rg.NodeGroups)
		adds, remove, planErr = shardPlanFor(rg, req)
	}()

	if planErr != nil {
		return nil, planErr
	}

	engines, err := b.startShardEngines(adds, 0)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("ModifyReplicationGroupShardConfiguration.apply")
	defer b.mu.Unlock()

	rg, err := b.shardableGroupLocked(region, id)
	if err == nil && len(rg.NodeGroups) != current {
		err = fmt.Errorf("%w: replication group changed while resharding", ErrReplicationGroupNotAvailable)
	}

	if err != nil {
		for _, e := range engines {
			b.releaseEngine(e)
		}

		return nil, err
	}

	groups := cloneNodeGroups(rg.NodeGroups)
	groups = b.removeShardsLocked(region, groups, remove)
	groups = b.addShardsLocked(region, rg, groups, engines, req.Resharding)
	assignEvenSlots(groups)

	rg.NodeGroups = groups
	b.markTransitionLocked(&rg.PendingStatus, &rg.AvailableAt, statusModifying)
	b.appendEventLocked(id, "replication-group", "shard configuration modified")

	return b.replicationGroupView(rg), nil
}

func (b *InMemoryBackend) shardableGroupLocked(region, id string) (*ReplicationGroup, error) {
	rg, ok := b.replicationGroupsStore(region).Get(id)
	if !ok {
		return nil, ErrReplicationGroupNotFound
	}

	if err := b.requireAvailableLocked(
		rg.Status, rg.PendingStatus, rg.AvailableAt, ErrReplicationGroupNotAvailable,
	); err != nil {
		return nil, err
	}

	if !rg.ClusterModeEnabled {
		return nil, ErrClusterModeRequired
	}

	return rg, nil
}

func assignEvenSlots(groups []NodeGroup) {
	for i := range groups {
		groups[i].Slots = evenSlots(i, len(groups))
	}
}

func (b *InMemoryBackend) removeShardsLocked(region string, groups []NodeGroup, remove []string) []NodeGroup {
	clusters := b.clustersStore(region)
	kept := make([]NodeGroup, 0, len(groups))

	for _, ng := range groups {
		if !slices.Contains(remove, ng.NodeGroupID) {
			kept = append(kept, ng)

			continue
		}

		for _, r := range ng.Replicas {
			b.removeMemberLocked(clusters, r.CacheClusterID)
		}

		if ng.PrimaryNode != nil {
			b.removeMemberLocked(clusters, ng.PrimaryNode.CacheClusterID)
		}
	}

	return kept
}

// addShardsLocked appends one shard per engine, each shaped like the first existing shard.
func (b *InMemoryBackend) addShardsLocked(
	region string, rg *ReplicationGroup, groups []NodeGroup, engines []*clusterEngine, resharding []ReshardingConfig,
) []NodeGroup {
	if len(engines) == 0 || len(groups) == 0 {
		return groups
	}

	template := b.groupPrimaryClusterLocked(region, groups[0])
	replicas := len(groups[0].Replicas)
	nextID := nextNodeGroupNumber(groups)

	for i, eng := range engines {
		ngID := fmt.Sprintf("%04d", nextID+i)
		zones := reshardingZones(resharding, i, ngID)

		primary := b.insertClusterLocked(region, memberClusterID(rg.ReplicationGroupID, ngID, 1, true),
			template.Engine, template.NodeType, template.CacheParameterGroupName,
			template.PreferredMaintenanceWindow, template.SnapshotWindow, 1, eng)
		copyMemberSettings(primary, template)
		primary.PreferredAvailabilityZone = zoneAt(zones, 0, template.PreferredAvailabilityZone)
		initClusterNodes(primary, region)

		ng := NodeGroup{
			NodeGroupID: ngID,
			Status:      statusAvailable,
			PrimaryNode: new(NodeGroupNode),
		}
		*ng.PrimaryNode = nodeFor(primary, rolePrimary, primary.PreferredAvailabilityZone)

		for r := range replicas {
			np := nodePlan{az: zoneAt(zones, r+1, primary.PreferredAvailabilityZone)}
			_ = b.addReplicaLocked(region, rg, &ng, np)
		}

		groups = append(groups, ng)
	}

	return groups
}

func nextNodeGroupNumber(groups []NodeGroup) int {
	highest := len(groups)

	for _, ng := range groups {
		if n, err := strconv.Atoi(ng.NodeGroupID); err == nil {
			highest = max(highest, n)
		}
	}

	return highest + 1
}

// reshardingZones returns the preferred zones for the i-th new node group.
func reshardingZones(cfg []ReshardingConfig, i int, ngID string) []string {
	for _, rc := range cfg {
		if rc.NodeGroupID != "" && rc.NodeGroupID == ngID {
			return rc.PreferredAvailabilityZones
		}
	}

	if i < len(cfg) && cfg[i].NodeGroupID == "" {
		return cfg[i].PreferredAvailabilityZones
	}

	return nil
}

// zoneAt returns zones[i%len(zones)], or fallback when no zones were given.
func zoneAt(zones []string, i int, fallback string) string {
	if len(zones) == 0 {
		return fallback
	}

	return zones[i%len(zones)]
}

// CheckReplicaAttach validates that a new cluster of the given engine may join replicationGroupID as a replica.
func (b *InMemoryBackend) CheckReplicaAttach(ctx context.Context, replicationGroupID, engine string) error {
	b.mu.RLock("CheckReplicaAttach")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	rg, ok := b.replicationGroupsStoreRO(region).Get(replicationGroupID)
	if !ok {
		return ErrReplicationGroupNotFound
	}

	if len(rg.NodeGroups) == 0 {
		return nil
	}

	if rg.ClusterModeEnabled {
		return fmt.Errorf("%w: a cluster cannot join a cluster mode enabled replication group",
			ErrInvalidParameterCombination)
	}

	if len(rg.NodeGroups[0].Replicas) >= maxReplicasPerNodeGroup {
		return fmt.Errorf("%w: a node group holds at most %d replicas", ErrNodeQuotaForClusterExceeded,
			maxReplicasPerNodeGroup)
	}

	if rg.Engine != "" && engine != "" && rg.Engine != engine {
		return fmt.Errorf("%w: cluster engine must match the replication group", ErrInvalidParameterCombination)
	}

	return nil
}
