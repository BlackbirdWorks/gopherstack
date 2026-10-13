package elasticache

import (
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"

	gopherDNS "github.com/blackbirdworks/gopherstack/pkgs/dns"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	maxReplicasPerNodeGroup = 5
	maxCacheClustersInGroup = maxReplicasPerNodeGroup + 1
	maxNodeGroupsPerGroup   = 500

	roleReplica = "replica"

	firstReplicaNumber = 2
	rolePrimary        = "primary"

	groupRolePrimary   = "primary"
	groupRoleSecondary = "secondary"
)

// NodeEndpoint is a DNS address and port.
type NodeEndpoint struct {
	Address string `json:"address"`
	Port    int    `json:"port"`
}

// NodeGroupConfig is one CreateReplicationGroup NodeGroupConfiguration entry.
type NodeGroupConfig struct {
	ReplicaCount       *int32
	NodeGroupID        string
	Slots              string
	PrimaryAZ          string
	PrimaryOutpostArn  string
	ReplicaAZs         []string
	ReplicaOutpostArns []string
}

type nodePlan struct {
	az      string
	outpost string
}

type shardPlan struct {
	id       string
	slots    string
	primary  nodePlan
	replicas []nodePlan
}

func cloneNodeGroups(in []NodeGroup) []NodeGroup {
	if in == nil {
		return nil
	}

	out := make([]NodeGroup, len(in))

	for i, ng := range in {
		out[i] = ng
		out[i].Replicas = slices.Clone(ng.Replicas)

		if ng.PrimaryNode != nil {
			p := *ng.PrimaryNode
			out[i].PrimaryNode = &p
		}

		if ng.PrimaryEndpoint != nil {
			e := *ng.PrimaryEndpoint
			out[i].PrimaryEndpoint = &e
		}

		if ng.ReaderEndpoint != nil {
			e := *ng.ReaderEndpoint
			out[i].ReaderEndpoint = &e
		}
	}

	return out
}

// evenSlots splits the 16384 hash slots across shards groups; "" when the group is not cluster-mode.
func evenSlots(i, shards int) string {
	size := redisClusterHashSlots / shards
	start := i * size
	end := start + size - 1

	if i == shards-1 {
		end = redisClusterHashSlots - 1
	}

	return fmt.Sprintf("%d-%d", start, end)
}

// shardCount resolves the number of node groups and whether the group is cluster mode enabled.
func shardCount(opts ReplicationGroupCreateOpts) (int, bool, error) {
	cfgs := len(opts.NodeGroupConfiguration)
	clusterMode := opts.ClusterModeEnabled || opts.ClusterMode == clusterModeEnabled ||
		opts.NumNodeGroups > 1 || cfgs > 1
	shards := 1

	switch {
	case cfgs > 0:
		shards = cfgs

		if opts.NumNodeGroups > 0 && int(opts.NumNodeGroups) != shards {
			return 0, false, fmt.Errorf("%w: NumNodeGroups must equal the number of NodeGroupConfiguration entries",
				ErrInvalidParameterCombination)
		}
	case opts.NumNodeGroups > 0:
		shards = int(opts.NumNodeGroups)
	}

	if shards > maxNodeGroupsPerGroup {
		return 0, false, fmt.Errorf("%w: a replication group holds at most %d node groups",
			ErrNodeGroupsQuotaExceeded, maxNodeGroupsPerGroup)
	}

	if opts.ClusterMode == statusDisabled && shards > 1 {
		return 0, false, fmt.Errorf(
			"%w: ClusterMode disabled allows a single node group",
			ErrInvalidParameterCombination,
		)
	}

	return shards, clusterMode, nil
}

// planShards turns CreateReplicationGroup's topology members into a per-shard plan.
func planShards(opts ReplicationGroupCreateOpts, region string) ([]shardPlan, bool, error) {
	shards, clusterMode, err := shardCount(opts)
	if err != nil {
		return nil, false, err
	}

	defaultReplicas, err := defaultReplicaCount(opts, shards, clusterMode)
	if err != nil {
		return nil, false, err
	}

	plans := make([]shardPlan, 0, shards)
	seen := make(map[string]bool, shards)

	for i := range shards {
		var cfg NodeGroupConfig
		if i < len(opts.NodeGroupConfiguration) {
			cfg = opts.NodeGroupConfiguration[i]
		}

		plan, planErr := planShard(opts, cfg, shardPosition{index: i, total: shards, clusterMode: clusterMode},
			defaultReplicas, region)
		if planErr != nil {
			return nil, false, planErr
		}

		if seen[plan.id] {
			return nil, false, fmt.Errorf("%w: duplicate NodeGroupId %s", ErrInvalidParameterValue, plan.id)
		}

		seen[plan.id] = true
		plans = append(plans, plan)
	}

	return plans, clusterMode, nil
}

// defaultReplicaCount is the per-shard replica count implied by NumCacheClusters,
// PreferredCacheClusterAZs and ReplicasPerNodeGroup.
func defaultReplicaCount(opts ReplicationGroupCreateOpts, shards int, clusterMode bool) (int, error) {
	hasReplicas := opts.HasReplicasPerNodeGroup || opts.ReplicasPerNodeGroup != 0

	if opts.NumCacheClusters > 0 || len(opts.PreferredCacheClusterAZs) > 0 {
		return replicasFromClusterCount(opts, shards, clusterMode, hasReplicas)
	}

	if !hasReplicas {
		return 0, nil
	}

	if opts.ReplicasPerNodeGroup < 0 || opts.ReplicasPerNodeGroup > maxReplicasPerNodeGroup {
		return 0, fmt.Errorf("%w: ReplicasPerNodeGroup must be between 0 and %d", ErrInvalidParameterValue,
			maxReplicasPerNodeGroup)
	}

	return int(opts.ReplicasPerNodeGroup), nil
}

func replicasFromClusterCount(opts ReplicationGroupCreateOpts, shards int, clusterMode, hasReplicas bool) (int, error) {
	if shards > 1 || clusterMode {
		return 0, fmt.Errorf("%w: NumCacheClusters and PreferredCacheClusterAZs are not used with multiple node groups",
			ErrInvalidParameterCombination)
	}

	if hasReplicas {
		return 0, fmt.Errorf("%w: ReplicasPerNodeGroup cannot be combined with NumCacheClusters",
			ErrInvalidParameterCombination)
	}

	n := int(opts.NumCacheClusters)
	if n == 0 {
		n = len(opts.PreferredCacheClusterAZs)
	}

	if len(opts.PreferredCacheClusterAZs) > 0 && len(opts.PreferredCacheClusterAZs) != n {
		return 0, fmt.Errorf("%w: PreferredCacheClusterAZs must list exactly NumCacheClusters (%d) zones",
			ErrInvalidParameterCombination, n)
	}

	if n < 1 || n > maxCacheClustersInGroup {
		return 0, fmt.Errorf("%w: NumCacheClusters must be between 1 and %d", ErrInvalidParameterValue,
			maxCacheClustersInGroup)
	}

	return n - 1, nil
}

type shardPosition struct {
	index       int
	total       int
	clusterMode bool
}

func planShard(
	opts ReplicationGroupCreateOpts, cfg NodeGroupConfig, pos shardPosition, defaultReplicas int, region string,
) (shardPlan, error) {
	plan := shardPlan{id: cfg.NodeGroupID, slots: cfg.Slots}
	if plan.id == "" {
		plan.id = fmt.Sprintf("%04d", pos.index+1)
	}

	if plan.slots == "" && pos.clusterMode {
		plan.slots = evenSlots(pos.index, pos.total)
	}

	replicas := defaultReplicas
	if cfg.ReplicaCount != nil {
		replicas = int(*cfg.ReplicaCount)
	}

	if replicas < 0 || replicas > maxReplicasPerNodeGroup {
		return plan, fmt.Errorf("%w: ReplicaCount must be between 0 and %d", ErrInvalidParameterValue,
			maxReplicasPerNodeGroup)
	}

	if (len(cfg.ReplicaAZs) > 0 && len(cfg.ReplicaAZs) != replicas) ||
		(len(cfg.ReplicaOutpostArns) > 0 && len(cfg.ReplicaOutpostArns) != replicas) {
		return plan, fmt.Errorf("%w: ReplicaAvailabilityZones and ReplicaOutpostArns must match ReplicaCount (%d)",
			ErrInvalidParameterCombination, replicas)
	}

	primaryAZ, azs := cfg.PrimaryAZ, cfg.ReplicaAZs
	if pos.total == 1 && len(opts.PreferredCacheClusterAZs) > 0 {
		primaryAZ, azs = opts.PreferredCacheClusterAZs[0], opts.PreferredCacheClusterAZs[1:]
	}

	plan.primary = nodePlan{az: firstNonEmpty(primaryAZ, zoneFor(region, 0)), outpost: cfg.PrimaryOutpostArn}

	for j := range replicas {
		np := nodePlan{az: replicaZone(opts, plan.primary.az, azs, j, region)}
		if j < len(cfg.ReplicaOutpostArns) {
			np.outpost = cfg.ReplicaOutpostArns[j]
		}

		plan.replicas = append(plan.replicas, np)
	}

	return plan, nil
}

// replicaZone picks the zone of the j-th replica: an explicit one, else spread when Multi-AZ, else the primary's.
func replicaZone(opts ReplicationGroupCreateOpts, primaryAZ string, azs []string, j int, region string) string {
	switch {
	case j < len(azs):
		return azs[j]
	case opts.MultiAZEnabled:
		return zoneFor(region, j+1)
	}

	return primaryAZ
}

// memberClusterID names the n-th (1-based) cluster of a node group.
func memberClusterID(rgID, ngID string, n int, clusterMode bool) string {
	if clusterMode {
		return fmt.Sprintf("%s-%s-%03d", rgID, ngID, n)
	}

	return fmt.Sprintf("%s-%03d", rgID, n)
}

// nodeNumber returns the numeric suffix of a member cluster ID.
func nodeNumber(clusterID string) int {
	idx := strings.LastIndex(clusterID, "-")
	n, err := strconv.Atoi(clusterID[idx+1:])

	if err != nil {
		return 0
	}

	return n
}

// registerEndpointDNS binds a group-level endpoint name to the shard's data plane.
func (b *InMemoryBackend) registerEndpointDNS(host, connectAddr string) {
	if b.dnsRegistrar == nil || host == "" {
		return
	}

	b.dnsRegistrar.Register(host)

	if connectAddr == "" {
		return
	}

	if ip, _, err := net.SplitHostPort(connectAddr); err == nil && ip != "" {
		if rr, ok := b.dnsRegistrar.(dnsRecordRegistrar); ok {
			rr.RegisterRecord(host, "A", []string{ip})
		}
	}
}

func (b *InMemoryBackend) deregisterEndpointDNS(e *NodeEndpoint) {
	if b.dnsRegistrar != nil && e != nil && e.Address != "" {
		b.dnsRegistrar.Deregister(e.Address)
	}
}

func syntheticGroupEndpoint(name, region string, port int) *NodeEndpoint {
	return &NodeEndpoint{Address: gopherDNS.SyntheticHostname(name, randomSuffix(), region, "cache"), Port: port}
}

// memberTemplate carries the replication-group-wide settings every member cluster inherits.
type memberTemplate struct {
	region string
	opts   ReplicationGroupCreateOpts
}

// newMemberLocked creates a member cluster row. A nil owner makes it the shard's
// data-plane owner (holding eng); otherwise it shares owner's engine.
func (b *InMemoryBackend) newMemberLocked(
	t memberTemplate, id string, np nodePlan, eng *clusterEngine, owner *Cluster,
) *Cluster {
	if owner != nil {
		eng = &clusterEngine{port: owner.Port, connectAddr: owner.ConnectAddress}
	}

	o := t.opts

	c := b.insertClusterLocked(t.region, id, o.Engine, o.CacheNodeType, o.ParameterGroupName,
		o.MaintenanceWindow, o.SnapshotWindow, 1, eng)

	if o.EngineVersion != "" {
		c.EngineVersion = o.EngineVersion
	}

	c.ReplicationGroupID = o.ID
	c.PreferredAvailabilityZone = np.az
	c.PreferredOutpostArn = np.outpost
	c.SubnetGroupName = o.SubnetGroupName
	c.SecurityGroupIDs = slices.Clone(o.SecurityGroupIDs)
	c.CacheSecurityGroupNames = slices.Clone(o.CacheSecurityGroupNames)
	c.SnapshotRetentionLimit = o.SnapshotRetentionLimit
	c.TransitEncryptionEnabled = o.TransitEncryptionEnabled
	c.TransitEncryptionMode = o.TransitEncryptionMode
	c.AtRestEncryptionEnabled = o.AtRestEncryptionEnabled
	c.AuthTokenEnabled = o.AuthTokenEnabled
	c.KmsKeyID = o.KmsKeyID
	c.NetworkType = o.NetworkType
	c.IPDiscovery = o.IPDiscovery
	c.AutoMinorVersionUpgrade = o.AutoMinorVersionUpgrade
	c.LogDeliveryConfigurations = slices.Clone(o.LogDeliveryConfigurations)

	if o.NotificationTopicArn != "" {
		c.NotificationTopicArn = o.NotificationTopicArn
		c.NotificationTopicStatus = statusActive
	}

	initClusterNodes(c, t.region)

	return c
}

func nodeFor(c *Cluster, role, az string) NodeGroupNode {
	return NodeGroupNode{
		CacheClusterID:            c.ClusterID,
		CacheNodeID:               nodeIDFor(1),
		CurrentRole:               role,
		PreferredAvailabilityZone: az,
		ReadEndpointAddress:       c.Endpoint,
		ReadEndpointPort:          c.Port,
	}
}

// shardEngines starts one data-plane engine per shard that has no existing primary cluster.
func (b *InMemoryBackend) startShardEngines(count, port int) ([]*clusterEngine, error) {
	engines := make([]*clusterEngine, 0, count)

	for range count {
		eng, err := b.startClusterEngine(port)
		if err != nil {
			for _, e := range engines {
				b.releaseEngine(e)
			}

			return nil, err
		}

		engines = append(engines, eng)
	}

	return engines, nil
}

// buildTopologyLocked creates member clusters and node groups for rg. adopted,
// when non-nil, is an existing cluster serving as the first shard's primary.
func (b *InMemoryBackend) buildTopologyLocked(
	rg *ReplicationGroup, t memberTemplate, plans []shardPlan, clusterMode bool,
	engines []*clusterEngine, adopted *Cluster,
) {
	groups := make([]NodeGroup, 0, len(plans))
	next := 0

	for si, plan := range plans {
		primaryCluster := adopted
		if si > 0 || adopted == nil {
			id := memberClusterID(rg.ReplicationGroupID, plan.id, 1, clusterMode)
			primaryCluster = b.newMemberLocked(t, id, plan.primary, engines[next], nil)
			next++
		} else {
			primaryCluster.ReplicationGroupID = rg.ReplicationGroupID
			plan.primary.az = primaryCluster.PreferredAvailabilityZone
		}

		primary := nodeFor(primaryCluster, rolePrimary, plan.primary.az)
		ng := NodeGroup{NodeGroupID: plan.id, Status: statusAvailable, Slots: plan.slots, PrimaryNode: &primary}

		for ri, rp := range plan.replicas {
			id := memberClusterID(rg.ReplicationGroupID, plan.id, ri+firstReplicaNumber, clusterMode)
			rc := b.newMemberLocked(t, id, rp, nil, primaryCluster)
			ng.Replicas = append(ng.Replicas, nodeFor(rc, roleReplica, rp.az))
		}

		if !clusterMode {
			ng.PrimaryEndpoint = syntheticGroupEndpoint(rg.ReplicationGroupID, t.region, primaryCluster.Port)
			ng.ReaderEndpoint = syntheticGroupEndpoint(rg.ReplicationGroupID+"-ro", t.region, primaryCluster.Port)
			b.registerEndpointDNS(ng.PrimaryEndpoint.Address, primaryCluster.ConnectAddress)
			b.registerEndpointDNS(ng.ReaderEndpoint.Address, primaryCluster.ConnectAddress)
		}

		groups = append(groups, ng)
	}

	if clusterMode {
		first := groups[0].PrimaryNode
		rg.ConfigurationEndpoint = syntheticGroupEndpoint(rg.ReplicationGroupID, t.region, first.ReadEndpointPort)

		if c, ok := b.clustersStore(t.region).Get(first.CacheClusterID); ok {
			b.registerEndpointDNS(rg.ConfigurationEndpoint.Address, c.ConnectAddress)
		}
	}

	rg.NodeGroups = groups
	rg.ReplicaCount = count32(len(plans[0].replicas))
}

// memberClusterIDs lists every cluster in rg's node groups, primary first per group.
func memberClusterIDs(rg *ReplicationGroup) []string {
	var ids []string

	for _, ng := range rg.NodeGroups {
		if ng.PrimaryNode != nil {
			ids = append(ids, ng.PrimaryNode.CacheClusterID)
		}

		for _, r := range ng.Replicas {
			ids = append(ids, r.CacheClusterID)
		}
	}

	return ids
}

// releaseGroupLocked removes rg's endpoints and member clusters. When retainPrimaries
// is set the primary clusters survive as standalone clusters.
func (b *InMemoryBackend) releaseGroupLocked(region string, rg *ReplicationGroup, retainPrimaries bool) {
	b.deregisterEndpointDNS(rg.ConfigurationEndpoint)

	clusters := b.clustersStore(region)

	for _, ng := range rg.NodeGroups {
		b.deregisterEndpointDNS(ng.PrimaryEndpoint)
		b.deregisterEndpointDNS(ng.ReaderEndpoint)

		for _, r := range ng.Replicas {
			b.removeMemberLocked(clusters, r.CacheClusterID)
		}

		if ng.PrimaryNode == nil {
			continue
		}

		if retainPrimaries {
			if c, ok := clusters.Get(ng.PrimaryNode.CacheClusterID); ok {
				c.ReplicationGroupID = ""
			}

			continue
		}

		b.removeMemberLocked(clusters, ng.PrimaryNode.CacheClusterID)
	}
}

// removeMemberLocked deletes one member cluster, handing its data-plane
// engine to a surviving cluster of the same group when it owns one.
func (b *InMemoryBackend) removeMemberLocked(clusters *store.Table[Cluster], id string) {
	c, ok := clusters.Get(id)
	if !ok {
		return
	}

	handoverEngineLocked(clusters, c)
	b.releaseClusterLocked(c)
	clusters.Delete(id)
}

// handoverEngineLocked moves c's miniredis instance and port to another cluster
// serving the same data plane, so deleting c leaves the shard's replicas usable.
func handoverEngineLocked(clusters *store.Table[Cluster], c *Cluster) {
	if (c.mini == nil && c.AllocatedPort == 0) || c.ReplicationGroupID == "" {
		return
	}

	for _, other := range clusters.All() {
		if other.ClusterID != c.ClusterID && other.ReplicationGroupID == c.ReplicationGroupID &&
			other.mini == nil && other.AllocatedPort == 0 && other.ConnectAddress == c.ConnectAddress {
			other.mini, other.AllocatedPort = c.mini, c.AllocatedPort
			c.mini, c.AllocatedPort = nil, 0

			return
		}
	}
}

// count32 narrows a small node or shard count.
func count32(n int) int32 {
	return int32(n) //nolint:gosec // G115: bounded by 500 shards of at most 6 nodes
}
