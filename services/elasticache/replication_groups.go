package elasticache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// authTokenHexLen is the byte length of a generated auth token before hex encoding.
const authTokenHexLen = 32

// redisClusterHashSlots is the total number of hash slots in a Redis cluster (gap #2).
const redisClusterHashSlots = 16384

// dataTieringMinMajorVersion is the minimum engine major version required for data tiering (gap #9).
const dataTieringMinMajorVersion = 7

// semverSplitParts is the max parts to split a semver string into for major version extraction.
const semverSplitParts = 2

// decimalBase is the base for decimal digit accumulation.
const decimalBase = 10

// Transit encryption mode constants.
const (
	transitEncryptionModePreferred = "preferred"
	transitEncryptionModeRequired  = "required"
)

func (b *InMemoryBackend) replicationGroupARN(region, id string) string {
	return arn.Build("elasticache", region, b.accountID, "replicationgroup:"+id)
}

// createReplicationGroupLocked creates a replication group assuming b.mu is already held.
func (b *InMemoryBackend) createReplicationGroupLocked(
	region, id, description, paramGroupName, maintenanceWindow, snapshotWindow string,
) *ReplicationGroup {
	rg := &ReplicationGroup{
		ReplicationGroupID:         id,
		Description:                description,
		Status:                     statusAvailable,
		ARN:                        b.replicationGroupARN(region, id),
		Tags:                       tags.New("elasticache.rg." + id + ".tags"),
		CreatedAt:                  time.Now(),
		CacheParameterGroupName:    paramGroupName,
		PreferredMaintenanceWindow: maintenanceWindow,
		SnapshotWindow:             snapshotWindow,
	}
	b.markCreatingLocked(&rg.PendingStatus, &rg.AvailableAt)
	b.replicationGroupsStore(region).Put(rg)
	b.appendEventLocked(id, "replication-group", "replication group created")

	return rg
}

// CreateReplicationGroup creates a replication group.
func (b *InMemoryBackend) CreateReplicationGroup(
	ctx context.Context,
	id, description string,
) (*ReplicationGroup, error) {
	b.mu.Lock("CreateReplicationGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	b.pruneRegionLocked(region)
	if _, exists := b.replicationGroupsStore(region).Get(id); exists {
		return nil, ErrReplicationGroupAlreadyExists
	}

	return b.replicationGroupView(b.createReplicationGroupLocked(region, id, description, "", "", "")), nil
}

// CreateReplicationGroupWithOptions creates a replication group with optional parameter group and scheduling windows.
func (b *InMemoryBackend) CreateReplicationGroupWithOptions(
	ctx context.Context,
	id, description, paramGroupName, maintenanceWindow, snapshotWindow string,
) (*ReplicationGroup, error) {
	b.mu.Lock("CreateReplicationGroupWithOptions")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	if _, exists := b.replicationGroupsStore(region).Get(id); exists {
		return nil, ErrReplicationGroupAlreadyExists
	}

	if paramGroupName != "" {
		if _, ok := b.parameterGroupsStore(region).Get(paramGroupName); !ok {
			return nil, ErrParameterGroupNotFound
		}
	}

	return b.replicationGroupView(b.createReplicationGroupLocked(
		region,
		id,
		description,
		paramGroupName,
		maintenanceWindow,
		snapshotWindow,
	)), nil
}

// DeleteReplicationGroup removes a replication group and all its member clusters.
func (b *InMemoryBackend) DeleteReplicationGroup(ctx context.Context, id string) error {
	return b.DeleteReplicationGroupFull(ctx, id, false)
}

// DeleteReplicationGroupFull removes a replication group. With retainPrimary
// the replicas are deleted but each primary cluster survives as a standalone cluster.
func (b *InMemoryBackend) DeleteReplicationGroupFull(ctx context.Context, id string, retainPrimary bool) error {
	b.mu.Lock("DeleteReplicationGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	b.pruneRegionLocked(region)
	tbl := b.replicationGroupsStore(region)
	rg, exists := tbl.Get(id)
	if !exists || isReaped(b.now(), rg.PendingStatus, rg.AvailableAt) {
		return ErrReplicationGroupNotFound
	}
	if err := b.requireAvailableLocked(
		rg.Status, rg.PendingStatus, rg.AvailableAt, ErrReplicationGroupNotAvailable,
	); err != nil {
		return err
	}

	if rg.GlobalReplicationGroupID != "" {
		return fmt.Errorf("%w: replication group belongs to global datastore %s",
			ErrReplicationGroupNotAvailable, rg.GlobalReplicationGroupID)
	}

	b.releaseGroupLocked(region, rg, retainPrimary)

	if d := b.pendingUntil(); !d.IsZero() {
		rg.PendingStatus = statusDeleting
		rg.AvailableAt = d
		b.appendEventLocked(id, "replication-group", "replication group deleting")

		return nil
	}

	rg.Tags.Close()
	tbl.Delete(id)
	b.appendEventLocked(id, "replication-group", "replication group deleted")

	return nil
}

// removeReplicationGroupLocked drops a replication group and its members without state checks.
func (b *InMemoryBackend) removeReplicationGroupLocked(region, id string) {
	tbl := b.replicationGroupsStore(region)

	rg, ok := tbl.Get(id)
	if !ok {
		return
	}

	b.releaseGroupLocked(region, rg, false)
	rg.Tags.Close()
	tbl.Delete(id)
}

// DescribeReplicationGroups returns one replication group by id, or a paginated list of all when id is empty.
func (b *InMemoryBackend) DescribeReplicationGroups(
	ctx context.Context,
	id, marker string,
	maxRecords int,
) (page.Page[ReplicationGroup], error) {
	b.mu.RLock("DescribeReplicationGroups")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	p, err := describePaged(b.replicationGroupsStoreRO(region), id, ErrReplicationGroupNotFound, nil,
		func(rg ReplicationGroup) string { return rg.ReplicationGroupID }, marker, maxRecords)

	return b.finalizeReplicationGroupPage(id, p, err)
}

// ModifyReplicationGroup modifies an existing replication group.
func (b *InMemoryBackend) ModifyReplicationGroup(
	ctx context.Context,
	id, description, paramGroupName, engineVersion, cacheNodeType, maintenanceWindow, snapshotWindow string,
	automaticFailoverEnabled, multiAZEnabled *bool,
) (*ReplicationGroup, error) {
	b.mu.Lock("ModifyReplicationGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	rg, exists := b.replicationGroupsStore(region).Get(id)
	if !exists {
		return nil, ErrReplicationGroupNotFound
	}

	if description != "" {
		rg.Description = description
	}

	if paramGroupName != "" {
		if _, ok := b.parameterGroupsStore(region).Get(paramGroupName); !ok {
			return nil, ErrParameterGroupNotFound
		}
		rg.CacheParameterGroupName = paramGroupName
	}

	if engineVersion != "" {
		rg.EngineVersion = engineVersion
	}

	if cacheNodeType != "" {
		rg.CacheNodeType = cacheNodeType
	}

	if automaticFailoverEnabled != nil {
		if *automaticFailoverEnabled {
			rg.AutomaticFailover = statusEnabled
		} else {
			rg.AutomaticFailover = statusDisabled
		}
	}

	if multiAZEnabled != nil {
		rg.MultiAZEnabled = *multiAZEnabled
	}

	if maintenanceWindow != "" {
		rg.PreferredMaintenanceWindow = maintenanceWindow
	}

	if snapshotWindow != "" {
		rg.SnapshotWindow = snapshotWindow
	}

	b.markTransitionLocked(&rg.PendingStatus, &rg.AvailableAt, statusModifying)
	b.appendEventLocked(id, "replication-group", "replication group modified")

	return b.replicationGroupView(rg), nil
}

// ----------------------------------------
// generateAuthToken creates a random 64-char hex token (gap #4)
// ----------------------------------------

func generateAuthToken() string {
	b := make([]byte, authTokenHexLen)
	_, _ = rand.Read(b)

	return hex.EncodeToString(b)
}

// ----------------------------------------
// validateCreateOpts validates cross-field constraints on create options
// ----------------------------------------

func validateCreateOpts(opts ReplicationGroupCreateOpts) error {
	if opts.DataTieringEnabled {
		if err := validateDataTiering(opts.CacheNodeType, opts.Engine, opts.EngineVersion); err != nil {
			return err
		}
	}

	if opts.TransitEncryptionMode == transitEncryptionModeRequired && !opts.AuthTokenEnabled {
		return ErrAuthTokenRequiredForMode
	}

	return nil
}

// validateDataTiering checks node-type and engine constraints for data tiering (gap #9).
func validateDataTiering(nodeType, engine, engineVersion string) error {
	if nodeType != "" && !strings.HasPrefix(nodeType, "cache.r7g") {
		return ErrDataTieringInvalid
	}

	if engine != "" && engine != engineRedis && engine != engineValkey {
		return ErrDataTieringInvalid
	}

	if engineVersion != "" {
		major := majorVersion(engineVersion)
		if major < dataTieringMinMajorVersion {
			return ErrDataTieringInvalid
		}
	}

	return nil
}

// majorVersion parses the leading integer from a semver string.
func majorVersion(v string) int {
	parts := strings.SplitN(v, ".", semverSplitParts)
	if len(parts) == 0 {
		return 0
	}

	n := 0
	for _, r := range parts[0] {
		if r < '0' || r > '9' {
			break
		}
		n = n*decimalBase + int(r-'0')
	}

	return n
}

// ----------------------------------------
// CreateReplicationGroupFull (gaps #1-#15)
// ----------------------------------------

// CreateReplicationGroupFull creates a replication group, its node groups and
// one member cluster per node.
func (b *InMemoryBackend) CreateReplicationGroupFull(
	ctx context.Context,
	opts ReplicationGroupCreateOpts,
) (*ReplicationGroup, error) {
	region := getRegion(ctx, b.region)

	var (
		prep    preparedGroup
		prepErr error
	)

	func() {
		b.mu.Lock("CreateReplicationGroupFull.prepare")
		defer b.mu.Unlock()
		b.pruneRegionLocked(region)

		prep, prepErr = b.prepareGroupLocked(region, &opts)
	}()

	if prepErr != nil {
		return nil, prepErr
	}

	need := len(prep.plans)
	if prep.adoptID != "" {
		need--
	}

	engines, err := b.startShardEngines(need, opts.Port)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CreateReplicationGroupFull.insert")
	defer b.mu.Unlock()

	rg, err := b.insertGroupLocked(region, opts, prep, engines)
	if err != nil {
		for _, e := range engines {
			b.releaseEngine(e)
		}

		return nil, err
	}

	return b.replicationGroupView(rg), nil
}

// preparedGroup is the validated topology of a replication group about to be created.
type preparedGroup struct {
	adoptID     string
	plans       []shardPlan
	clusterMode bool
}

// prepareGroupLocked validates opts against current state and resolves the
// topology. It may fill engine, version and node type from the restore
// snapshot or the adopted primary cluster. Must hold b.mu.
func (b *InMemoryBackend) prepareGroupLocked(region string, opts *ReplicationGroupCreateOpts) (preparedGroup, error) {
	var prep preparedGroup

	if _, exists := b.replicationGroupsStore(region).Get(opts.ID); exists {
		return prep, ErrReplicationGroupAlreadyExists
	}

	if err := b.checkGroupReferencesLocked(region, opts); err != nil {
		return prep, err
	}

	if err := validateCreateOpts(*opts); err != nil {
		return prep, err
	}

	if err := b.adoptPrimaryLocked(region, opts, &prep); err != nil {
		return prep, err
	}

	if err := validateSnapshotArns(opts.SnapshotArns, opts.Engine); err != nil {
		return prep, err
	}

	plans, clusterMode, err := planShards(*opts, region)
	if err != nil {
		return prep, err
	}

	prep.plans, prep.clusterMode = plans, clusterMode

	for _, plan := range plans {
		for n := range len(plan.replicas) + 1 {
			id := memberClusterID(opts.ID, plan.id, n+1, clusterMode)
			if _, taken := b.clustersStore(region).Get(id); taken && (n != 0 || id != prep.adoptID) {
				return prep, fmt.Errorf("%w: member cluster %s", ErrClusterAlreadyExists, id)
			}
		}
	}

	return prep, nil
}

// checkGroupReferencesLocked verifies every named dependency of a create request exists.
func (b *InMemoryBackend) checkGroupReferencesLocked(region string, opts *ReplicationGroupCreateOpts) error {
	if opts.ParameterGroupName != "" {
		if _, ok := b.parameterGroupsStore(region).Get(opts.ParameterGroupName); !ok {
			return ErrParameterGroupNotFound
		}
	}

	if opts.SubnetGroupName != "" {
		if _, ok := b.subnetGroupsStoreRO(region).Get(opts.SubnetGroupName); !ok {
			return ErrSubnetGroupNotFound
		}
	}

	for _, name := range opts.CacheSecurityGroupNames {
		if _, ok := b.cacheSecurityGroupsStoreRO(region).Get(name); !ok {
			return ErrCacheSecurityGroupNotFound
		}
	}

	if opts.GlobalReplicationGroupID != "" {
		grg, ok := b.getGlobalReplicationGroup(opts.GlobalReplicationGroupID)
		if !ok || isReaped(b.now(), grg.PendingStatus, grg.AvailableAt) {
			return ErrGlobalReplicationGroupNotFound
		}
	}

	if opts.ServerlessSnapshotName != "" {
		if _, ok := b.serverlessCacheSnapshotsStoreRO(region).Get(opts.ServerlessSnapshotName); !ok {
			return fmt.Errorf("%w: %s", ErrServerlessCacheSnapshotNotFound, opts.ServerlessSnapshotName)
		}
	}

	if opts.SnapshotName != "" {
		snap, ok := b.snapshotsStore(region).Get(opts.SnapshotName)
		if !ok || isReaped(b.now(), snap.PendingStatus, snap.AvailableAt) {
			return ErrSnapshotNotFound
		}

		// Restoring from a snapshot inherits its engine/version/node type
		// for any field the caller didn't explicitly override.
		opts.Engine = firstNonEmpty(opts.Engine, snap.Engine)
		opts.EngineVersion = firstNonEmpty(opts.EngineVersion, snap.EngineVersion)
		opts.CacheNodeType = firstNonEmpty(opts.CacheNodeType, snap.NodeType)
		restoreSnapshotTopology(opts, snap)
	}

	return nil
}

// restoreSnapshotTopology gives a group restored from a snapshot the
// snapshot's node groups unless the request names its own layout.
func restoreSnapshotTopology(opts *ReplicationGroupCreateOpts, snap *CacheSnapshot) {
	explicit := opts.NumNodeGroups > 0 || opts.NumCacheClusters > 0 || len(opts.PreferredCacheClusterAZs) > 0 ||
		len(opts.NodeGroupConfiguration) > 0 || opts.HasReplicasPerNodeGroup || opts.ReplicasPerNodeGroup != 0 ||
		opts.PrimaryClusterID != ""
	if explicit || len(snap.NodeGroups) == 0 {
		return
	}

	for _, ng := range snap.NodeGroups {
		rc := count32(len(ng.Replicas))
		cfg := NodeGroupConfig{NodeGroupID: ng.NodeGroupID, Slots: ng.Slots, ReplicaCount: &rc}

		if ng.PrimaryNode != nil {
			cfg.PrimaryAZ = ng.PrimaryNode.PreferredAvailabilityZone
		}

		for _, r := range ng.Replicas {
			cfg.ReplicaAZs = append(cfg.ReplicaAZs, r.PreferredAvailabilityZone)
		}

		opts.NodeGroupConfiguration = append(opts.NodeGroupConfiguration, cfg)
	}

	if snap.NodeGroups[0].Slots != "" {
		opts.ClusterModeEnabled = true
	}
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}

	return b
}

// adoptPrimaryLocked validates CreateReplicationGroup's PrimaryClusterId and
// copies the cluster's engine settings into opts.
func (b *InMemoryBackend) adoptPrimaryLocked(
	region string,
	opts *ReplicationGroupCreateOpts,
	prep *preparedGroup,
) error {
	if opts.PrimaryClusterID == "" {
		return nil
	}

	c, ok := b.clustersStore(region).Get(opts.PrimaryClusterID)
	if !ok || isReaped(b.now(), c.PendingStatus, c.AvailableAt) {
		return ErrClusterNotFound
	}

	if err := b.requireAvailableLocked(c.Status, c.PendingStatus, c.AvailableAt, ErrClusterNotAvailable); err != nil {
		return err
	}

	if c.Engine == engineMemcached {
		return fmt.Errorf(
			"%w: PrimaryClusterId must name a Valkey or Redis OSS cluster",
			ErrInvalidParameterCombination,
		)
	}

	if c.ReplicationGroupID != "" {
		return fmt.Errorf("%w: cluster %s already belongs to a replication group", ErrClusterNotAvailable, c.ClusterID)
	}

	if opts.ClusterModeEnabled || opts.NumNodeGroups > 1 || len(opts.NodeGroupConfiguration) > 1 {
		return fmt.Errorf("%w: PrimaryClusterId requires a single node group", ErrInvalidParameterCombination)
	}

	opts.Engine = firstNonEmpty(opts.Engine, c.Engine)
	opts.EngineVersion = firstNonEmpty(opts.EngineVersion, c.EngineVersion)
	opts.CacheNodeType = firstNonEmpty(opts.CacheNodeType, c.NodeType)
	prep.adoptID = c.ClusterID

	return nil
}

// insertGroupLocked stores the replication group and materialises its topology.
func (b *InMemoryBackend) insertGroupLocked(
	region string, opts ReplicationGroupCreateOpts, prep preparedGroup, engines []*clusterEngine,
) (*ReplicationGroup, error) {
	rgStore := b.replicationGroupsStore(region)
	if _, exists := rgStore.Get(opts.ID); exists {
		return nil, ErrReplicationGroupAlreadyExists
	}

	var adopted *Cluster

	if prep.adoptID != "" {
		c, ok := b.clustersStore(region).Get(prep.adoptID)
		if !ok || c.ReplicationGroupID != "" {
			return nil, ErrClusterNotAvailable
		}

		adopted = c
	}

	rg := b.buildReplicationGroupFromCreateOpts(region, opts)
	rg.ClusterModeEnabled = prep.clusterMode && opts.ClusterMode != statusDisabled
	b.buildTopologyLocked(
		rg,
		memberTemplate{opts: opts, region: region},
		prep.plans,
		prep.clusterMode,
		engines,
		adopted,
	)
	b.linkGlobalGroupLocked(rg, region, opts.GlobalReplicationGroupID)
	b.markCreatingLocked(&rg.PendingStatus, &rg.AvailableAt)
	rgStore.Put(rg)
	b.appendEventLocked(opts.ID, "replication-group", "replication group created")

	return rg, nil
}

// buildReplicationGroupFromCreateOpts assembles the ReplicationGroup from opts.
func (b *InMemoryBackend) buildReplicationGroupFromCreateOpts(
	region string,
	opts ReplicationGroupCreateOpts,
) *ReplicationGroup {
	rg := &ReplicationGroup{
		ReplicationGroupID:         opts.ID,
		Description:                opts.Description,
		Status:                     statusAvailable,
		ARN:                        b.replicationGroupARN(region, opts.ID),
		Tags:                       tags.New("elasticache.rg." + opts.ID + ".tags"),
		CreatedAt:                  time.Now(),
		CacheParameterGroupName:    opts.ParameterGroupName,
		PreferredMaintenanceWindow: opts.MaintenanceWindow,
		SnapshotWindow:             opts.SnapshotWindow,
		ClusterModeEnabled:         opts.ClusterModeEnabled,
		AtRestEncryptionEnabled:    opts.AtRestEncryptionEnabled,
		TransitEncryptionEnabled:   opts.TransitEncryptionEnabled,
		TransitEncryptionMode:      opts.TransitEncryptionMode,
		KmsKeyID:                   opts.KmsKeyID,
		NotificationTopicArn:       opts.NotificationTopicArn,
		DataTieringEnabled:         opts.DataTieringEnabled,
		MultiAZEnabled:             opts.MultiAZEnabled,
		SnapshotRetentionLimit:     opts.SnapshotRetentionLimit,
		LogDeliveryConfigurations:  opts.LogDeliveryConfigurations,
		Durability:                 opts.Durability,
		AutoMinorVersionUpgrade:    opts.AutoMinorVersionUpgrade,
		NetworkType:                opts.NetworkType,
		IPDiscovery:                opts.IPDiscovery,
		ClusterMode:                opts.ClusterMode,
	}

	if opts.ClusterMode != "" {
		rg.ClusterModeEnabled = opts.ClusterMode == clusterModeEnabled
	}

	applyAuthToken(rg, opts.AuthToken, opts.AuthTokenEnabled)

	if opts.AutomaticFailoverEnabled {
		rg.AutomaticFailover = statusEnabled
	}

	rg.Engine = opts.Engine
	rg.EngineVersion = opts.EngineVersion
	rg.CacheNodeType = opts.CacheNodeType

	if len(opts.UserGroupIDs) > 0 {
		rg.UserGroupIDs = opts.UserGroupIDs
	}

	for k, v := range opts.Tags {
		rg.Tags.Set(k, v)
	}

	return rg
}

// applyAuthToken sets auth-token fields on a replication group.
func applyAuthToken(rg *ReplicationGroup, token string, enabled bool) {
	if enabled {
		rg.AuthTokenEnabled = true
		if token == "" {
			token = generateAuthToken()
		}
		rg.AuthToken = token
		now := time.Now()
		rg.AuthTokenLastModifiedDate = &now
	}
}

// ----------------------------------------
// ModifyReplicationGroupFull (gap #7 ApplyImmediately, gap #4 auth token rotation)
// ----------------------------------------

// ModifyReplicationGroupFull modifies a replication group with the full set of options.
func (b *InMemoryBackend) ModifyReplicationGroupFull(
	ctx context.Context,
	id string,
	opts ReplicationGroupModifyOpts,
) (*ReplicationGroup, error) {
	b.mu.Lock("ModifyReplicationGroupFull")
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

	if opts.ParameterGroupName != "" {
		if _, ok := b.parameterGroupsStore(region).Get(opts.ParameterGroupName); !ok {
			return nil, ErrParameterGroupNotFound
		}
	}

	if len(opts.CacheSecurityGroupNames) > 0 {
		sgStore := b.cacheSecurityGroupsStoreRO(region)
		for _, sgName := range opts.CacheSecurityGroupNames {
			if _, ok := sgStore.Get(sgName); !ok {
				return nil, ErrCacheSecurityGroupNotFound
			}
		}
	}

	if err := validateTransitEncryptionModify(rg, opts); err != nil {
		return nil, err
	}

	if err := b.promotePrimaryLocked(region, rg, opts); err != nil {
		return nil, err
	}

	b.applyModifyOptsLocked(rg, opts)
	b.syncMembersLocked(region, rg, opts)
	b.markTransitionLocked(&rg.PendingStatus, &rg.AvailableAt, statusModifying)
	b.appendEventLocked(id, "replication-group", "replication group modified")

	return b.replicationGroupView(rg), nil
}

// promotePrimaryLocked applies ModifyReplicationGroup's PrimaryClusterId.
func (b *InMemoryBackend) promotePrimaryLocked(
	region string,
	rg *ReplicationGroup,
	opts ReplicationGroupModifyOpts,
) error {
	if opts.PrimaryClusterID == "" {
		return nil
	}

	if _, ok := b.clustersStore(region).Get(opts.PrimaryClusterID); !ok {
		return ErrClusterNotFound
	}

	multiAZ := rg.MultiAZEnabled
	if opts.MultiAZEnabled != nil {
		multiAZ = *opts.MultiAZEnabled
	}

	if multiAZ {
		return fmt.Errorf(
			"%w: PrimaryClusterId applies to groups with Multi-AZ disabled",
			ErrInvalidParameterCombination,
		)
	}

	return promoteClusterLocked(rg, opts.PrimaryClusterID)
}

// syncMembersLocked applies the group-wide members of a modify request to the member clusters.
func (b *InMemoryBackend) syncMembersLocked(region string, rg *ReplicationGroup, opts ReplicationGroupModifyOpts) {
	for _, c := range b.clustersStore(region).All() {
		if c.ReplicationGroupID == rg.ReplicationGroupID {
			applyMemberSettings(c, opts)
		}
	}
}

func applyMemberSettings(c *Cluster, opts ReplicationGroupModifyOpts) {
	if len(opts.CacheSecurityGroupNames) > 0 {
		c.CacheSecurityGroupNames = slices.Clone(opts.CacheSecurityGroupNames)
	}

	if len(opts.SecurityGroupIDs) > 0 {
		c.SecurityGroupIDs = slices.Clone(opts.SecurityGroupIDs)
	}

	if opts.NotificationTopicArn != "" {
		c.NotificationTopicArn = opts.NotificationTopicArn
		c.NotificationTopicStatus = statusActive
	}

	if opts.NotificationTopicStatus != "" {
		c.NotificationTopicStatus = opts.NotificationTopicStatus
	}

	applyMemberEngineSettings(c, opts)
}

func applyMemberEngineSettings(c *Cluster, opts ReplicationGroupModifyOpts) {
	if opts.CacheNodeType != "" {
		c.NodeType = opts.CacheNodeType
	}

	if opts.EngineVersion != "" && opts.ApplyImmediately {
		c.EngineVersion = opts.EngineVersion
	}

	if opts.ParameterGroupName != "" {
		c.CacheParameterGroupName = opts.ParameterGroupName
	}

	if opts.MaintenanceWindow != "" {
		c.PreferredMaintenanceWindow = opts.MaintenanceWindow
	}

	if opts.SnapshotWindow != "" {
		c.SnapshotWindow = opts.SnapshotWindow
	}
}

// applyModifyOptsLocked applies modification options to an existing replication group.
func (b *InMemoryBackend) applyModifyOptsLocked(rg *ReplicationGroup, opts ReplicationGroupModifyOpts) {
	if opts.Description != "" {
		rg.Description = opts.Description
	}

	if opts.ParameterGroupName != "" {
		rg.CacheParameterGroupName = opts.ParameterGroupName
	}

	if opts.EngineVersion != "" {
		rg.EngineVersion = opts.EngineVersion
	}

	if opts.CacheNodeType != "" {
		rg.CacheNodeType = opts.CacheNodeType
	}

	if opts.MaintenanceWindow != "" {
		rg.PreferredMaintenanceWindow = opts.MaintenanceWindow
	}

	if opts.SnapshotWindow != "" {
		rg.SnapshotWindow = opts.SnapshotWindow
	}

	if opts.NotificationTopicArn != "" {
		rg.NotificationTopicArn = opts.NotificationTopicArn
	}

	if opts.Durability != "" {
		rg.Durability = opts.Durability
	}

	if len(opts.LogDeliveryConfigurations) > 0 {
		rg.LogDeliveryConfigurations = opts.LogDeliveryConfigurations
	}

	if opts.SnapshotRetentionLimit != nil {
		rg.SnapshotRetentionLimit = *opts.SnapshotRetentionLimit
	}

	applyAutoFailoverModify(rg, opts.AutomaticFailoverEnabled)

	if opts.MultiAZEnabled != nil {
		rg.MultiAZEnabled = *opts.MultiAZEnabled
	}

	if opts.ReplicaCount != nil {
		rg.ReplicaCount = *opts.ReplicaCount
	}

	applyModifyExtras(rg, opts)

	applyUserGroupIDsModify(rg, opts.UserGroupIDsToAdd, opts.UserGroupIDsToRemove)
	applyAuthTokenModify(rg, opts.AuthToken, opts.AuthTokenUpdateStrategy)
	applyTransitEncryptionModify(rg, opts.TransitEncryptionMode)
	applyPendingChanges(rg, opts)
}

const clusterModeEnabled = "enabled"

// applyModifyExtras applies the network, cluster-mode, snapshotting-cluster and user-group-removal members.
func applyModifyExtras(rg *ReplicationGroup, opts ReplicationGroupModifyOpts) {
	if opts.AutoMinorVersionUpgrade != nil {
		rg.AutoMinorVersionUpgrade = opts.AutoMinorVersionUpgrade
	}

	if opts.IPDiscovery != "" {
		rg.IPDiscovery = opts.IPDiscovery
	}

	if opts.ClusterMode != "" {
		rg.ClusterMode = opts.ClusterMode
		rg.ClusterModeEnabled = opts.ClusterMode == clusterModeEnabled
	}

	if opts.SnapshottingClusterID != "" {
		rg.SnapshottingClusterID = opts.SnapshottingClusterID
	}

	if opts.RemoveUserGroups {
		rg.UserGroupIDs = nil
	}
}

func applyAutoFailoverModify(rg *ReplicationGroup, enabled *bool) {
	if enabled == nil {
		return
	}

	if *enabled {
		rg.AutomaticFailover = statusEnabled
	} else {
		rg.AutomaticFailover = statusDisabled
	}
}

// applyUserGroupIDsModify adds/removes user group IDs on a replication group (gap #15).
func applyUserGroupIDsModify(rg *ReplicationGroup, toAdd, toRemove []string) {
	if len(toAdd) == 0 && len(toRemove) == 0 {
		return
	}

	removeSet := make(map[string]bool, len(toRemove))
	for _, id := range toRemove {
		removeSet[id] = true
	}

	filtered := rg.UserGroupIDs[:0:0]
	for _, id := range rg.UserGroupIDs {
		if !removeSet[id] {
			filtered = append(filtered, id)
		}
	}

	addSet := make(map[string]bool)
	for _, id := range filtered {
		addSet[id] = true
	}

	for _, id := range toAdd {
		if !addSet[id] {
			filtered = append(filtered, id)
		}
	}

	rg.UserGroupIDs = filtered
}

// applyAuthTokenModify handles auth token rotation strategies (gap #4).
func applyAuthTokenModify(rg *ReplicationGroup, token, strategy string) {
	if token == "" && strategy == "" {
		return
	}

	switch strategy {
	case authTokenUpdateStrategyDelete:
		rg.AuthToken = ""
		rg.AuthTokenEnabled = false
		rg.AuthTokenLastModifiedDate = nil
	case authTokenUpdateStrategySet:
		if token == "" {
			token = generateAuthToken()
		}
		rg.AuthToken = token
		rg.AuthTokenEnabled = true
		now := time.Now()
		rg.AuthTokenLastModifiedDate = &now
	case authTokenUpdateStrategyRotate:
		rg.AuthToken = generateAuthToken()
		now := time.Now()
		rg.AuthTokenLastModifiedDate = &now
	}
}

// validateTransitEncryptionModify rejects switching TransitEncryptionMode to
// "required" when the replication group will end up without an auth token,
// matching AWS's rule that required-mode transit encryption needs an auth
// token enabled (either already present or set in the same request).
func validateTransitEncryptionModify(rg *ReplicationGroup, opts ReplicationGroupModifyOpts) error {
	if opts.TransitEncryptionMode != transitEncryptionModeRequired {
		return nil
	}

	authTokenWillBeEnabled := rg.AuthTokenEnabled

	switch opts.AuthTokenUpdateStrategy {
	case authTokenUpdateStrategySet, authTokenUpdateStrategyRotate:
		authTokenWillBeEnabled = true
	case authTokenUpdateStrategyDelete:
		authTokenWillBeEnabled = false
	}

	if opts.AuthToken != "" {
		authTokenWillBeEnabled = true
	}

	if !authTokenWillBeEnabled {
		return ErrTransitEncryptionModeInvalid
	}

	return nil
}

// applyTransitEncryptionModify applies transit encryption mode change (gap #13).
func applyTransitEncryptionModify(rg *ReplicationGroup, mode string) {
	if mode == "" {
		return
	}

	rg.TransitEncryptionMode = mode

	if mode == transitEncryptionModeRequired || mode == transitEncryptionModePreferred {
		rg.TransitEncryptionEnabled = true
	}
}

// applyPendingChanges records pending modifications when ApplyImmediately is false (gap #7).
func applyPendingChanges(rg *ReplicationGroup, opts ReplicationGroupModifyOpts) {
	if opts.ApplyImmediately {
		rg.PendingModifiedValues = nil

		return
	}

	if opts.CacheNodeType == "" && opts.EngineVersion == "" &&
		opts.AutomaticFailoverEnabled == nil && opts.ReplicaCount == nil {
		return
	}

	pending := &RGPendingModifiedValues{}

	if opts.CacheNodeType != "" {
		pending.CacheNodeType = opts.CacheNodeType
	}

	if opts.EngineVersion != "" {
		pending.EngineVersion = opts.EngineVersion
	}

	if opts.AutomaticFailoverEnabled != nil {
		if *opts.AutomaticFailoverEnabled {
			pending.AutomaticFailoverStatus = statusEnabled
		} else {
			pending.AutomaticFailoverStatus = statusDisabled
		}
	}

	if opts.ReplicaCount != nil {
		rc := *opts.ReplicaCount
		pending.ReplicaCount = &rc
	}

	rg.PendingModifiedValues = pending
}

// ----------------------------------------
// TriggerAutoSnapshot (gap #14)
// ----------------------------------------

// TriggerAutoSnapshot creates an automated snapshot for the given replication group.
func (b *InMemoryBackend) TriggerAutoSnapshot(ctx context.Context, replicationGroupID string) (*CacheSnapshot, error) {
	b.mu.Lock("TriggerAutoSnapshot")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	rg, ok := b.replicationGroupsStore(region).Get(replicationGroupID)
	if !ok {
		return nil, ErrReplicationGroupNotFound
	}

	snapStore := b.snapshotsStore(region)
	snapName := buildAutoSnapshotName(replicationGroupID)
	if _, exists := snapStore.Get(snapName); exists {
		return nil, ErrSnapshotAlreadyExists
	}

	snap := buildAutoSnapshot(b, region, snapName, rg)
	snapStore.Put(snap)

	b.appendEventLocked(replicationGroupID, "replication-group", "automated snapshot created: "+snapName)
	pruneExpiredSnapshots(b, snapStore, replicationGroupID, rg.SnapshotRetentionLimit)

	result := *snap

	return &result, nil
}

// buildAutoSnapshotName generates a daily automated snapshot name.
func buildAutoSnapshotName(replicationGroupID string) string {
	return replicationGroupID + "-auto-" + time.Now().UTC().Format("2006-01-02")
}

// buildAutoSnapshot constructs the snapshot object.
func buildAutoSnapshot(b *InMemoryBackend, region, snapName string, rg *ReplicationGroup) *CacheSnapshot {
	ev := rg.EngineVersion
	if ev == "" {
		ev = defaultEngineVersion(engineRedis)
	}

	snap := &CacheSnapshot{
		SnapshotName:       snapName,
		ReplicationGroupID: rg.ReplicationGroupID,
		Status:             statusAvailable,
		ARN:                b.snapshotARN(region, snapName),
		SnapshotSource:     snapshotSourceAutomated,
		Engine:             engineRedis,
		EngineVersion:      ev,
		NodeType:           rg.CacheNodeType,
		CreatedAt:          time.Now(),
		Tags:               tags.New("elasticache.snapshot." + snapName + ".tags"),
	}
	snapshotGroupState(snap, rg)

	return snap
}

// sortAutoSnapshots sorts snapshots by CreatedAt ascending (oldest first).
func sortAutoSnapshots(snaps []CacheSnapshot) {
	n := len(snaps)
	for i := range n - 1 {
		for j := i + 1; j < n; j++ {
			if snaps[i].CreatedAt.After(snaps[j].CreatedAt) {
				snaps[i], snaps[j] = snaps[j], snaps[i]
			}
		}
	}
}

// pruneExpiredSnapshots removes automated snapshots beyond the retention limit (gap #14).
func pruneExpiredSnapshots(
	_ *InMemoryBackend,
	tbl *store.Table[CacheSnapshot],
	replicationGroupID string,
	retentionLimit int,
) {
	if retentionLimit <= 0 {
		return
	}

	var autoSnaps []CacheSnapshot
	for _, s := range tbl.All() {
		if s.ReplicationGroupID == replicationGroupID && s.SnapshotSource == "automated" {
			autoSnaps = append(autoSnaps, *s)
		}
	}

	if len(autoSnaps) <= retentionLimit {
		return
	}

	// Sort oldest first.
	sortAutoSnapshots(autoSnaps)

	excess := len(autoSnaps) - retentionLimit
	for i := range excess {
		snap := autoSnaps[i]
		if s, ok := tbl.Get(snap.SnapshotName); ok {
			s.Tags.Close()
			tbl.Delete(snap.SnapshotName)
		}
	}
}

// StartMigration starts a migration for a replication group.
// CustomerNodeEndpointList is required on the wire (aws-sdk-go-v2's
// StartMigrationInput); this backend has nowhere real to echo the endpoints
// back (ReplicationGroup's response shape never carries them), so it acts on
// the field by enforcing AWS's required-member contract instead.
func (b *InMemoryBackend) StartMigration(
	ctx context.Context,
	replicationGroupID string,
	customerNodeEndpoints []CustomerNodeEndpoint,
) (*ReplicationGroup, error) {
	if len(customerNodeEndpoints) == 0 {
		return nil, ErrCustomerNodeEndpointsRequired
	}

	b.mu.Lock("StartMigration")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	rg, ok := b.replicationGroupsStore(region).Get(replicationGroupID)
	if !ok {
		return nil, ErrReplicationGroupNotFound
	}

	rg.Status = "migrating"
	result := *rg

	return &result, nil
}

// TestMigration tests a migration for a replication group. See StartMigration's
// doc comment for why CustomerNodeEndpointList is validated but not persisted.
func (b *InMemoryBackend) TestMigration(
	ctx context.Context,
	replicationGroupID string,
	customerNodeEndpoints []CustomerNodeEndpoint,
) (*ReplicationGroup, error) {
	if len(customerNodeEndpoints) == 0 {
		return nil, ErrCustomerNodeEndpointsRequired
	}

	b.mu.Lock("TestMigration")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	rg, ok := b.replicationGroupsStore(region).Get(replicationGroupID)
	if !ok {
		return nil, ErrReplicationGroupNotFound
	}

	result := *rg

	return &result, nil
}

// CompleteMigration completes an online data migration from an external Redis server to this replication group.
func (b *InMemoryBackend) CompleteMigration(
	ctx context.Context,
	replicationGroupID string,
	_ bool,
) (*ReplicationGroup, error) {
	b.mu.Lock("CompleteMigration")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	rg, ok := b.replicationGroupsStore(region).Get(replicationGroupID)
	if !ok {
		return nil, ErrReplicationGroupNotFound
	}

	rg.Status = statusAvailable
	result := *rg

	return &result, nil
}

// ----------------------------------------
// Seed helpers (for test isolation)
// ----------------------------------------
