package elasticache

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	engineRedisCap = "Redis"
	allUpfrontPlan = "All Upfront"
)

var (
	ErrUserGroupNotFound                  = errors.New("UserGroupNotFound")
	ErrUserGroupAlreadyExists             = errors.New("UserGroupAlreadyExistsFault")
	ErrReservedCacheNodeNotFound          = errors.New("ReservedCacheNodeNotFound")
	ErrReservedCacheNodeAlreadyExists     = errors.New("ReservedCacheNodeAlreadyExists")
	ErrReservedCacheNodesOfferingNotFound = errors.New("ReservedCacheNodesOfferingNotFound")
)

// UserGroup represents an ElastiCache user group.
type UserGroup struct {
	CreatedAt                   time.Time  `json:"createdAt"`
	Tags                        *tags.Tags `json:"tags,omitempty"`
	UserGroupID                 string     `json:"userGroupID"`
	Description                 string     `json:"description"`
	Status                      string     `json:"status"`
	ARN                         string     `json:"arn"`
	Engine                      string     `json:"engine"`
	UserIDs                     []string   `json:"userIDs,omitempty"`
	AssignedReplicationGroupIDs []string   `json:"assignedReplicationGroupIDs,omitempty"`
}

// ReservedCacheNode represents a purchased reserved cache node.
type ReservedCacheNode struct {
	StartTime           time.Time `json:"startTime"`
	ReservationID       string    `json:"reservationID,omitempty"`
	ReservedCacheNodeID string    `json:"reservedCacheNodeID"`
	ARN                 string    `json:"arn"`
	CacheNodeType       string    `json:"cacheNodeType"`
	OfferingType        string    `json:"offeringType"`
	ProductDescription  string    `json:"productDescription"`
	State               string    `json:"state"`
	OfferingID          string    `json:"offeringID"`
	FixedPrice          float64   `json:"fixedPrice"`
	UsagePrice          float64   `json:"usagePrice"`
	Duration            int32     `json:"duration"`
	CacheNodeCount      int32     `json:"cacheNodeCount"`
}

// ReservedCacheNodesOffering represents a reserved node offering.
type ReservedCacheNodesOffering struct {
	OfferingID         string
	CacheNodeType      string
	ProductDescription string
	OfferingType       string
	FixedPrice         float64
	UsagePrice         float64
	Duration           int32
}

// CacheEngineVersion represents an engine version.
type CacheEngineVersion struct {
	Engine                        string
	EngineVersion                 string
	CacheParameterGroupFamily     string
	CacheEngineDescription        string
	CacheEngineVersionDescription string
}

// ServiceUpdate represents a service update.
type ServiceUpdate struct {
	ServiceUpdateName string
	Status            string
}

// UpdateAction represents an update action.
type UpdateAction struct {
	ReplicationGroupID string
	CacheClusterID     string
	ServiceUpdateName  string
	UpdateActionStatus string
}

func (b *InMemoryBackend) userGroupARN(region, id string) string {
	return arn.Build("elasticache", region, b.accountID, "usergroup:"+id)
}

func (b *InMemoryBackend) reservedCacheNodeARN(region, id string) string {
	return arn.Build("elasticache", region, b.accountID, "reserved-instance:"+id)
}

// reservedOneYearSeconds is the duration in seconds for a 1-year reserved cache node.
const reservedOneYearSeconds int32 = 365 * 24 * 60 * 60

// Builtin offering prices (USD, fixed price, all upfront, 1 year).
const (
	reservedPriceLarge  float64 = 500
	reservedPriceXLarge float64 = 1000
	reservedPriceMicro  float64 = 50
)

func builtinReservedOfferings() []ReservedCacheNodesOffering {
	return []ReservedCacheNodesOffering{
		{
			OfferingID:         "31153cd5-4ce6-45a9-b6ce-7f0b6789b8fa",
			CacheNodeType:      "cache.r6g.large",
			Duration:           reservedOneYearSeconds,
			FixedPrice:         reservedPriceLarge,
			UsagePrice:         0.0,
			ProductDescription: engineRedisCap,
			OfferingType:       allUpfrontPlan,
		},
		{
			OfferingID:         "649fd0c8-cf6d-47a0-bfa6-060f8e75e95f",
			CacheNodeType:      "cache.r6g.xlarge",
			Duration:           reservedOneYearSeconds,
			FixedPrice:         reservedPriceXLarge,
			UsagePrice:         0.0,
			ProductDescription: engineRedisCap,
			OfferingType:       allUpfrontPlan,
		},
		{
			OfferingID:         "a2b54b70-d5b3-4b96-a72e-afedbc16e70f",
			CacheNodeType:      nodeTypeT3Micro,
			Duration:           reservedOneYearSeconds,
			FixedPrice:         reservedPriceMicro,
			UsagePrice:         0.0,
			ProductDescription: engineRedisCap,
			OfferingType:       allUpfrontPlan,
		},
	}
}

func builtinCacheEngineVersions() []CacheEngineVersion {
	return []CacheEngineVersion{
		{
			Engine:                        engineRedis,
			EngineVersion:                 versionRedis710,
			CacheParameterGroupFamily:     familyRedis7,
			CacheEngineDescription:        engineRedisCap,
			CacheEngineVersionDescription: "Redis 7.1.0",
		},
		{
			Engine:                        engineRedis,
			EngineVersion:                 "7.0.7",
			CacheParameterGroupFamily:     familyRedis7,
			CacheEngineDescription:        engineRedisCap,
			CacheEngineVersionDescription: "Redis 7.0.7",
		},
		{
			Engine:                        engineRedis,
			EngineVersion:                 "6.2.6",
			CacheParameterGroupFamily:     "redis6.x",
			CacheEngineDescription:        engineRedisCap,
			CacheEngineVersionDescription: "Redis 6.2.6",
		},
		{
			Engine:                        engineRedis,
			EngineVersion:                 "5.0.6",
			CacheParameterGroupFamily:     "redis5.0",
			CacheEngineDescription:        engineRedisCap,
			CacheEngineVersionDescription: "Redis 5.0.6",
		},
		{
			Engine:                        engineMemcached,
			EngineVersion:                 "1.6.17",
			CacheParameterGroupFamily:     "memcached1.6",
			CacheEngineDescription:        "Memcached",
			CacheEngineVersionDescription: "Memcached 1.6.17",
		},
		{
			Engine:                        engineMemcached,
			EngineVersion:                 "1.5.16",
			CacheParameterGroupFamily:     "memcached1.5",
			CacheEngineDescription:        "Memcached",
			CacheEngineVersionDescription: "Memcached 1.5.16",
		},
		{
			Engine:                        engineValkey,
			EngineVersion:                 versionValkey82,
			CacheParameterGroupFamily:     familyValkey8,
			CacheEngineDescription:        engineValkeyCap,
			CacheEngineVersionDescription: "Valkey 8.2.0",
		},
		{
			Engine:                        engineValkey,
			EngineVersion:                 "8.0.1",
			CacheParameterGroupFamily:     familyValkey8,
			CacheEngineDescription:        engineValkeyCap,
			CacheEngineVersionDescription: "Valkey 8.0.1",
		},
		{
			Engine:                        engineValkey,
			EngineVersion:                 "7.2.7",
			CacheParameterGroupFamily:     familyValkey7,
			CacheEngineDescription:        engineValkeyCap,
			CacheEngineVersionDescription: "Valkey 7.2.7",
		},
	}
}

// DeleteUser deletes a user by ID.
func (b *InMemoryBackend) DeleteUser(ctx context.Context, userID string) (*User, error) {
	b.mu.Lock("DeleteUser")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tbl := b.usersStore(region)
	u, ok := tbl.Get(userID)
	if !ok {
		return nil, ErrUserNotFound
	}

	for _, ug := range b.userGroupsStore(region).All() {
		if slices.Contains(ug.UserIDs, userID) {
			return nil, fmt.Errorf("user %q belongs to group %q: %w", userID, ug.UserGroupID, ErrUserNotInGroup)
		}
	}

	result := *u
	tbl.Delete(userID)
	b.appendEventLocked(userID, "user", "user deleted")

	return &result, nil
}

// DescribeUsers returns a paginated list of users, optionally filtered by userID.
func (b *InMemoryBackend) DescribeUsers(
	ctx context.Context,
	userID, marker string,
	maxRecords int,
) (page.Page[User], error) {
	b.mu.RLock("DescribeUsers")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return describePaged(b.usersStore(region), userID, ErrUserNotFound, nil,
		func(u User) string { return u.UserID }, marker, maxRecords)
}

// ModifyUser modifies a user's access string and/or password settings.
func (b *InMemoryBackend) ModifyUser(
	ctx context.Context,
	userID, accessString string,
	noPasswordRequired bool,
) (*User, error) {
	b.mu.Lock("ModifyUser")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	u, ok := b.usersStore(region).Get(userID)
	if !ok {
		return nil, ErrUserNotFound
	}

	if accessString != "" {
		u.AccessString = accessString
	}

	u.NoPasswordRequired = noPasswordRequired
	result := *u

	return &result, nil
}

// CreateUserGroup creates a new user group.
func (b *InMemoryBackend) CreateUserGroup(
	ctx context.Context,
	groupID, description, engine string,
	userIDs []string,
) (*UserGroup, error) {
	b.mu.Lock("CreateUserGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tbl := b.userGroupsStore(region)
	if _, exists := tbl.Get(groupID); exists {
		return nil, ErrUserGroupAlreadyExists
	}

	if engine == "" {
		engine = engineRedis
	}

	ug := &UserGroup{
		UserGroupID: groupID,
		Description: description,
		Status:      statusActive,
		ARN:         b.userGroupARN(region, groupID),
		Engine:      engine,
		UserIDs:     userIDs,
		CreatedAt:   time.Now(),
		Tags:        tags.New("elasticache.usergroup." + groupID + ".tags"),
	}
	tbl.Put(ug)
	b.appendEventLocked(groupID, "user-group", "user group created")

	return ug, nil
}

// DeleteUserGroup deletes a user group by ID.
func (b *InMemoryBackend) DeleteUserGroup(ctx context.Context, groupID string) (*UserGroup, error) {
	b.mu.Lock("DeleteUserGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tbl := b.userGroupsStore(region)
	ug, ok := tbl.Get(groupID)
	if !ok {
		return nil, ErrUserGroupNotFound
	}

	result := *ug
	tbl.Delete(groupID)
	b.appendEventLocked(groupID, "user-group", "user group deleted")

	return &result, nil
}

// DescribeUserGroups returns a paginated list of user groups, optionally filtered by groupID.
func (b *InMemoryBackend) DescribeUserGroups(
	ctx context.Context,
	groupID, marker string,
	maxRecords int,
) (page.Page[UserGroup], error) {
	b.mu.RLock("DescribeUserGroups")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return describePaged(b.userGroupsStore(region), groupID, ErrUserGroupNotFound, nil,
		func(ug UserGroup) string { return ug.UserGroupID }, marker, maxRecords)
}

// ModifyUserGroup adds or removes users from a user group.
func (b *InMemoryBackend) ModifyUserGroup(
	ctx context.Context,
	groupID string,
	userIDsToAdd, userIDsToRemove []string,
) (*UserGroup, error) {
	b.mu.Lock("ModifyUserGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	ug, ok := b.userGroupsStore(region).Get(groupID)
	if !ok {
		return nil, ErrUserGroupNotFound
	}

	removeSet := make(map[string]bool, len(userIDsToRemove))
	for _, id := range userIDsToRemove {
		removeSet[id] = true
	}

	filtered := make([]string, 0, len(ug.UserIDs))
	for _, id := range ug.UserIDs {
		if !removeSet[id] {
			filtered = append(filtered, id)
		}
	}

	filtered = append(filtered, userIDsToAdd...)
	ug.UserIDs = filtered
	result := *ug

	return &result, nil
}

// DeleteGlobalReplicationGroup deletes a global replication group.
func (b *InMemoryBackend) DeleteGlobalReplicationGroup(
	_ context.Context,
	id string,
	_ bool,
) (*GlobalReplicationGroup, error) {
	b.mu.Lock("DeleteGlobalReplicationGroup")
	defer b.mu.Unlock()

	grg, ok := b.getGlobalReplicationGroup(id)

	if !ok || isReaped(b.now(), grg.PendingStatus, grg.AvailableAt) {
		return nil, ErrGlobalReplicationGroupNotFound
	}

	if d := b.pendingUntil(); !d.IsZero() {
		grg.PendingStatus = statusDeleting
		grg.AvailableAt = d
		b.appendEventLocked(id, "global-replication-group", "global replication group deleting")

		return b.globalReplicationGroupView(grg), nil
	}

	result := *grg
	grg.Tags.Close()
	b.deleteGlobalReplicationGroup(id)

	b.appendEventLocked(id, "global-replication-group", "global replication group deleted")

	return &result, nil
}

// DescribeGlobalReplicationGroups returns a paginated list of global replication groups.
func (b *InMemoryBackend) DescribeGlobalReplicationGroups(
	_ context.Context,
	id, marker string,
	maxRecords int,
) (page.Page[GlobalReplicationGroup], error) {
	b.mu.RLock("DescribeGlobalReplicationGroups")
	defer b.mu.RUnlock()

	now := b.now()
	if id != "" {
		grg, ok := b.getGlobalReplicationGroup(id)

		if !ok || isReaped(now, grg.PendingStatus, grg.AvailableAt) {
			return page.Page[GlobalReplicationGroup]{}, ErrGlobalReplicationGroupNotFound
		}

		view := *b.globalReplicationGroupView(grg)

		return page.Page[GlobalReplicationGroup]{Data: []GlobalReplicationGroup{view}}, nil
	}

	grgs := b.listGlobalReplicationGroups()
	out := make([]GlobalReplicationGroup, 0, len(grgs))
	for _, grg := range grgs {
		if isReaped(now, grg.PendingStatus, grg.AvailableAt) {
			continue
		}
		out = append(out, *b.globalReplicationGroupView(grg))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].GlobalReplicationGroupID < out[j].GlobalReplicationGroupID
	})

	return page.New(out, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// DisassociateGlobalReplicationGroup removes a secondary replication group from a global replication group.
func (b *InMemoryBackend) DisassociateGlobalReplicationGroup(
	_ context.Context,
	id, _, _ string,
) (*GlobalReplicationGroup, error) {
	b.mu.Lock("DisassociateGlobalReplicationGroup")
	defer b.mu.Unlock()

	grg, ok := b.getGlobalReplicationGroup(id)

	if !ok {
		return nil, ErrGlobalReplicationGroupNotFound
	}

	result := *grg

	return &result, nil
}

// FailoverGlobalReplicationGroup promotes a secondary region to primary.
func (b *InMemoryBackend) FailoverGlobalReplicationGroup(
	_ context.Context,
	id, _, _ string,
) (*GlobalReplicationGroup, error) {
	b.mu.Lock("FailoverGlobalReplicationGroup")
	defer b.mu.Unlock()

	grg, ok := b.getGlobalReplicationGroup(id)

	if !ok {
		return nil, ErrGlobalReplicationGroupNotFound
	}

	result := *grg

	return &result, nil
}

// IncreaseNodeGroupsInGlobalReplicationGroup increases the node group count.
func (b *InMemoryBackend) IncreaseNodeGroupsInGlobalReplicationGroup(
	_ context.Context,
	id string,
	nodeGroupCount int32,
) (*GlobalReplicationGroup, error) {
	b.mu.Lock("IncreaseNodeGroupsInGlobalReplicationGroup")
	defer b.mu.Unlock()

	grg, ok := b.getGlobalReplicationGroup(id)

	if !ok {
		return nil, ErrGlobalReplicationGroupNotFound
	}

	if nodeGroupCount > grg.NodeGroupCount {
		grg.NodeGroupCount = nodeGroupCount
	}

	result := *grg

	return &result, nil
}

// DecreaseNodeGroupsInGlobalReplicationGroup decreases the node group count.
func (b *InMemoryBackend) DecreaseNodeGroupsInGlobalReplicationGroup(
	_ context.Context,
	id string,
	nodeGroupCount int32,
) (*GlobalReplicationGroup, error) {
	b.mu.Lock("DecreaseNodeGroupsInGlobalReplicationGroup")
	defer b.mu.Unlock()

	grg, ok := b.getGlobalReplicationGroup(id)

	if !ok {
		return nil, ErrGlobalReplicationGroupNotFound
	}

	if nodeGroupCount > 0 && nodeGroupCount < grg.NodeGroupCount {
		grg.NodeGroupCount = nodeGroupCount
	}

	result := *grg

	return &result, nil
}

// ModifyGlobalReplicationGroup modifies a global replication group.
func (b *InMemoryBackend) ModifyGlobalReplicationGroup(
	_ context.Context,
	id, description, engineVersion string,
	automaticFailoverEnabled bool,
) (*GlobalReplicationGroup, error) {
	b.mu.Lock("ModifyGlobalReplicationGroup")
	defer b.mu.Unlock()

	grg, ok := b.getGlobalReplicationGroup(id)

	if !ok {
		return nil, ErrGlobalReplicationGroupNotFound
	}

	if description != "" {
		grg.Description = description
	}

	if engineVersion != "" {
		grg.EngineVersion = engineVersion
	}

	_ = automaticFailoverEnabled
	b.markTransitionLocked(&grg.PendingStatus, &grg.AvailableAt, statusModifying)

	return b.globalReplicationGroupView(grg), nil
}

// RebalanceSlotsInGlobalReplicationGroup rebalances slots.
func (b *InMemoryBackend) RebalanceSlotsInGlobalReplicationGroup(
	_ context.Context,
	id string,
) (*GlobalReplicationGroup, error) {
	b.mu.Lock("RebalanceSlotsInGlobalReplicationGroup")
	defer b.mu.Unlock()

	grg, ok := b.getGlobalReplicationGroup(id)

	if !ok {
		return nil, ErrGlobalReplicationGroupNotFound
	}

	result := *grg

	return &result, nil
}

// DescribeReservedCacheNodes returns a paginated list of reserved cache nodes.
func (b *InMemoryBackend) DescribeReservedCacheNodes(
	ctx context.Context,
	id, cacheNodeType, offeringType, marker string,
	maxRecords int,
) (page.Page[ReservedCacheNode], error) {
	b.mu.RLock("DescribeReservedCacheNodes")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return describePaged(
		b.reservedCacheNodesStore(region),
		id,
		ErrReservedCacheNodeNotFound,
		func(rcn ReservedCacheNode) bool {
			return (cacheNodeType == "" || rcn.CacheNodeType == cacheNodeType) &&
				(offeringType == "" || rcn.OfferingType == offeringType)
		},
		func(rcn ReservedCacheNode) string { return rcn.ReservedCacheNodeID },
		marker,
		maxRecords,
	)
}

// DescribeReservedCacheNodesOfferings returns a paginated list of reserved cache node offerings.
func (b *InMemoryBackend) DescribeReservedCacheNodesOfferings(
	_ context.Context,
	offeringID, cacheNodeType, offeringType, marker string,
	maxRecords int,
) (page.Page[ReservedCacheNodesOffering], error) {
	b.mu.RLock("DescribeReservedCacheNodesOfferings")
	defer b.mu.RUnlock()

	all := builtinReservedOfferings()

	if offeringID != "" {
		for _, o := range all {
			if o.OfferingID == offeringID {
				return page.Page[ReservedCacheNodesOffering]{Data: []ReservedCacheNodesOffering{o}}, nil
			}
		}

		return page.Page[ReservedCacheNodesOffering]{}, ErrReservedCacheNodesOfferingNotFound
	}

	filtered := make([]ReservedCacheNodesOffering, 0, len(all))
	for _, o := range all {
		if cacheNodeType != "" && o.CacheNodeType != cacheNodeType {
			continue
		}
		if offeringType != "" && o.OfferingType != offeringType {
			continue
		}
		filtered = append(filtered, o)
	}

	return page.New(filtered, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// PurchaseReservedCacheNodesOffering purchases a reserved cache node offering.
func (b *InMemoryBackend) PurchaseReservedCacheNodesOffering(
	ctx context.Context,
	offeringID, reservedCacheNodeID string,
	cacheNodeCount int32,
) (*ReservedCacheNode, error) {
	b.mu.Lock("PurchaseReservedCacheNodesOffering")
	defer b.mu.Unlock()

	var found *ReservedCacheNodesOffering

	offerings := builtinReservedOfferings()
	for idx := range offerings {
		if offerings[idx].OfferingID == offeringID {
			found = &offerings[idx]

			break
		}
	}

	if found == nil {
		return nil, ErrReservedCacheNodesOfferingNotFound
	}

	if cacheNodeCount <= 0 {
		cacheNodeCount = 1
	}

	if reservedCacheNodeID == "" {
		reservedCacheNodeID = fmt.Sprintf("rcn-%s-%s", offeringID[:8], randomSuffix())
	}

	region := getRegion(ctx, b.region)
	tbl := b.reservedCacheNodesStore(region)
	if _, exists := tbl.Get(reservedCacheNodeID); exists {
		return nil, fmt.Errorf("reserved cache node %q: %w", reservedCacheNodeID, ErrReservedCacheNodeAlreadyExists)
	}

	rcn := &ReservedCacheNode{
		ReservedCacheNodeID: reservedCacheNodeID,
		ARN:                 b.reservedCacheNodeARN(region, reservedCacheNodeID),
		CacheNodeType:       found.CacheNodeType,
		Duration:            found.Duration,
		FixedPrice:          found.FixedPrice,
		UsagePrice:          found.UsagePrice,
		ProductDescription:  found.ProductDescription,
		OfferingType:        found.OfferingType,
		OfferingID:          found.OfferingID,
		State:               statusActive,
		CacheNodeCount:      cacheNodeCount,
		StartTime:           time.Now(),
	}
	tbl.Put(rcn)
	b.appendEventLocked(reservedCacheNodeID, "reserved-cache-node", "reserved cache node purchased")

	return rcn, nil
}

// DeleteServerlessCache deletes a serverless cache.
func (b *InMemoryBackend) DeleteServerlessCache(ctx context.Context, name string) (*ServerlessCache, error) {
	b.mu.Lock("DeleteServerlessCache")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	b.pruneRegionLocked(region)
	tbl := b.serverlessCachesStore(region)
	sc, ok := tbl.Get(name)
	if !ok || isReaped(b.now(), sc.PendingStatus, sc.AvailableAt) {
		return nil, ErrServerlessCacheNotFound
	}

	if d := b.pendingUntil(); !d.IsZero() {
		sc.PendingStatus = statusDeleting
		sc.AvailableAt = d
		b.appendEventLocked(name, "serverless-cache", "serverless cache deleting")

		return b.serverlessCacheView(sc), nil
	}

	result := *sc
	sc.Tags.Close()
	tbl.Delete(name)
	b.appendEventLocked(name, "serverless-cache", "serverless cache deleted")

	return &result, nil
}

// DeleteServerlessCacheSnapshot deletes a serverless cache snapshot.
func (b *InMemoryBackend) DeleteServerlessCacheSnapshot(
	ctx context.Context,
	name string,
) (*ServerlessCacheSnapshot, error) {
	b.mu.Lock("DeleteServerlessCacheSnapshot")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tbl := b.serverlessCacheSnapshotsStore(region)
	snap, ok := tbl.Get(name)
	if !ok {
		return nil, ErrServerlessCacheSnapshotNotFound
	}

	result := *snap
	tbl.Delete(name)
	b.appendEventLocked(name, "serverless-cache-snapshot", "serverless cache snapshot deleted")

	return &result, nil
}

// DescribeServerlessCaches returns a paginated list of serverless caches.
func (b *InMemoryBackend) DescribeServerlessCaches(
	ctx context.Context,
	name, marker string,
	maxRecords int,
) (page.Page[ServerlessCache], error) {
	b.mu.RLock("DescribeServerlessCaches")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	p, err := describePaged(b.serverlessCachesStore(region), name, ErrServerlessCacheNotFound, nil,
		func(sc ServerlessCache) string { return sc.Name }, marker, maxRecords)

	return b.finalizeServerlessCachePage(name, p, err)
}

// DescribeServerlessCacheSnapshots returns a paginated list of serverless cache snapshots.
func (b *InMemoryBackend) DescribeServerlessCacheSnapshots(
	ctx context.Context,
	serverlessCacheName, snapshotName, marker string,
	maxRecords int,
) (page.Page[ServerlessCacheSnapshot], error) {
	b.mu.RLock("DescribeServerlessCacheSnapshots")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	tbl := b.serverlessCacheSnapshotsStore(region)

	if snapshotName != "" {
		snap, ok := tbl.Get(snapshotName)
		if !ok {
			return page.Page[ServerlessCacheSnapshot]{}, ErrServerlessCacheSnapshotNotFound
		}

		return page.Page[ServerlessCacheSnapshot]{Data: []ServerlessCacheSnapshot{*snap}}, nil
	}

	all := tbl.All()
	out := make([]ServerlessCacheSnapshot, 0, len(all))
	for _, snap := range all {
		if serverlessCacheName != "" && snap.ServerlessCacheName != serverlessCacheName {
			continue
		}

		out = append(out, *snap)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return page.New(out, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// ExportServerlessCacheSnapshot exports a serverless cache snapshot to S3.
func (b *InMemoryBackend) ExportServerlessCacheSnapshot(
	ctx context.Context,
	snapshotName, _ string,
) (*ServerlessCacheSnapshot, error) {
	b.mu.Lock("ExportServerlessCacheSnapshot")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	snap, ok := b.serverlessCacheSnapshotsStore(region).Get(snapshotName)
	if !ok {
		return nil, ErrServerlessCacheSnapshotNotFound
	}

	result := *snap

	return &result, nil
}

// ModifyServerlessCache modifies a serverless cache.
func (b *InMemoryBackend) ModifyServerlessCache(
	ctx context.Context,
	name, description string,
) (*ServerlessCache, error) {
	b.mu.Lock("ModifyServerlessCache")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	sc, ok := b.serverlessCachesStore(region).Get(name)
	if !ok {
		return nil, ErrServerlessCacheNotFound
	}

	if description != "" {
		sc.Description = description
	}

	b.markTransitionLocked(&sc.PendingStatus, &sc.AvailableAt, statusModifying)

	return b.serverlessCacheView(sc), nil
}

// StartMigration starts a migration for a replication group.
func (b *InMemoryBackend) StartMigration(ctx context.Context, replicationGroupID string) (*ReplicationGroup, error) {
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

// TestMigration tests a migration for a replication group.
func (b *InMemoryBackend) TestMigration(ctx context.Context, replicationGroupID string) (*ReplicationGroup, error) {
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

// IncreaseReplicaCount increases the replica count for a replication group.
func (b *InMemoryBackend) IncreaseReplicaCount(
	ctx context.Context,
	replicationGroupID string,
	newReplicaCount int32,
) (*ReplicationGroup, error) {
	b.mu.Lock("IncreaseReplicaCount")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	rg, ok := b.replicationGroupsStore(region).Get(replicationGroupID)
	if !ok {
		return nil, ErrReplicationGroupNotFound
	}

	if newReplicaCount > 0 {
		rg.ReplicaCount = newReplicaCount
	}

	b.appendEventLocked(replicationGroupID, "replication-group", "replica count increased")

	result := *rg

	return &result, nil
}

// DecreaseReplicaCount decreases the replica count for a replication group.
func (b *InMemoryBackend) DecreaseReplicaCount(
	ctx context.Context,
	replicationGroupID string,
	newReplicaCount int32,
) (*ReplicationGroup, error) {
	b.mu.Lock("DecreaseReplicaCount")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	rg, ok := b.replicationGroupsStore(region).Get(replicationGroupID)
	if !ok {
		return nil, ErrReplicationGroupNotFound
	}

	if newReplicaCount >= 0 {
		rg.ReplicaCount = newReplicaCount
	}

	b.appendEventLocked(replicationGroupID, "replication-group", "replica count decreased")

	result := *rg

	return &result, nil
}

// ModifyReplicationGroupShardConfiguration modifies the shard configuration of a replication group.
// Cluster mode must be enabled to use this operation.
func (b *InMemoryBackend) ModifyReplicationGroupShardConfiguration(
	ctx context.Context,
	replicationGroupID string,
	nodeGroupCount int32,
) (*ReplicationGroup, error) {
	b.mu.Lock("ModifyReplicationGroupShardConfiguration")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	rg, ok := b.replicationGroupsStore(region).Get(replicationGroupID)
	if !ok {
		return nil, ErrReplicationGroupNotFound
	}

	if !rg.ClusterModeEnabled {
		return nil, ErrClusterModeRequired
	}

	if nodeGroupCount > 0 {
		rg.NodeGroups = resizeNodeGroups(rg.NodeGroups, int(nodeGroupCount), int(rg.ReplicaCount))
	}

	b.appendEventLocked(replicationGroupID, "replication-group", "shard configuration modified")

	result := *rg

	return &result, nil
}

// DescribeCacheEngineVersions returns engine versions, optionally filtered.
func (b *InMemoryBackend) DescribeCacheEngineVersions(
	_ context.Context,
	engine, family, engineVersion, marker string,
	maxRecords int,
) (page.Page[CacheEngineVersion], error) {
	b.mu.RLock("DescribeCacheEngineVersions")
	defer b.mu.RUnlock()

	all := builtinCacheEngineVersions()
	filtered := make([]CacheEngineVersion, 0, len(all))

	for _, v := range all {
		if engine != "" && v.Engine != engine {
			continue
		}

		if family != "" && v.CacheParameterGroupFamily != family {
			continue
		}

		if engineVersion != "" && v.EngineVersion != engineVersion {
			continue
		}

		filtered = append(filtered, v)
	}

	return page.New(filtered, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// RebootCacheCluster reboots a cache cluster.
func (b *InMemoryBackend) RebootCacheCluster(
	ctx context.Context,
	clusterID string,
	nodeIDs []string,
) (*Cluster, error) {
	b.mu.Lock("RebootCacheCluster")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	c, ok := b.clustersStore(region).Get(clusterID)
	if !ok {
		return nil, ErrClusterNotFound
	}

	// Record an event for each rebooted node (or one general event if no node IDs provided).
	if len(nodeIDs) == 0 {
		b.appendEventLocked(clusterID, "cache-cluster", "cache cluster reboot started")
	} else {
		for _, nodeID := range nodeIDs {
			b.appendEventLocked(clusterID, "cache-cluster", "cache node "+nodeID+" reboot started")
		}
	}

	b.markTransitionLocked(&c.PendingStatus, &c.AvailableAt, statusRebooting)

	return b.clusterView(c), nil
}

// DeleteCacheSecurityGroup deletes a cache security group.
func (b *InMemoryBackend) DeleteCacheSecurityGroup(ctx context.Context, name string) error {
	b.mu.Lock("DeleteCacheSecurityGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	sgStore := b.cacheSecurityGroupsStore(region)
	if _, ok := sgStore.Get(name); !ok {
		return ErrCacheSecurityGroupNotFound
	}

	sgStore.Delete(name)
	delete(b.cacheSecurityGroupIngressStore(region), name)

	return nil
}

// DescribeCacheSecurityGroups returns a paginated list of cache security groups.
func (b *InMemoryBackend) DescribeCacheSecurityGroups(
	ctx context.Context,
	name, marker string,
	maxRecords int,
) (page.Page[CacheSecurityGroup], error) {
	b.mu.RLock("DescribeCacheSecurityGroups")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return describePaged(b.cacheSecurityGroupsStore(region), name, ErrCacheSecurityGroupNotFound, nil,
		func(sg CacheSecurityGroup) string { return sg.Name }, marker, maxRecords)
}

// RevokeCacheSecurityGroupIngress removes an EC2 security group authorization.
func (b *InMemoryBackend) RevokeCacheSecurityGroupIngress(
	ctx context.Context,
	name, ec2SecurityGroupName, ec2SecurityGroupOwnerID string,
) (*CacheSecurityGroup, error) {
	b.mu.Lock("RevokeCacheSecurityGroupIngress")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	sg, ok := b.cacheSecurityGroupsStore(region).Get(name)
	if !ok {
		return nil, ErrCacheSecurityGroupNotFound
	}

	ingressStore := b.cacheSecurityGroupIngressStore(region)
	ingress := ingressStore[name]
	filtered := make([]EC2SecurityGroupMembership, 0, len(ingress))

	for _, entry := range ingress {
		if entry.EC2SecurityGroupName == ec2SecurityGroupName &&
			entry.EC2SecurityGroupOwnerID == ec2SecurityGroupOwnerID {
			continue
		}

		filtered = append(filtered, entry)
	}

	ingressStore[name] = filtered
	result := *sg

	return &result, nil
}

// DescribeEngineDefaultParameters returns the default parameters for a parameter group family.
func (b *InMemoryBackend) DescribeEngineDefaultParameters(
	_ context.Context,
	cacheParameterGroupFamily string,
	marker string,
	maxRecords int,
) (page.Page[CacheParameter], error) {
	b.mu.RLock("DescribeEngineDefaultParameters")
	defer b.mu.RUnlock()

	return page.New(
		builtinEngineDefaultParameters(cacheParameterGroupFamily),
		marker,
		maxRecords,
		elasticacheDefaultMaxRecords,
	), nil
}

// builtinEngineDefaultParameters returns well-known default parameters for a given family.
func builtinEngineDefaultParameters(family string) []CacheParameter {
	switch {
	case strings.HasPrefix(family, "memcached"):
		return builtinMemcachedDefaultParameters()
	case strings.HasPrefix(family, "valkey"):
		return builtinValkeyDefaultParameters()
	default:
		return builtinRedisDefaultParameters()
	}
}

func builtinMemcachedDefaultParameters() []CacheParameter {
	return []CacheParameter{
		{
			Name:          "max_item_size",
			Value:         "1048576",
			Description:   "Maximum size of a single item",
			DataType:      dataTypeInteger,
			AllowedValues: "1-1073741824",
			IsModifiable:  true,
		},
		{
			Name:          "chunk_size",
			Value:         "48",
			Description:   "Minimum space allocated for key+value+flags",
			DataType:      dataTypeInteger,
			AllowedValues: "1-4096",
			IsModifiable:  true,
		},
		{
			Name:          "max_simultaneous_connections",
			Value:         "65000",
			Description:   "Maximum number of simultaneous connections",
			DataType:      dataTypeInteger,
			AllowedValues: "1-65000",
			IsModifiable:  true,
		},
		{
			Name:          "backlog_queue_limit",
			Value:         "1024",
			Description:   "TCP backlog queue limit",
			DataType:      dataTypeInteger,
			AllowedValues: "1-10000",
			IsModifiable:  true,
		},
		{
			Name:          "cas_disabled",
			Value:         "0",
			Description:   "Disable CAS command",
			DataType:      dataTypeInteger,
			AllowedValues: "0,1",
			IsModifiable:  true,
		},
		{
			Name:          "loglevel",
			Value:         "notice",
			Description:   "Log verbosity level",
			DataType:      dataTypeString,
			AllowedValues: "notice,verbose,debug,trace",
			IsModifiable:  true,
		},
	}
}

func builtinValkeyDefaultParameters() []CacheParameter {
	return []CacheParameter{
		{
			Name:          "maxmemory-policy",
			Value:         "noeviction",
			Description:   "Eviction policy when memory is full",
			DataType:      dataTypeString,
			AllowedValues: allowedValuesEvictionPolicy,
			IsModifiable:  true,
		},
		{
			Name:          "hz",
			Value:         "10",
			Description:   "Background task frequency in Hz",
			DataType:      dataTypeInteger,
			AllowedValues: "1-500",
			IsModifiable:  true,
		},
		{
			Name:          "timeout",
			Value:         "0",
			Description:   "Client idle timeout in seconds (0=disabled)",
			DataType:      dataTypeInteger,
			AllowedValues: allowedValuesMaxInt32,
			IsModifiable:  true,
		},
		{
			Name:          "tcp-keepalive",
			Value:         "300",
			Description:   "TCP keepalive interval in seconds",
			DataType:      dataTypeInteger,
			AllowedValues: allowedValuesMaxInt32,
			IsModifiable:  true,
		},
		{
			Name:          "maxmemory-samples",
			Value:         "5",
			Description:   "Samples for LRU/LFU approximation",
			DataType:      dataTypeInteger,
			AllowedValues: "1-64",
			IsModifiable:  true,
		},
		{
			Name:          "activerehashing",
			Value:         "yes",
			Description:   "Active rehashing",
			DataType:      dataTypeString,
			AllowedValues: allowedValuesYesNo,
			IsModifiable:  true,
		},
		{
			Name:          "lazyfree-lazy-eviction",
			Value:         "no",
			Description:   "Lazy free on eviction",
			DataType:      dataTypeString,
			AllowedValues: allowedValuesYesNo,
			IsModifiable:  true,
		},
		{
			Name:          "lazyfree-lazy-expire",
			Value:         "no",
			Description:   "Lazy free on expiry",
			DataType:      dataTypeString,
			AllowedValues: allowedValuesYesNo,
			IsModifiable:  true,
		},
	}
}

func builtinRedisDefaultParameters() []CacheParameter {
	return append(builtinValkeyDefaultParameters(), builtinRedisPersistenceParameters()...)
}

func builtinRedisPersistenceParameters() []CacheParameter {
	return []CacheParameter{
		{
			Name:          "appendonly",
			Value:         "no",
			Description:   "Enable AOF persistence",
			DataType:      dataTypeString,
			AllowedValues: allowedValuesYesNo,
			IsModifiable:  true,
		},
		{
			Name:          "appendfsync",
			Value:         "everysec",
			Description:   "AOF fsync policy",
			DataType:      dataTypeString,
			AllowedValues: "always,everysec,no",
			IsModifiable:  true,
		},
		{
			Name:          "no-appendfsync-on-rewrite",
			Value:         "no",
			Description:   "Disable fsync during BGSAVE/BGREWRITEAOF",
			DataType:      dataTypeString,
			AllowedValues: allowedValuesYesNo,
			IsModifiable:  true,
		},
		{
			Name:          "auto-aof-rewrite-percentage",
			Value:         "100",
			Description:   "Min AOF growth percent before rewrite",
			DataType:      dataTypeInteger,
			AllowedValues: "0-",
			IsModifiable:  true,
		},
		{
			Name:          "auto-aof-rewrite-min-size",
			Value:         "67108864",
			Description:   "Min AOF size before rewrite (bytes)",
			DataType:      dataTypeInteger,
			AllowedValues: "0-",
			IsModifiable:  true,
		},
		{
			Name:          "slowlog-log-slower-than",
			Value:         "10000",
			Description:   "Slow log threshold in microseconds",
			DataType:      dataTypeInteger,
			AllowedValues: "-1-",
			IsModifiable:  true,
		},
		{
			Name:          "slowlog-max-len",
			Value:         "128",
			Description:   "Maximum slow log entries",
			DataType:      dataTypeInteger,
			AllowedValues: "0-",
			IsModifiable:  true,
		},
		{
			Name:          "latency-monitor-threshold",
			Value:         "0",
			Description:   "Latency monitor threshold in milliseconds (0=disabled)",
			DataType:      dataTypeInteger,
			AllowedValues: "0-",
			IsModifiable:  true,
		},
	}
}

// DescribeServiceUpdates returns service updates, filtered by name and status.
func (b *InMemoryBackend) DescribeServiceUpdates(
	_ context.Context,
	serviceUpdateName string,
	marker string,
	maxRecords int,
	status []string,
) (page.Page[ServiceUpdate], error) {
	data, next, err := b.DescribeServiceUpdatesFull(serviceUpdateName, status, marker, maxRecords)
	if err != nil {
		return page.Page[ServiceUpdate]{}, err
	}

	return page.Page[ServiceUpdate]{Data: data, Next: next}, nil
}

// DescribeUpdateActions returns update actions, filtered by service update name.
func (b *InMemoryBackend) DescribeUpdateActions(
	_ context.Context,
	serviceUpdateName string,
	marker string,
	maxRecords int,
) (page.Page[UpdateAction], error) {
	data, next, err := b.DescribeUpdateActionsFull(serviceUpdateName, marker, maxRecords)
	if err != nil {
		return page.Page[UpdateAction]{}, err
	}

	return page.Page[UpdateAction]{Data: data, Next: next}, nil
}

// ListAllowedNodeTypeModifications returns a list of allowed node type modifications.
func (b *InMemoryBackend) ListAllowedNodeTypeModifications(_ context.Context, _, _ string) ([]string, error) {
	return []string{
		nodeTypeT3Micro, "cache.t3.small", "cache.t3.medium",
		"cache.m6g.large", "cache.m6g.xlarge",
		"cache.r6g.large", "cache.r6g.xlarge", "cache.r6g.2xlarge",
	}, nil
}

// AddUserGroupInternal seeds a user group for testing.
func (b *InMemoryBackend) AddUserGroupInternal(ug *UserGroup) {
	b.mu.Lock("AddUserGroupInternal")
	defer b.mu.Unlock()
	b.userGroupsStore(b.region).Put(ug)
}
