package elasticache

import (
	"errors"
	"fmt"
	"sort"
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

func (b *InMemoryBackend) userGroupARN(id string) string {
	return arn.Build("elasticache", b.region, b.accountID, "usergroup:"+id)
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
func (b *InMemoryBackend) DeleteUser(userID string) (*User, error) {
	b.mu.Lock("DeleteUser")
	defer b.mu.Unlock()

	u, ok := b.users[userID]
	if !ok {
		return nil, ErrUserNotFound
	}

	result := *u
	delete(b.users, userID)
	b.appendEventLocked(userID, "user", "user deleted")

	return &result, nil
}

// DescribeUsers returns a paginated list of users, optionally filtered by userID.
func (b *InMemoryBackend) DescribeUsers(userID, marker string, maxRecords int) (page.Page[User], error) {
	b.mu.RLock("DescribeUsers")
	defer b.mu.RUnlock()

	if userID != "" {
		u, ok := b.users[userID]
		if !ok {
			return page.Page[User]{}, ErrUserNotFound
		}

		return page.Page[User]{Data: []User{*u}}, nil
	}

	out := make([]User, 0, len(b.users))
	for _, u := range b.users {
		out = append(out, *u)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].UserID < out[j].UserID })

	return page.New(out, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// ModifyUser modifies a user's access string and/or password settings.
func (b *InMemoryBackend) ModifyUser(userID, accessString string, noPasswordRequired bool) (*User, error) {
	b.mu.Lock("ModifyUser")
	defer b.mu.Unlock()

	u, ok := b.users[userID]
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
func (b *InMemoryBackend) CreateUserGroup(groupID, description, engine string, userIDs []string) (*UserGroup, error) {
	b.mu.Lock("CreateUserGroup")
	defer b.mu.Unlock()

	if _, exists := b.userGroups[groupID]; exists {
		return nil, ErrUserGroupAlreadyExists
	}

	if engine == "" {
		engine = engineRedis
	}

	ug := &UserGroup{
		UserGroupID: groupID,
		Description: description,
		Status:      statusActive,
		ARN:         b.userGroupARN(groupID),
		Engine:      engine,
		UserIDs:     userIDs,
		CreatedAt:   time.Now(),
		Tags:        tags.New("elasticache.usergroup." + groupID + ".tags"),
	}
	b.userGroups[groupID] = ug
	b.appendEventLocked(groupID, "user-group", "user group created")

	return ug, nil
}

// DeleteUserGroup deletes a user group by ID.
func (b *InMemoryBackend) DeleteUserGroup(groupID string) (*UserGroup, error) {
	b.mu.Lock("DeleteUserGroup")
	defer b.mu.Unlock()

	ug, ok := b.userGroups[groupID]
	if !ok {
		return nil, ErrUserGroupNotFound
	}

	result := *ug
	delete(b.userGroups, groupID)
	b.appendEventLocked(groupID, "user-group", "user group deleted")

	return &result, nil
}

// DescribeUserGroups returns a paginated list of user groups, optionally filtered by groupID.
func (b *InMemoryBackend) DescribeUserGroups(groupID, marker string, maxRecords int) (page.Page[UserGroup], error) {
	b.mu.RLock("DescribeUserGroups")
	defer b.mu.RUnlock()

	if groupID != "" {
		ug, ok := b.userGroups[groupID]
		if !ok {
			return page.Page[UserGroup]{}, ErrUserGroupNotFound
		}

		return page.Page[UserGroup]{Data: []UserGroup{*ug}}, nil
	}

	out := make([]UserGroup, 0, len(b.userGroups))
	for _, ug := range b.userGroups {
		out = append(out, *ug)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].UserGroupID < out[j].UserGroupID })

	return page.New(out, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// ModifyUserGroup adds or removes users from a user group.
func (b *InMemoryBackend) ModifyUserGroup(groupID string, userIDsToAdd, userIDsToRemove []string) (*UserGroup, error) {
	b.mu.Lock("ModifyUserGroup")
	defer b.mu.Unlock()

	ug, ok := b.userGroups[groupID]
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
func (b *InMemoryBackend) DeleteGlobalReplicationGroup(id string, _ bool) (*GlobalReplicationGroup, error) {
	b.mu.Lock("DeleteGlobalReplicationGroup")
	defer b.mu.Unlock()

	grg, ok := b.globalReplicationGroups[id]
	if !ok {
		return nil, ErrGlobalReplicationGroupNotFound
	}

	result := *grg
	delete(b.globalReplicationGroups, id)
	b.appendEventLocked(id, "global-replication-group", "global replication group deleted")

	return &result, nil
}

// DescribeGlobalReplicationGroups returns a paginated list of global replication groups.
func (b *InMemoryBackend) DescribeGlobalReplicationGroups(
	id, marker string,
	maxRecords int,
) (page.Page[GlobalReplicationGroup], error) {
	b.mu.RLock("DescribeGlobalReplicationGroups")
	defer b.mu.RUnlock()

	if id != "" {
		grg, ok := b.globalReplicationGroups[id]
		if !ok {
			return page.Page[GlobalReplicationGroup]{}, ErrGlobalReplicationGroupNotFound
		}

		return page.Page[GlobalReplicationGroup]{Data: []GlobalReplicationGroup{*grg}}, nil
	}

	out := make([]GlobalReplicationGroup, 0, len(b.globalReplicationGroups))
	for _, grg := range b.globalReplicationGroups {
		out = append(out, *grg)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].GlobalReplicationGroupID < out[j].GlobalReplicationGroupID
	})

	return page.New(out, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// DisassociateGlobalReplicationGroup removes a secondary replication group from a global replication group.
func (b *InMemoryBackend) DisassociateGlobalReplicationGroup(id, _, _ string) (*GlobalReplicationGroup, error) {
	b.mu.Lock("DisassociateGlobalReplicationGroup")
	defer b.mu.Unlock()

	grg, ok := b.globalReplicationGroups[id]
	if !ok {
		return nil, ErrGlobalReplicationGroupNotFound
	}

	result := *grg

	return &result, nil
}

// FailoverGlobalReplicationGroup promotes a secondary region to primary.
func (b *InMemoryBackend) FailoverGlobalReplicationGroup(id, _, _ string) (*GlobalReplicationGroup, error) {
	b.mu.Lock("FailoverGlobalReplicationGroup")
	defer b.mu.Unlock()

	grg, ok := b.globalReplicationGroups[id]
	if !ok {
		return nil, ErrGlobalReplicationGroupNotFound
	}

	result := *grg

	return &result, nil
}

// IncreaseNodeGroupsInGlobalReplicationGroup increases the node group count.
func (b *InMemoryBackend) IncreaseNodeGroupsInGlobalReplicationGroup(
	id string,
	_ int32,
) (*GlobalReplicationGroup, error) {
	b.mu.Lock("IncreaseNodeGroupsInGlobalReplicationGroup")
	defer b.mu.Unlock()

	grg, ok := b.globalReplicationGroups[id]
	if !ok {
		return nil, ErrGlobalReplicationGroupNotFound
	}

	result := *grg

	return &result, nil
}

// DecreaseNodeGroupsInGlobalReplicationGroup decreases the node group count.
func (b *InMemoryBackend) DecreaseNodeGroupsInGlobalReplicationGroup(
	id string,
	_ int32,
) (*GlobalReplicationGroup, error) {
	b.mu.Lock("DecreaseNodeGroupsInGlobalReplicationGroup")
	defer b.mu.Unlock()

	grg, ok := b.globalReplicationGroups[id]
	if !ok {
		return nil, ErrGlobalReplicationGroupNotFound
	}

	result := *grg

	return &result, nil
}

// ModifyGlobalReplicationGroup modifies a global replication group.
func (b *InMemoryBackend) ModifyGlobalReplicationGroup(
	id, description, engineVersion string,
	automaticFailoverEnabled bool,
) (*GlobalReplicationGroup, error) {
	b.mu.Lock("ModifyGlobalReplicationGroup")
	defer b.mu.Unlock()

	grg, ok := b.globalReplicationGroups[id]
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
	result := *grg

	return &result, nil
}

// RebalanceSlotsInGlobalReplicationGroup rebalances slots.
func (b *InMemoryBackend) RebalanceSlotsInGlobalReplicationGroup(id string) (*GlobalReplicationGroup, error) {
	b.mu.Lock("RebalanceSlotsInGlobalReplicationGroup")
	defer b.mu.Unlock()

	grg, ok := b.globalReplicationGroups[id]
	if !ok {
		return nil, ErrGlobalReplicationGroupNotFound
	}

	result := *grg

	return &result, nil
}

// DescribeReservedCacheNodes returns a paginated list of reserved cache nodes.
func (b *InMemoryBackend) DescribeReservedCacheNodes(
	id, cacheNodeType, offeringType, marker string,
	maxRecords int,
) (page.Page[ReservedCacheNode], error) {
	b.mu.RLock("DescribeReservedCacheNodes")
	defer b.mu.RUnlock()

	if id != "" {
		rcn, ok := b.reservedCacheNodes[id]
		if !ok {
			return page.Page[ReservedCacheNode]{}, ErrReservedCacheNodeNotFound
		}

		return page.Page[ReservedCacheNode]{Data: []ReservedCacheNode{*rcn}}, nil
	}

	out := make([]ReservedCacheNode, 0, len(b.reservedCacheNodes))
	for _, rcn := range b.reservedCacheNodes {
		if cacheNodeType != "" && rcn.CacheNodeType != cacheNodeType {
			continue
		}
		if offeringType != "" && rcn.OfferingType != offeringType {
			continue
		}
		out = append(out, *rcn)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ReservedCacheNodeID < out[j].ReservedCacheNodeID })

	return page.New(out, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// DescribeReservedCacheNodesOfferings returns a paginated list of reserved cache node offerings.
func (b *InMemoryBackend) DescribeReservedCacheNodesOfferings(
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
		reservedCacheNodeID = fmt.Sprintf("rcn-%s", offeringID[:8])
	}

	if _, exists := b.reservedCacheNodes[reservedCacheNodeID]; exists {
		return nil, fmt.Errorf("reserved cache node %q: %w", reservedCacheNodeID, ErrReservedCacheNodeAlreadyExists)
	}

	rcn := &ReservedCacheNode{
		ReservedCacheNodeID: reservedCacheNodeID,
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
	b.reservedCacheNodes[reservedCacheNodeID] = rcn
	b.appendEventLocked(reservedCacheNodeID, "reserved-cache-node", "reserved cache node purchased")

	return rcn, nil
}

// DeleteServerlessCache deletes a serverless cache.
func (b *InMemoryBackend) DeleteServerlessCache(name string) (*ServerlessCache, error) {
	b.mu.Lock("DeleteServerlessCache")
	defer b.mu.Unlock()

	sc, ok := b.serverlessCaches[name]
	if !ok {
		return nil, ErrServerlessCacheNotFound
	}

	result := *sc
	delete(b.serverlessCaches, name)
	b.appendEventLocked(name, "serverless-cache", "serverless cache deleted")

	return &result, nil
}

// DeleteServerlessCacheSnapshot deletes a serverless cache snapshot.
func (b *InMemoryBackend) DeleteServerlessCacheSnapshot(name string) (*ServerlessCacheSnapshot, error) {
	b.mu.Lock("DeleteServerlessCacheSnapshot")
	defer b.mu.Unlock()

	snap, ok := b.serverlessCacheSnapshots[name]
	if !ok {
		return nil, ErrServerlessCacheSnapshotNotFound
	}

	result := *snap
	delete(b.serverlessCacheSnapshots, name)
	b.appendEventLocked(name, "serverless-cache-snapshot", "serverless cache snapshot deleted")

	return &result, nil
}

// DescribeServerlessCaches returns a paginated list of serverless caches.
func (b *InMemoryBackend) DescribeServerlessCaches(
	name, marker string,
	maxRecords int,
) (page.Page[ServerlessCache], error) {
	b.mu.RLock("DescribeServerlessCaches")
	defer b.mu.RUnlock()

	if name != "" {
		sc, ok := b.serverlessCaches[name]
		if !ok {
			return page.Page[ServerlessCache]{}, ErrServerlessCacheNotFound
		}

		return page.Page[ServerlessCache]{Data: []ServerlessCache{*sc}}, nil
	}

	out := make([]ServerlessCache, 0, len(b.serverlessCaches))
	for _, sc := range b.serverlessCaches {
		out = append(out, *sc)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return page.New(out, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// DescribeServerlessCacheSnapshots returns a paginated list of serverless cache snapshots.
func (b *InMemoryBackend) DescribeServerlessCacheSnapshots(
	serverlessCacheName, snapshotName, marker string,
	maxRecords int,
) (page.Page[ServerlessCacheSnapshot], error) {
	b.mu.RLock("DescribeServerlessCacheSnapshots")
	defer b.mu.RUnlock()

	if snapshotName != "" {
		snap, ok := b.serverlessCacheSnapshots[snapshotName]
		if !ok {
			return page.Page[ServerlessCacheSnapshot]{}, ErrServerlessCacheSnapshotNotFound
		}

		return page.Page[ServerlessCacheSnapshot]{Data: []ServerlessCacheSnapshot{*snap}}, nil
	}

	out := make([]ServerlessCacheSnapshot, 0, len(b.serverlessCacheSnapshots))
	for _, snap := range b.serverlessCacheSnapshots {
		if serverlessCacheName != "" && snap.ServerlessCacheName != serverlessCacheName {
			continue
		}

		out = append(out, *snap)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return page.New(out, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// ExportServerlessCacheSnapshot exports a serverless cache snapshot to S3.
func (b *InMemoryBackend) ExportServerlessCacheSnapshot(snapshotName, _ string) (*ServerlessCacheSnapshot, error) {
	b.mu.Lock("ExportServerlessCacheSnapshot")
	defer b.mu.Unlock()

	snap, ok := b.serverlessCacheSnapshots[snapshotName]
	if !ok {
		return nil, ErrServerlessCacheSnapshotNotFound
	}

	result := *snap

	return &result, nil
}

// ModifyServerlessCache modifies a serverless cache.
func (b *InMemoryBackend) ModifyServerlessCache(name, description string) (*ServerlessCache, error) {
	b.mu.Lock("ModifyServerlessCache")
	defer b.mu.Unlock()

	sc, ok := b.serverlessCaches[name]
	if !ok {
		return nil, ErrServerlessCacheNotFound
	}

	if description != "" {
		sc.Description = description
	}

	result := *sc

	return &result, nil
}

// StartMigration starts a migration for a replication group.
func (b *InMemoryBackend) StartMigration(replicationGroupID string) (*ReplicationGroup, error) {
	b.mu.Lock("StartMigration")
	defer b.mu.Unlock()

	rg, ok := b.replicationGroups[replicationGroupID]
	if !ok {
		return nil, ErrReplicationGroupNotFound
	}

	rg.Status = "migrating"
	result := *rg

	return &result, nil
}

// TestMigration tests a migration for a replication group.
func (b *InMemoryBackend) TestMigration(replicationGroupID string) (*ReplicationGroup, error) {
	b.mu.Lock("TestMigration")
	defer b.mu.Unlock()

	rg, ok := b.replicationGroups[replicationGroupID]
	if !ok {
		return nil, ErrReplicationGroupNotFound
	}

	result := *rg

	return &result, nil
}

// IncreaseReplicaCount increases the replica count for a replication group.
func (b *InMemoryBackend) IncreaseReplicaCount(
	replicationGroupID string,
	newReplicaCount int32,
) (*ReplicationGroup, error) {
	b.mu.Lock("IncreaseReplicaCount")
	defer b.mu.Unlock()

	rg, ok := b.replicationGroups[replicationGroupID]
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
	replicationGroupID string,
	newReplicaCount int32,
) (*ReplicationGroup, error) {
	b.mu.Lock("DecreaseReplicaCount")
	defer b.mu.Unlock()

	rg, ok := b.replicationGroups[replicationGroupID]
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
	replicationGroupID string,
	nodeGroupCount int32,
) (*ReplicationGroup, error) {
	b.mu.Lock("ModifyReplicationGroupShardConfiguration")
	defer b.mu.Unlock()

	rg, ok := b.replicationGroups[replicationGroupID]
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
func (b *InMemoryBackend) RebootCacheCluster(clusterID string, nodeIDs []string) (*Cluster, error) {
	b.mu.Lock("RebootCacheCluster")
	defer b.mu.Unlock()

	c, ok := b.clusters[clusterID]
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

	result := *c

	return &result, nil
}

// DeleteCacheSecurityGroup deletes a cache security group.
func (b *InMemoryBackend) DeleteCacheSecurityGroup(name string) error {
	b.mu.Lock("DeleteCacheSecurityGroup")
	defer b.mu.Unlock()

	if _, ok := b.cacheSecurityGroups[name]; !ok {
		return ErrCacheSecurityGroupNotFound
	}

	delete(b.cacheSecurityGroups, name)
	delete(b.cacheSecurityGroupIngress, name)

	return nil
}

// DescribeCacheSecurityGroups returns a paginated list of cache security groups.
func (b *InMemoryBackend) DescribeCacheSecurityGroups(
	name, marker string,
	maxRecords int,
) (page.Page[CacheSecurityGroup], error) {
	b.mu.RLock("DescribeCacheSecurityGroups")
	defer b.mu.RUnlock()

	if name != "" {
		sg, ok := b.cacheSecurityGroups[name]
		if !ok {
			return page.Page[CacheSecurityGroup]{}, ErrCacheSecurityGroupNotFound
		}

		return page.Page[CacheSecurityGroup]{Data: []CacheSecurityGroup{*sg}}, nil
	}

	out := make([]CacheSecurityGroup, 0, len(b.cacheSecurityGroups))
	for _, sg := range b.cacheSecurityGroups {
		out = append(out, *sg)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return page.New(out, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// RevokeCacheSecurityGroupIngress removes an EC2 security group authorization.
func (b *InMemoryBackend) RevokeCacheSecurityGroupIngress(
	name, ec2SecurityGroupName, ec2SecurityGroupOwnerID string,
) (*CacheSecurityGroup, error) {
	b.mu.Lock("RevokeCacheSecurityGroupIngress")
	defer b.mu.Unlock()

	sg, ok := b.cacheSecurityGroups[name]
	if !ok {
		return nil, ErrCacheSecurityGroupNotFound
	}

	ingress := b.cacheSecurityGroupIngress[name]
	filtered := make([]EC2SecurityGroupMembership, 0, len(ingress))

	for _, entry := range ingress {
		if entry.EC2SecurityGroupName == ec2SecurityGroupName &&
			entry.EC2SecurityGroupOwnerID == ec2SecurityGroupOwnerID {
			continue
		}

		filtered = append(filtered, entry)
	}

	b.cacheSecurityGroupIngress[name] = filtered
	result := *sg

	return &result, nil
}

// DescribeEngineDefaultParameters returns the default parameters for a parameter group family.
func (b *InMemoryBackend) DescribeEngineDefaultParameters(
	_ string,
	marker string,
	maxRecords int,
) (page.Page[CacheParameter], error) {
	b.mu.RLock("DescribeEngineDefaultParameters")
	defer b.mu.RUnlock()

	return page.New([]CacheParameter{}, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// DescribeServiceUpdates returns service updates.
func (b *InMemoryBackend) DescribeServiceUpdates(
	_ string,
	marker string,
	maxRecords int,
	_ []string,
) (page.Page[ServiceUpdate], error) {
	b.mu.RLock("DescribeServiceUpdates")
	defer b.mu.RUnlock()

	return page.New([]ServiceUpdate{}, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// DescribeUpdateActions returns update actions.
func (b *InMemoryBackend) DescribeUpdateActions(
	_ string,
	marker string,
	maxRecords int,
) (page.Page[UpdateAction], error) {
	b.mu.RLock("DescribeUpdateActions")
	defer b.mu.RUnlock()

	return page.New([]UpdateAction{}, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// ListAllowedNodeTypeModifications returns a list of allowed node type modifications.
func (b *InMemoryBackend) ListAllowedNodeTypeModifications(_, _ string) ([]string, error) {
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
	b.userGroups[ug.UserGroupID] = ug
}
