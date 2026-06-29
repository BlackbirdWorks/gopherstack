package elasticache

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	statusActive = "active"
)

// ----------------------------------------
// Sentinel errors for new op types
// ----------------------------------------

var (
	ErrCacheSecurityGroupNotFound      = errors.New("CacheSecurityGroupNotFound")
	ErrCacheSecurityGroupAlreadyExists = errors.New("CacheSecurityGroupAlreadyExists")
	ErrGlobalReplicationGroupNotFound  = errors.New("GlobalReplicationGroupNotFound")
	ErrGlobalReplicationGroupExists    = errors.New("GlobalReplicationGroupAlreadyExistsFault")
	ErrServerlessCacheNotFound         = errors.New("ServerlessCacheNotFound")
	ErrServerlessCacheAlreadyExists    = errors.New("ServerlessCacheAlreadyExistsFault")
	ErrServerlessCacheSnapshotNotFound = errors.New("ServerlessCacheSnapshotNotFoundFault")
	ErrServerlessCacheSnapshotExists   = errors.New("ServerlessCacheSnapshotAlreadyExistsFault")
	ErrUserNotFound                    = errors.New("UserNotFound")
	ErrUserAlreadyExists               = errors.New("UserAlreadyExists")
)

// ----------------------------------------
// New model types
// ----------------------------------------

// CacheSecurityGroup represents an ElastiCache cache security group (EC2-Classic).
type CacheSecurityGroup struct {
	Tags        *tags.Tags `json:"tags,omitempty"`
	Name        string     `json:"name"`
	Description string     `json:"description"`
	ARN         string     `json:"arn"`
	OwnerID     string     `json:"ownerId"`
}

// EC2SecurityGroupMembership is a single EC2 security group authorization on a cache security group.
type EC2SecurityGroupMembership struct {
	EC2SecurityGroupName    string `json:"ec2SecurityGroupName"`
	EC2SecurityGroupOwnerID string `json:"ec2SecurityGroupOwnerId"`
	Status                  string `json:"status"`
}

// GlobalReplicationGroup represents an ElastiCache global replication group.
type GlobalReplicationGroup struct {
	CreatedAt                     time.Time         `json:"createdAt"`
	Tags                          *tags.Tags        `json:"tags,omitempty"`
	SecondaryReplicationGroups    map[string]string `json:"secondaryReplicationGroups,omitempty"`
	GlobalReplicationGroupID      string            `json:"globalReplicationGroupId"`
	Description                   string            `json:"description"`
	Status                        string            `json:"status"`
	ARN                           string            `json:"arn"`
	Engine                        string            `json:"engine"`
	EngineVersion                 string            `json:"engineVersion"`
	PrimaryReplicationGroupRegion string            `json:"primaryReplicationGroupRegion,omitempty"`
	NodeGroupCount                int32             `json:"nodeGroupCount,omitempty"`
}

// ServerlessCacheEndpoint holds the address and port for a serverless cache endpoint.
type ServerlessCacheEndpoint struct {
	Address string `json:"address"`
	Port    int    `json:"port"`
}

// ServerlessCache represents an ElastiCache serverless cache.
type ServerlessCache struct {
	CreatedAt              time.Time                `json:"createdAt"`
	Tags                   *tags.Tags               `json:"tags,omitempty"`
	Endpoint               *ServerlessCacheEndpoint `json:"endpoint,omitempty"`
	ReaderEndpoint         *ServerlessCacheEndpoint `json:"readerEndpoint,omitempty"`
	Name                   string                   `json:"name"`
	Description            string                   `json:"description"`
	Status                 string                   `json:"status"`
	ARN                    string                   `json:"arn"`
	Engine                 string                   `json:"engine"`
	KmsKeyID               string                   `json:"kmsKeyId,omitempty"`
	UserGroupID            string                   `json:"userGroupId,omitempty"`
	SubnetGroupName        string                   `json:"subnetGroupName,omitempty"`
	DailySnapshotTime      string                   `json:"dailySnapshotTime,omitempty"`
	MajorEngineVersion     string                   `json:"majorEngineVersion,omitempty"`
	SubnetIDs              []string                 `json:"subnetIds,omitempty"`
	SecurityGroupIDs       []string                 `json:"securityGroupIds,omitempty"`
	SnapshotRetentionLimit int32                    `json:"snapshotRetentionLimit,omitempty"`
}

// ServerlessCacheSnapshot represents a snapshot of a serverless cache.
type ServerlessCacheSnapshot struct {
	CreatedAt           time.Time  `json:"createdAt"`
	Tags                *tags.Tags `json:"tags,omitempty"`
	Name                string     `json:"name"`
	Status              string     `json:"status"`
	ARN                 string     `json:"arn"`
	ServerlessCacheName string     `json:"serverlessCacheName"`
	SnapshotType        string     `json:"snapshotType"` // "manual" or "automated"
}

// User represents an ElastiCache user.
type User struct {
	CreatedAt          time.Time  `json:"createdAt"`
	Tags               *tags.Tags `json:"tags,omitempty"`
	UserID             string     `json:"userId"`
	UserName           string     `json:"userName"`
	Status             string     `json:"status"`
	ARN                string     `json:"arn"`
	Engine             string     `json:"engine"`
	AccessString       string     `json:"accessString"`
	NoPasswordRequired bool       `json:"noPasswordRequired"`
}

// UpdateActionResult represents the outcome of a single update action.
type UpdateActionResult struct {
	ReplicationGroupID string `json:"replicationGroupId,omitempty"`
	CacheClusterID     string `json:"cacheClusterId,omitempty"`
	ServiceUpdateName  string `json:"serviceUpdateName"`
	UpdateActionStatus string `json:"updateActionStatus"`
}

// BatchUpdateResult holds the results of a BatchApplyUpdateAction / BatchStopUpdateAction call.
type BatchUpdateResult struct {
	ProcessedUpdateActions   []UpdateActionResult `json:"processedUpdateActions"`
	UnprocessedUpdateActions []UpdateActionResult `json:"unprocessedUpdateActions"`
}

// ----------------------------------------
// ARN builders
// ----------------------------------------

func (b *InMemoryBackend) cacheSecurityGroupARN(region, name string) string {
	return arn.Build("elasticache", region, b.accountID, "securitygroup:"+name)
}

func (b *InMemoryBackend) globalReplicationGroupARN(id string) string {
	return arn.Build("elasticache", b.region, b.accountID, "globalreplicationgroup:"+id)
}

func (b *InMemoryBackend) serverlessCacheARN(region, name string) string {
	return arn.Build("elasticache", region, b.accountID, "serverlesscache:"+name)
}

func (b *InMemoryBackend) serverlessCacheSnapshotARN(region, name string) string {
	return arn.Build("elasticache", region, b.accountID, "serverlesssnapshot:"+name)
}

func (b *InMemoryBackend) userARN(region, userID string) string {
	return arn.Build("elasticache", region, b.accountID, "user:"+userID)
}

// ----------------------------------------
// CreateCacheSecurityGroup
// ----------------------------------------

// CreateCacheSecurityGroup creates a new cache security group.
func (b *InMemoryBackend) CreateCacheSecurityGroup(
	ctx context.Context,
	name, description string,
) (*CacheSecurityGroup, error) {
	b.mu.Lock("CreateCacheSecurityGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.cacheSecurityGroupsStore(region)
	if _, exists := store[name]; exists {
		return nil, ErrCacheSecurityGroupAlreadyExists
	}

	sg := &CacheSecurityGroup{
		Name:        name,
		Description: description,
		ARN:         b.cacheSecurityGroupARN(region, name),
		OwnerID:     b.accountID,
		Tags:        tags.New("elasticache.sg." + name + ".tags"),
	}
	store[name] = sg

	return sg, nil
}

// AuthorizeCacheSecurityGroupIngress adds an EC2 security group authorization to the named cache security group.
func (b *InMemoryBackend) AuthorizeCacheSecurityGroupIngress(
	ctx context.Context,
	name, ec2SecurityGroupName, ec2SecurityGroupOwnerID string,
) (*CacheSecurityGroup, error) {
	b.mu.Lock("AuthorizeCacheSecurityGroupIngress")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	sg, ok := b.cacheSecurityGroupsStore(region)[name]
	if !ok {
		return nil, ErrCacheSecurityGroupNotFound
	}

	ingressStore := b.cacheSecurityGroupIngressStore(region)
	ingressStore[name] = append(ingressStore[name], EC2SecurityGroupMembership{
		EC2SecurityGroupName:    ec2SecurityGroupName,
		EC2SecurityGroupOwnerID: ec2SecurityGroupOwnerID,
		Status:                  "authorized",
	})

	result := *sg

	return &result, nil
}

// ----------------------------------------
// CreateGlobalReplicationGroup
// ----------------------------------------

// CreateGlobalReplicationGroup creates a new global replication group.
func (b *InMemoryBackend) CreateGlobalReplicationGroup(
	ctx context.Context,
	globalReplicationGroupIDSuffix, description, primaryReplicationGroupID string,
) (*GlobalReplicationGroup, error) {
	b.mu.Lock("CreateGlobalReplicationGroup")
	defer b.mu.Unlock()

	id := "ldgnf-" + globalReplicationGroupIDSuffix
	if _, exists := b.getGlobalReplicationGroup(id); exists {
		return nil, ErrGlobalReplicationGroupExists
	}

	region := getRegion(ctx, b.region)
	engine := engineRedis
	engineVersion := versionRedis710
	if rg, ok := b.replicationGroupsStore(region)[primaryReplicationGroupID]; ok {
		if rg.EngineVersion != "" {
			engineVersion = rg.EngineVersion
		}
		if rg.Engine != "" {
			engine = rg.Engine
		}
	}

	nodeGroupCount := int32(1)
	if rg, ok := b.replicationGroupsStore(region)[primaryReplicationGroupID]; ok && len(rg.NodeGroups) > 0 {
		var cnt int32
		for range rg.NodeGroups {
			cnt++
		}
		nodeGroupCount = cnt
	}

	grg := &GlobalReplicationGroup{
		GlobalReplicationGroupID:      id,
		Description:                   description,
		Status:                        statusAvailable,
		ARN:                           b.globalReplicationGroupARN(id),
		Engine:                        engine,
		EngineVersion:                 engineVersion,
		PrimaryReplicationGroupRegion: region,
		SecondaryReplicationGroups:    make(map[string]string),
		CreatedAt:                     time.Now(),
		Tags:                          tags.New("elasticache.grg." + id + ".tags"),
		NodeGroupCount:                nodeGroupCount,
	}
	b.putGlobalReplicationGroup(id, grg)
	b.appendEventLocked(id, "global-replication-group", "global replication group created")

	return grg, nil
}

// ----------------------------------------
// CreateServerlessCache
// ----------------------------------------

// CreateServerlessCache creates a new serverless cache.
func (b *InMemoryBackend) CreateServerlessCache(
	ctx context.Context,
	name, description, engine string,
) (*ServerlessCache, error) {
	b.mu.Lock("CreateServerlessCache")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.serverlessCachesStore(region)
	if _, exists := store[name]; exists {
		return nil, ErrServerlessCacheAlreadyExists
	}

	if engine == "" {
		engine = engineRedis
	}

	suffix := randomSuffix()
	host := fmt.Sprintf("%s.serverless.%s.%s.cache.amazonaws.com", name, suffix, region)
	readerHost := fmt.Sprintf("%s.serverless.%s.%s.cache.amazonaws.com", name+"-ro", suffix, region)
	port := 6379
	if engine == engineMemcached {
		port = 11211
	}

	ep := &ServerlessCacheEndpoint{Address: host, Port: port}
	readerEp := &ServerlessCacheEndpoint{Address: readerHost, Port: port}

	sc := &ServerlessCache{
		Name:           name,
		Description:    description,
		Status:         statusServerlessAvailable,
		ARN:            b.serverlessCacheARN(region, name),
		Engine:         engine,
		CreatedAt:      time.Now(),
		Tags:           tags.New("elasticache.serverless." + name + ".tags"),
		Endpoint:       ep,
		ReaderEndpoint: readerEp,
	}
	store[name] = sc
	b.appendEventLocked(name, "serverless-cache", "serverless cache created")

	return sc, nil
}

// ----------------------------------------
// CreateServerlessCacheSnapshot
// ----------------------------------------

// CreateServerlessCacheSnapshot creates a manual snapshot of a serverless cache.
func (b *InMemoryBackend) CreateServerlessCacheSnapshot(
	ctx context.Context,
	snapshotName, serverlessCacheName string,
) (*ServerlessCacheSnapshot, error) {
	b.mu.Lock("CreateServerlessCacheSnapshot")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	snapStore := b.serverlessCacheSnapshotsStore(region)
	if _, exists := snapStore[snapshotName]; exists {
		return nil, ErrServerlessCacheSnapshotExists
	}

	if _, ok := b.serverlessCachesStore(region)[serverlessCacheName]; !ok {
		return nil, ErrServerlessCacheNotFound
	}

	snap := &ServerlessCacheSnapshot{
		Name:                snapshotName,
		Status:              statusAvailable,
		ARN:                 b.serverlessCacheSnapshotARN(region, snapshotName),
		ServerlessCacheName: serverlessCacheName,
		SnapshotType:        snapshotSourceManual,
		CreatedAt:           time.Now(),
		Tags:                tags.New("elasticache.serverlesssnap." + snapshotName + ".tags"),
	}
	snapStore[snapshotName] = snap

	return snap, nil
}

// ----------------------------------------
// CopyServerlessCacheSnapshot
// ----------------------------------------

// CopyServerlessCacheSnapshot copies a serverless cache snapshot to a new name.
func (b *InMemoryBackend) CopyServerlessCacheSnapshot(
	ctx context.Context,
	sourceSnapshotName, targetSnapshotName string,
) (*ServerlessCacheSnapshot, error) {
	b.mu.Lock("CopyServerlessCacheSnapshot")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.serverlessCacheSnapshotsStore(region)

	src, ok := store[sourceSnapshotName]
	if !ok {
		return nil, ErrServerlessCacheSnapshotNotFound
	}

	if _, exists := store[targetSnapshotName]; exists {
		return nil, ErrServerlessCacheSnapshotExists
	}

	cp := *src
	cp.Name = targetSnapshotName
	cp.ARN = b.serverlessCacheSnapshotARN(region, targetSnapshotName)
	cp.CreatedAt = time.Now()
	cp.Tags = tags.New("elasticache.serverlesssnap." + targetSnapshotName + ".tags")
	store[targetSnapshotName] = &cp

	result := cp

	return &result, nil
}

// ----------------------------------------
// CreateUser
// ----------------------------------------

// CreateUser creates a new ElastiCache user.
func (b *InMemoryBackend) CreateUser(
	ctx context.Context,
	userID, userName, accessString, engine string,
	noPasswordRequired bool,
) (*User, error) {
	b.mu.Lock("CreateUser")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.usersStore(region)
	if _, exists := store[userID]; exists {
		return nil, ErrUserAlreadyExists
	}

	if engine == "" {
		engine = engineRedis
	}

	u := &User{
		UserID:             userID,
		UserName:           userName,
		Status:             statusActive,
		ARN:                b.userARN(region, userID),
		Engine:             engine,
		AccessString:       accessString,
		NoPasswordRequired: noPasswordRequired,
		CreatedAt:          time.Now(),
		Tags:               tags.New("elasticache.user." + userID + ".tags"),
	}
	store[userID] = u
	b.appendEventLocked(userID, "user", "user created")

	return u, nil
}

// batchUpdateActions processes a list of replication groups and clusters for a service update,
// returning processed (found) and unprocessed (not found) update action results.
func (b *InMemoryBackend) batchUpdateActions(
	replicationGroupIDs, cacheClusterIDs []string,
	serviceUpdateName, matchedStatus string,
) *BatchUpdateResult {
	result := &BatchUpdateResult{
		ProcessedUpdateActions:   []UpdateActionResult{},
		UnprocessedUpdateActions: []UpdateActionResult{},
	}

	for _, rgID := range replicationGroupIDs {
		found := false
		for _, regionRGs := range b.replicationGroups {
			if _, ok := regionRGs[rgID]; ok {
				found = true

				break
			}
		}
		if found {
			result.ProcessedUpdateActions = append(result.ProcessedUpdateActions, UpdateActionResult{
				ReplicationGroupID: rgID,
				ServiceUpdateName:  serviceUpdateName,
				UpdateActionStatus: matchedStatus,
			})
		} else {
			result.UnprocessedUpdateActions = append(result.UnprocessedUpdateActions, UpdateActionResult{
				ReplicationGroupID: rgID,
				ServiceUpdateName:  serviceUpdateName,
				UpdateActionStatus: "not-applicable",
			})
		}
	}

	for _, clusterID := range cacheClusterIDs {
		found := false
		for _, regionClusters := range b.clusters {
			if _, ok := regionClusters[clusterID]; ok {
				found = true

				break
			}
		}
		if found {
			result.ProcessedUpdateActions = append(result.ProcessedUpdateActions, UpdateActionResult{
				CacheClusterID:     clusterID,
				ServiceUpdateName:  serviceUpdateName,
				UpdateActionStatus: matchedStatus,
			})
		} else {
			result.UnprocessedUpdateActions = append(result.UnprocessedUpdateActions, UpdateActionResult{
				CacheClusterID:     clusterID,
				ServiceUpdateName:  serviceUpdateName,
				UpdateActionStatus: "not-applicable",
			})
		}
	}

	sort.Slice(result.ProcessedUpdateActions, func(i, j int) bool {
		ki := result.ProcessedUpdateActions[i].ReplicationGroupID + result.ProcessedUpdateActions[i].CacheClusterID
		kj := result.ProcessedUpdateActions[j].ReplicationGroupID + result.ProcessedUpdateActions[j].CacheClusterID

		return ki < kj
	})

	return result
}

// ----------------------------------------
// BatchApplyUpdateAction
// ----------------------------------------

// BatchApplyUpdateAction schedules a service update for the given replication groups and clusters.
func (b *InMemoryBackend) BatchApplyUpdateAction(
	_ context.Context,
	replicationGroupIDs, cacheClusterIDs []string,
	serviceUpdateName string,
) (*BatchUpdateResult, error) {
	b.mu.Lock("BatchApplyUpdateAction")
	defer b.mu.Unlock()

	result := b.batchUpdateActions(replicationGroupIDs, cacheClusterIDs, serviceUpdateName, "scheduling")

	for _, ua := range result.ProcessedUpdateActions {
		action := &UpdateAction{
			ReplicationGroupID: ua.ReplicationGroupID,
			CacheClusterID:     ua.CacheClusterID,
			ServiceUpdateName:  ua.ServiceUpdateName,
			UpdateActionStatus: ua.UpdateActionStatus,
		}
		b.updateActions = append(b.updateActions, action)
	}

	return result, nil
}

// ----------------------------------------
// BatchStopUpdateAction
// ----------------------------------------

// BatchStopUpdateAction stops a pending service update for the given replication groups and clusters.
func (b *InMemoryBackend) BatchStopUpdateAction(
	_ context.Context,
	replicationGroupIDs, cacheClusterIDs []string,
	serviceUpdateName string,
) (*BatchUpdateResult, error) {
	b.mu.RLock("BatchStopUpdateAction")
	defer b.mu.RUnlock()

	return b.batchUpdateActions(replicationGroupIDs, cacheClusterIDs, serviceUpdateName, "stopped"), nil
}

// ----------------------------------------
// CompleteMigration
// ----------------------------------------

// CompleteMigration completes an online data migration from an external Redis server to this replication group.
func (b *InMemoryBackend) CompleteMigration(
	ctx context.Context,
	replicationGroupID string,
	_ bool,
) (*ReplicationGroup, error) {
	b.mu.Lock("CompleteMigration")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	rg, ok := b.replicationGroupsStore(region)[replicationGroupID]
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

// AddCacheSecurityGroupInternal seeds a cache security group for testing.
func (b *InMemoryBackend) AddCacheSecurityGroupInternal(sg *CacheSecurityGroup) {
	b.mu.Lock("AddCacheSecurityGroupInternal")
	defer b.mu.Unlock()
	b.cacheSecurityGroupsStore(b.region)[sg.Name] = sg
}

// AddGlobalReplicationGroupInternal seeds a global replication group for testing.
func (b *InMemoryBackend) AddGlobalReplicationGroupInternal(grg *GlobalReplicationGroup) {
	b.mu.Lock("AddGlobalReplicationGroupInternal")
	defer b.mu.Unlock()
	b.putGlobalReplicationGroup(grg.GlobalReplicationGroupID, grg)
}

// AddServerlessCacheInternal seeds a serverless cache for testing.
func (b *InMemoryBackend) AddServerlessCacheInternal(sc *ServerlessCache) {
	b.mu.Lock("AddServerlessCacheInternal")
	defer b.mu.Unlock()
	b.serverlessCachesStore(b.region)[sc.Name] = sc
}

// AddServerlessCacheSnapshotInternal seeds a serverless cache snapshot for testing.
func (b *InMemoryBackend) AddServerlessCacheSnapshotInternal(snap *ServerlessCacheSnapshot) {
	b.mu.Lock("AddServerlessCacheSnapshotInternal")
	defer b.mu.Unlock()
	b.serverlessCacheSnapshotsStore(b.region)[snap.Name] = snap
}

// AddUserInternal seeds a user for testing.
func (b *InMemoryBackend) AddUserInternal(u *User) {
	b.mu.Lock("AddUserInternal")
	defer b.mu.Unlock()
	b.usersStore(b.region)[u.UserID] = u
}
