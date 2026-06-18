package elasticache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// ----------------------------------------
// Valkey engine constants (gap #10)
// ----------------------------------------

const (
	engineValkey    = "valkey"
	familyValkey8   = "valkey8"
	familyValkey7   = "valkey7"
	versionValkey82 = "8.2.0"
)

// Transit encryption mode constants (gap #13).
const (
	transitEncryptionModePreferred = "preferred"
	transitEncryptionModeRequired  = "required"
)

// statusEnabled is the AWS "enabled" state string.
const statusEnabled = "enabled"

// engineValkeyCap is the display-name capitalisation for Valkey.
const engineValkeyCap = "Valkey"

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

// ----------------------------------------
// Audit-1 errors
// ----------------------------------------

var (
	ErrClusterModeRequired          = errors.New("cluster mode must be enabled for shard configuration changes")
	ErrDataTieringInvalid           = errors.New("data tiering requires r7g node type and Redis/Valkey 7.0+")
	ErrTransitEncryptionModeInvalid = errors.New("transit encryption mode 'required' requires an auth token")
	ErrAuthTokenRequiredForMode     = errors.New(
		"auth token must be provided when transit encryption mode is 'required'",
	)
)

// ----------------------------------------
// New model types (gaps #1–#15)
// ----------------------------------------

// CacheNodeMember represents a Memcached cluster node member (gap #3).
type CacheNodeMember struct {
	CacheClusterID            string    `json:"cacheClusterId"`
	CacheNodeID               string    `json:"cacheNodeId"`
	CacheNodeStatus           string    `json:"cacheNodeStatus"`
	PreferredAvailabilityZone string    `json:"preferredAvailabilityZone"`
	CacheNodeCreateTime       time.Time `json:"cacheNodeCreateTime"`
	Endpoint                  struct {
		Address string `json:"address"`
		Port    int    `json:"port"`
	} `json:"endpoint"`
}

// NodeGroupNode represents a single node within a node group (gap #2).
type NodeGroupNode struct {
	ReadEndpointAddress       string `json:"readEndpointAddress,omitempty"`
	CacheClusterID            string `json:"cacheClusterId"`
	CacheNodeID               string `json:"cacheNodeId"`
	CurrentRole               string `json:"currentRole"` // "primary" or "replica"
	PreferredAvailabilityZone string `json:"preferredAvailabilityZone"`
	ReadEndpointPort          int    `json:"readEndpointPort,omitempty"`
}

// NodeGroup represents a shard / node group in a cluster-mode-enabled replication group (gap #2).
type NodeGroup struct {
	PrimaryNode *NodeGroupNode  `json:"primaryNode,omitempty"`
	NodeGroupID string          `json:"nodeGroupId"`
	Status      string          `json:"status"`
	Slots       string          `json:"slots"`
	Replicas    []NodeGroupNode `json:"replicas,omitempty"`
}

// RGPendingModifiedValues holds modifications queued for the next maintenance window (gap #7).
type RGPendingModifiedValues struct {
	ReplicaCount            *int32 `json:"replicaCount,omitempty"`
	CacheNodeType           string `json:"cacheNodeType,omitempty"`
	EngineVersion           string `json:"engineVersion,omitempty"`
	AuthTokenStatus         string `json:"authTokenStatus,omitempty"`
	AutomaticFailoverStatus string `json:"automaticFailoverStatus,omitempty"`
}

// LogDeliveryConfig holds log delivery configuration for slow-log or engine-log (gap #6).
type LogDeliveryConfig struct {
	DestinationDetails string `json:"destinationDetails"`
	LogType            string `json:"logType"`         // "slow-log" or "engine-log"
	DestinationType    string `json:"destinationType"` // "cloudwatch-logs" or "kinesis-firehose"
	LogFormat          string `json:"logFormat"`       // "text" or "json"
	Status             string `json:"status"`
	Message            string `json:"message,omitempty"`
}

// ----------------------------------------
// CreateReplicationGroupFull options (all gaps)
// ----------------------------------------

// ReplicationGroupCreateOpts carries all fields for full replication-group creation.
type ReplicationGroupCreateOpts struct {
	Tags                      map[string]string
	Engine                    string
	EngineVersion             string
	ID                        string
	Description               string
	ParameterGroupName        string
	MaintenanceWindow         string
	TransitEncryptionMode     string
	AuthToken                 string
	KmsKeyID                  string
	NotificationTopicArn      string
	CacheNodeType             string
	SnapshotWindow            string
	UserGroupIDs              []string
	LogDeliveryConfigurations []LogDeliveryConfig
	SnapshotRetentionLimit    int
	ReplicasPerNodeGroup      int32
	NumNodeGroups             int32
	ClusterModeEnabled        bool
	AuthTokenEnabled          bool
	AtRestEncryptionEnabled   bool
	TransitEncryptionEnabled  bool
	DataTieringEnabled        bool
	AutomaticFailoverEnabled  bool
	MultiAZEnabled            bool
}

// ReplicationGroupModifyOpts carries all fields for full replication-group modification.
type ReplicationGroupModifyOpts struct {
	SnapshotRetentionLimit    *int
	ReplicaCount              *int32
	AutomaticFailoverEnabled  *bool
	MultiAZEnabled            *bool
	Description               string
	ParameterGroupName        string
	EngineVersion             string
	CacheNodeType             string
	MaintenanceWindow         string
	SnapshotWindow            string
	AuthToken                 string
	AuthTokenUpdateStrategy   string
	NotificationTopicArn      string
	TransitEncryptionMode     string
	LogDeliveryConfigurations []LogDeliveryConfig
	UserGroupIDsToAdd         []string
	UserGroupIDsToRemove      []string
	ApplyImmediately          bool
}

// ----------------------------------------
// resizeNodeGroups helper (gap #2)
// ----------------------------------------

// resizeNodeGroups resizes a node-group slice to targetCount, preserving existing groups
// and adding stub groups as needed. replicaCount controls the replica stub count.
func resizeNodeGroups(existing []NodeGroup, targetCount, replicaCount int) []NodeGroup {
	if targetCount <= 0 {
		return existing
	}

	if len(existing) == targetCount {
		return existing
	}

	out := make([]NodeGroup, targetCount)
	copy(out, existing)

	slotSize := redisClusterHashSlots / targetCount
	for i := len(existing); i < targetCount; i++ {
		slotStart := i * slotSize
		slotEnd := slotStart + slotSize - 1
		if i == targetCount-1 {
			slotEnd = redisClusterHashSlots - 1
		}

		ng := NodeGroup{
			NodeGroupID: fmt.Sprintf("%04d", i+1),
			Status:      statusAvailable,
			Slots:       fmt.Sprintf("%d-%d", slotStart, slotEnd),
		}
		ng.Replicas = make([]NodeGroupNode, replicaCount)
		out[i] = ng
	}

	return out
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

// CreateReplicationGroupFull creates a replication group with the full set of options.
func (b *InMemoryBackend) CreateReplicationGroupFull(
	ctx context.Context,
	opts ReplicationGroupCreateOpts,
) (*ReplicationGroup, error) {
	b.mu.Lock("CreateReplicationGroupFull")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	rgStore := b.replicationGroupsStore(region)

	if _, exists := rgStore[opts.ID]; exists {
		return nil, ErrReplicationGroupAlreadyExists
	}

	if opts.ParameterGroupName != "" {
		if _, ok := b.parameterGroupsStore(region)[opts.ParameterGroupName]; !ok {
			return nil, ErrParameterGroupNotFound
		}
	}

	if err := validateCreateOpts(opts); err != nil {
		return nil, err
	}

	rg := b.buildReplicationGroupFromCreateOpts(region, opts)
	rgStore[opts.ID] = rg
	b.appendEventLocked(opts.ID, "replication-group", "replication group created")

	cp := *rg

	return &cp, nil
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
	}

	applyAuthToken(rg, opts.AuthToken, opts.AuthTokenEnabled)

	if opts.AutomaticFailoverEnabled {
		rg.AutomaticFailover = statusEnabled
	}

	if opts.Engine != "" {
		rg.Engine = opts.Engine
	}

	if opts.EngineVersion != "" {
		rg.EngineVersion = opts.EngineVersion
	}

	if opts.CacheNodeType != "" {
		rg.CacheNodeType = opts.CacheNodeType
	}

	if opts.NumNodeGroups > 0 {
		rg.NodeGroups = resizeNodeGroups(nil, int(opts.NumNodeGroups), int(opts.ReplicasPerNodeGroup))
	}

	if opts.ReplicasPerNodeGroup > 0 {
		rg.ReplicaCount = opts.ReplicasPerNodeGroup
	}

	if len(opts.UserGroupIDs) > 0 {
		rg.UserGroupIDs = opts.UserGroupIDs
	}

	if len(opts.Tags) > 0 {
		for k, v := range opts.Tags {
			rg.Tags.Set(k, v)
		}
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
	rg, exists := b.replicationGroupsStore(region)[id]
	if !exists {
		return nil, ErrReplicationGroupNotFound
	}

	if opts.ParameterGroupName != "" {
		if _, ok := b.parameterGroupsStore(region)[opts.ParameterGroupName]; !ok {
			return nil, ErrParameterGroupNotFound
		}
	}

	b.applyModifyOptsLocked(rg, opts)
	b.appendEventLocked(id, "replication-group", "replication group modified")

	cp := *rg

	return &cp, nil
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

	applyUserGroupIDsModify(rg, opts.UserGroupIDsToAdd, opts.UserGroupIDsToRemove)
	applyAuthTokenModify(rg, opts.AuthToken, opts.AuthTokenUpdateStrategy)
	applyTransitEncryptionModify(rg, opts.TransitEncryptionMode)
	applyPendingChanges(rg, opts)
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
	case "DELETE":
		rg.AuthToken = ""
		rg.AuthTokenEnabled = false
		rg.AuthTokenLastModifiedDate = nil
	case "SET":
		if token == "" {
			token = generateAuthToken()
		}
		rg.AuthToken = token
		rg.AuthTokenEnabled = true
		now := time.Now()
		rg.AuthTokenLastModifiedDate = &now
	case "ROTATE":
		rg.AuthToken = generateAuthToken()
		now := time.Now()
		rg.AuthTokenLastModifiedDate = &now
	}
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
	rg, ok := b.replicationGroupsStore(region)[replicationGroupID]
	if !ok {
		return nil, ErrReplicationGroupNotFound
	}

	snapStore := b.snapshotsStore(region)
	snapName := buildAutoSnapshotName(replicationGroupID)
	if _, exists := snapStore[snapName]; exists {
		return nil, ErrSnapshotAlreadyExists
	}

	snap := buildAutoSnapshot(b, region, snapName, rg)
	snapStore[snapName] = snap

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

	return &CacheSnapshot{
		SnapshotName:       snapName,
		ReplicationGroupID: rg.ReplicationGroupID,
		Status:             statusAvailable,
		ARN:                b.snapshotARN(region, snapName),
		SnapshotSource:     "automated",
		Engine:             engineRedis,
		EngineVersion:      ev,
		NodeType:           rg.CacheNodeType,
		CreatedAt:          time.Now(),
		Tags:               tags.New("elasticache.snapshot." + snapName + ".tags"),
	}
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
	store map[string]*CacheSnapshot,
	replicationGroupID string,
	retentionLimit int,
) {
	if retentionLimit <= 0 {
		return
	}

	var autoSnaps []CacheSnapshot
	for _, s := range store {
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
		if s, ok := store[snap.SnapshotName]; ok {
			s.Tags.Close()
			delete(store, snap.SnapshotName)
		}
	}
}
