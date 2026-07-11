package efs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// Package-local sentinels used as the inner error for wrapped error types.
// They are not exported; callers should match via the exported Err* vars.
var (
	errTokenIdentical = errors.New("creation token exists with identical parameters")
	errThrottled      = errors.New("too many requests")
)

const (
	statusAvailable = "available"
	statusCreating  = "creating"
	statusDeleting  = "deleting"
	statusDeleted   = "deleted"
	statusUpdating  = "updating"
)

const (
	protectionEnabled     = "ENABLED"
	protectionDisabled    = "DISABLED"
	protectionReplicating = "REPLICATING"
)

const (
	backupStatusEnabled  = "ENABLED"
	backupStatusEnabling = "ENABLING"
	backupStatusDisabled = "DISABLED"
)

const (
	managedKMSKeyARN = "arn:aws:kms:us-east-1:000000000000:key/mrk-00000000000000000000000000000000"

	maxTagsPerResource = 50
	maxTagKeyLen       = 128
	maxTagValueLen     = 256

	maxSecurityGroups = 5

	maxFileSystemPolicyBytes = 20 * 1024

	throughputCooldown = 24 * time.Hour

	maxCreationTokenLen        = 64
	maxReplicationDestinations = 1
)

func isValidTransitionToIA(v string) bool {
	switch v {
	case "AFTER_7_DAYS", "AFTER_14_DAYS", "AFTER_30_DAYS", "AFTER_60_DAYS",
		"AFTER_90_DAYS", "AFTER_180_DAYS", "AFTER_270_DAYS", "AFTER_365_DAYS", "NONE":
		return true
	default:
		return false
	}
}

func isValidTransitionToPrimary(v string) bool {
	return v == "AFTER_1_ACCESS"
}

func isValidTransitionToArchive(v string) bool {
	switch v {
	case "AFTER_1_ACCESS", "AFTER_7_DAYS", "AFTER_14_DAYS", "AFTER_30_DAYS",
		"AFTER_60_DAYS", "AFTER_90_DAYS", "AFTER_180_DAYS", "AFTER_270_DAYS",
		"AFTER_365_DAYS", "AFTER_90_DAYS_1":
		return true
	default:
		return false
	}
}

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("FileSystemNotFound", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource with the same token already exists but args differ.
	ErrAlreadyExists = awserr.New("FileSystemAlreadyExists", awserr.ErrConflict)
	// ErrCreationTokenExists is returned when the same creation token with identical args is reused.
	ErrCreationTokenExists = awserr.New("FileSystemAlreadyExists", errTokenIdentical)
	// ErrMountTargetNotFound is returned when a requested mount target does not exist.
	ErrMountTargetNotFound = awserr.New("MountTargetNotFound", awserr.ErrNotFound)
	// ErrAccessPointNotFound is returned when a requested access point does not exist.
	ErrAccessPointNotFound = awserr.New("AccessPointNotFound", awserr.ErrNotFound)
	// ErrPolicyNotFound is returned when no resource policy is configured for a file system.
	ErrPolicyNotFound = awserr.New("PolicyNotFound", awserr.ErrNotFound)
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	// ErrFileSystemInUse is returned when attempting to delete a file system that has mount targets.
	ErrFileSystemInUse = awserr.New("FileSystemInUse", awserr.ErrConflict)
	// ErrMountTargetConflict is returned when a duplicate mount target is created in the same subnet.
	ErrMountTargetConflict = awserr.New("MountTargetConflict", awserr.ErrConflict)
	// ErrSecurityGroupLimitExceeded is returned when too many security groups are specified.
	ErrSecurityGroupLimitExceeded = awserr.New("SecurityGroupLimitExceeded", awserr.ErrConflict)
	// ErrTooManyRequests is returned when a throughput change cooldown is violated.
	ErrTooManyRequests = awserr.New("TooManyRequests", errThrottled)
)

const (
	throughputModeBursting    = "bursting"
	throughputModeProvisioned = "provisioned"
	throughputModeElastic     = "elastic"
	performanceModeGeneral    = "generalPurpose"
	performanceModeMaxIO      = "maxIO"
)

// PosixUser represents the POSIX identity used for all file system operations
// by NFS clients using the access point.
type PosixUser struct {
	SecondaryGids []int64 `json:"SecondaryGids,omitempty"`
	UID           int64   `json:"Uid"`
	GID           int64   `json:"Gid"`
}

// CreationInfo specifies the POSIX IDs and permissions to apply to the access
// point's RootDirectory when it does not exist.
type CreationInfo struct {
	Permissions string `json:"Permissions"`
	OwnerUID    int64  `json:"OwnerUid"`
	OwnerGID    int64  `json:"OwnerGid"`
}

// RootDirectory specifies the directory on the Amazon EFS file system that the
// access point provides access to.
type RootDirectory struct {
	CreationInfo *CreationInfo `json:"CreationInfo,omitempty"`
	Path         string        `json:"Path,omitempty"`
}

// FileSystem represents an EFS file system.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via TagResource / CreateFileSystem.
type FileSystem struct {
	CreationTime                   time.Time  `json:"creationTime"`
	Tags                           *tags.Tags `json:"tags,omitempty"`
	LastThroughputChange           time.Time  `json:"lastThroughputChange,omitzero"`
	PerformanceMode                string     `json:"performanceMode"`
	FileSystemArn                  string     `json:"fileSystemArn"`
	CreationToken                  string     `json:"creationToken"`
	Name                           string     `json:"name,omitempty"`
	FileSystemID                   string     `json:"fileSystemId"`
	ThroughputMode                 string     `json:"throughputMode"`
	LifeCycleState                 string     `json:"lifeCycleState"`
	AccountID                      string     `json:"accountId"`
	Region                         string     `json:"region"`
	KmsKeyID                       string     `json:"kmsKeyId,omitempty"`
	AvailabilityZoneName           string     `json:"availabilityZoneName,omitempty"`
	AvailabilityZoneID             string     `json:"availabilityZoneId,omitempty"`
	ReplicationOverwriteProtection string     `json:"replicationOverwriteProtection,omitempty"`
	ProvisionedThroughputMib       float64    `json:"provisionedThroughputMib,omitempty"`
	NumberOfMountTargets           int32      `json:"numberOfMountTargets"`
	Encrypted                      bool       `json:"encrypted"`
}

// MountTarget represents an EFS mount target.
type MountTarget struct {
	// region is the AWS region this mount target belongs to. It is the outer
	// half of the composite key ("region|MountTargetID") used by the
	// backend's flat store.Table[MountTarget] (see store_setup.go), which
	// replaces the old map[string]map[string]*MountTarget nesting (outer key
	// = region). Unexported so it never appears in EFS wire responses;
	// persistence.go carries it through a DTO explicitly since json.Marshal
	// never sees unexported fields.
	region               string
	MountTargetID        string   `json:"mountTargetId"`
	MountTargetArn       string   `json:"mountTargetArn"`
	FileSystemID         string   `json:"fileSystemId"`
	SubnetID             string   `json:"subnetId"`
	VpcID                string   `json:"vpcId"`
	AvailabilityZoneName string   `json:"availabilityZoneName"`
	AvailabilityZoneID   string   `json:"availabilityZoneId"`
	NetworkInterfaceID   string   `json:"networkInterfaceId"`
	IPAddress            string   `json:"ipAddress"`
	LifeCycleState       string   `json:"lifeCycleState"`
	OwnerID              string   `json:"ownerId"`
	SecurityGroups       []string `json:"securityGroups,omitempty"`
}

// AccessPoint represents an EFS access point.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via TagResource.
type AccessPoint struct {
	// region is the AWS region this access point belongs to. It is the outer
	// half of the composite key ("region|AccessPointID") used by the
	// backend's flat store.Table[AccessPoint] (see store_setup.go), which
	// replaces the old map[string]map[string]*AccessPoint nesting (outer key
	// = region). Unexported so it never appears in EFS wire responses;
	// persistence.go carries it through a DTO explicitly since json.Marshal
	// never sees unexported fields.
	region         string
	AccessPointID  string         `json:"accessPointId"`
	AccessPointArn string         `json:"accessPointArn"`
	FileSystemID   string         `json:"fileSystemId"`
	ClientToken    string         `json:"clientToken,omitempty"`
	Name           string         `json:"name,omitempty"`
	LifeCycleState string         `json:"lifeCycleState"`
	Tags           *tags.Tags     `json:"tags,omitempty"`
	PosixUser      *PosixUser     `json:"posixUser,omitempty"`
	RootDirectory  *RootDirectory `json:"rootDirectory,omitempty"`
	OwnerID        string         `json:"ownerId"`
}

// ReplicationDestination represents a destination in an EFS replication configuration.
type ReplicationDestination struct {
	FileSystemID            string `json:"FileSystemId,omitempty"`
	FileSystemArn           string `json:"FileSystemArn,omitempty"`
	Region                  string `json:"Region,omitempty"`
	AvailabilityZoneName    string `json:"AvailabilityZoneName,omitempty"`
	KmsKeyID                string `json:"KmsKeyID,omitempty"`
	OwnerID                 string `json:"OwnerId,omitempty"`
	LastReplicatedTimestamp string `json:"LastReplicatedTimestamp,omitempty"`
	Status                  string `json:"Status,omitempty"`
}

// ReplicationConfiguration represents an EFS replication configuration.
type ReplicationConfiguration struct {
	OriginalSourceFileSystemARN string                   `json:"OriginalSourceFileSystemArn"`
	SourceFileSystemARN         string                   `json:"SourceFileSystemArn"`
	SourceFileSystemID          string                   `json:"SourceFileSystemId"`
	SourceFileSystemRegion      string                   `json:"SourceFileSystemRegion"`
	SourceFileSystemOwnerID     string                   `json:"SourceFileSystemOwnerId"`
	Destinations                []ReplicationDestination `json:"Destinations"`
	CreationTime                int64                    `json:"CreationTime"`
}

// AccountPreferences represents EFS account preferences.
type AccountPreferences struct {
	ResourceIDType string `json:"ResourceIdType"`
}

// UpdateFileSystemRequest holds parameters for updating an EFS file system.
type UpdateFileSystemRequest struct {
	ThroughputMode           string  `json:"ThroughputMode,omitempty"`
	ProvisionedThroughputMib float64 `json:"ProvisionedThroughputInMibps,omitempty"`
}

// CreateFileSystemRequest holds parameters for creating an EFS file system.
type CreateFileSystemRequest struct {
	Tags                     map[string]string
	CreationToken            string
	PerformanceMode          string
	ThroughputMode           string
	KmsKeyID                 string
	AvailabilityZoneName     string
	ProvisionedThroughputMib float64
	Encrypted                bool
}

// CreateMountTargetRequest holds parameters for creating an EFS mount target.
type CreateMountTargetRequest struct {
	FileSystemID   string
	SubnetID       string
	IPAddress      string
	SecurityGroups []string
}

// CreateAccessPointRequest holds parameters for creating an EFS access point.
type CreateAccessPointRequest struct {
	Tags          map[string]string
	PosixUser     *PosixUser
	RootDirectory *RootDirectory
	FileSystemID  string
	ClientToken   string
}

// InMemoryBackend is the in-memory store for EFS resources.
//
// The four resource collections below (fileSystems, mountTargets,
// accessPoints, replicationConfigs) were previously nested by region (outer
// key = region) so that same-named resources in different regions were fully
// isolated. Phase 3.3 flattens each into a single *store.Table[T] keyed by
// the composite "region|id" string (see regionKey in store_setup.go), with a
// companion *store.Index grouping entries by region for the old per-region
// scans, and -- for fileSystemsByARN/mountTargetsByARN/accessPointsByARN/
// accessPointsByClientToken -- an unregistered derived-cache *store.Table
// (same pattern as services/ecs's taskDefByArn) providing O(1) region-scoped
// lookup by the resource's own ARN/ClientToken. accountPreferences is
// account-level state in AWS and so is not region-nested.
type InMemoryBackend struct {
	// registry is the Phase 3.3 datalayer lifecycle registry: every
	// *store.Table below except the four ARN/client-token derived caches
	// (which are rebuilt from the registered tables, not independently
	// persisted -- see store_setup.go) is registered on it exactly once at
	// construction, so Reset/Snapshot/Restore collapse to one registry call
	// each instead of one hand-written block per map.
	registry                   *store.Registry
	fileSystems                *store.Table[FileSystem]
	fileSystemsByRegion        *store.Index[FileSystem]
	fileSystemsByARN           *store.Table[FileSystem]
	mountTargets               *store.Table[MountTarget]
	mountTargetsByRegion       *store.Index[MountTarget]
	mountTargetsByARN          *store.Table[MountTarget]
	accessPoints               *store.Table[AccessPoint]
	accessPointsByRegion       *store.Index[AccessPoint]
	accessPointsByARN          *store.Table[AccessPoint]
	accessPointsByClientToken  *store.Table[AccessPoint]
	replicationConfigs         *store.Table[ReplicationConfiguration]
	replicationConfigsByRegion *store.Index[ReplicationConfiguration]

	// lifecyclePolicies, backupPolicies, and fileSystemPolicies are
	// deliberately NOT store.Table-converted: their value types
	// ([]LifecyclePolicy / string) carry no identity of their own, so
	// store.Table (which wraps map[string]*V) is out of scope for them. They
	// remain plain region-nested maps, persisted directly (see
	// persistence.go).
	lifecyclePolicies  map[string]map[string][]LifecyclePolicy
	backupPolicies     map[string]map[string]string
	fileSystemPolicies map[string]map[string]string

	// creationTokenIdx, mtSubnetIdx, and apByFS are performance indexes
	// (avoid O(n) scans on hot paths) whose values are plain strings/
	// struct{}, not *T, so they too are out of store.Table's scope. They are
	// never persisted; Restore rebuilds them from the restored resource
	// tables (see rebuildDerivedIndexes in persistence.go).
	creationTokenIdx map[string]map[string]string              // region → creationToken → fsID
	mtSubnetIdx      map[string]map[string]map[string]string   // region → fsID → subnetID → mtID
	apByFS           map[string]map[string]map[string]struct{} // region → fsID → apID → {}

	accountPreferences AccountPreferences
	mu                 *lockmetrics.RWMutex
	accountID          string
	region             string
	// fsActivationDelay controls how long CreateFileSystem waits before transitioning
	// a file system from "creating" to "available". Zero (default) means the transition
	// is synchronous and immediate, matching legacy behaviour. A non-zero value enables
	// the AWS-accurate lifecycle simulation and is only set in parity tests.
	fsActivationDelay time.Duration
}

// LifecyclePolicy represents an EFS lifecycle management policy.
type LifecyclePolicy struct {
	TransitionToIA                  string `json:"TransitionToIA,omitempty"`
	TransitionToPrimaryStorageClass string `json:"TransitionToPrimaryStorageClass,omitempty"`
	TransitionToArchive             string `json:"TransitionToArchive,omitempty"`
}

// NewInMemoryBackend creates a new in-memory EFS backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		accountPreferences: AccountPreferences{ResourceIDType: "LONG_ID"},
		accountID:          accountID,
		region:             region,
		mu:                 lockmetrics.New("efs"),
		registry:           store.NewRegistry(),
	}

	registerAllTables(b)
	b.initRegionMaps()

	return b
}

// initRegionMaps (re)allocates the (empty) auxiliary maps that were not
// converted to store.Table -- see the doc comments on InMemoryBackend's
// lifecyclePolicies/backupPolicies/fileSystemPolicies/creationTokenIdx/
// mtSubnetIdx/apByFS fields.
func (b *InMemoryBackend) initRegionMaps() {
	b.lifecyclePolicies = make(map[string]map[string][]LifecyclePolicy)
	b.backupPolicies = make(map[string]map[string]string)
	b.fileSystemPolicies = make(map[string]map[string]string)
	b.creationTokenIdx = make(map[string]map[string]string)
	b.mtSubnetIdx = make(map[string]map[string]map[string]string)
	b.apByFS = make(map[string]map[string]map[string]struct{})
}

// The following per-region store helpers return the inner map for region,
// lazily creating it on first access. Callers must hold b.mu.

func (b *InMemoryBackend) lifecycleStore(region string) map[string][]LifecyclePolicy {
	if b.lifecyclePolicies[region] == nil {
		b.lifecyclePolicies[region] = make(map[string][]LifecyclePolicy)
	}

	return b.lifecyclePolicies[region]
}

func (b *InMemoryBackend) backupStore(region string) map[string]string {
	if b.backupPolicies[region] == nil {
		b.backupPolicies[region] = make(map[string]string)
	}

	return b.backupPolicies[region]
}

func (b *InMemoryBackend) fsPolicyStore(region string) map[string]string {
	if b.fileSystemPolicies[region] == nil {
		b.fileSystemPolicies[region] = make(map[string]string)
	}

	return b.fileSystemPolicies[region]
}

func (b *InMemoryBackend) tokenIdxStore(region string) map[string]string {
	if b.creationTokenIdx[region] == nil {
		b.creationTokenIdx[region] = make(map[string]string)
	}

	return b.creationTokenIdx[region]
}

func (b *InMemoryBackend) mtSubnetStore(region, fsID string) map[string]string {
	if b.mtSubnetIdx[region] == nil {
		b.mtSubnetIdx[region] = make(map[string]map[string]string)
	}

	if b.mtSubnetIdx[region][fsID] == nil {
		b.mtSubnetIdx[region][fsID] = make(map[string]string)
	}

	return b.mtSubnetIdx[region][fsID]
}

func (b *InMemoryBackend) apFSStore(region, fsID string) map[string]struct{} {
	if b.apByFS[region] == nil {
		b.apByFS[region] = make(map[string]map[string]struct{})
	}

	if b.apByFS[region][fsID] == nil {
		b.apByFS[region][fsID] = make(map[string]struct{})
	}

	return b.apByFS[region][fsID]
}

// subnetDerivedVpcID returns a stable synthetic VpcID derived from a subnet ID.
// AWS derives VpcId from the subnet; since the mock has no VPC backend, we synthesise
// it deterministically: "subnet-XXXXXXXX" → "vpc-XXXXXXXX", anything else → "vpc-00000000".
func subnetDerivedVpcID(subnetID string) string {
	const prefix = "subnet-"
	if strings.HasPrefix(subnetID, prefix) {
		return "vpc-" + subnetID[len(prefix):]
	}

	return "vpc-00000000"
}

// mountTargetAZName returns the availability zone name for a new mount target.
// If the file system was pinned to a zone (One Zone storage class), that zone is used;
// otherwise the first zone in the region is returned as a default.
func mountTargetAZName(fs *FileSystem, region string) string {
	if fs.AvailabilityZoneName != "" {
		return fs.AvailabilityZoneName
	}

	return region + "a"
}

// azNameToID converts an AZ name like "us-east-1a" to an AZ ID like "use1-az1".
// The mapping is approximate but sufficient for mock parity.
func azNameToID(azName string) string {
	if azName == "" {
		return ""
	}
	// Strip the trailing zone letter (a=1, b=2, …) to build the ID suffix.
	letter := azName[len(azName)-1]
	suffix := int(letter-'a') + 1
	// Compress the region prefix: drop hyphens and digits except the last digit.
	// e.g. "us-east-1" → "use1", "us-west-2" → "usw2", "eu-west-1" → "euw1"
	regionPart := azName[:len(azName)-1] // strip trailing letter
	if idx := strings.LastIndex(regionPart, "-"); idx >= 0 {
		regionPart = strings.ReplaceAll(regionPart[:idx], "-", "") + regionPart[idx+1:]
	}

	return fmt.Sprintf("%s-az%d", regionPart, suffix)
}

// Reset clears all stored resources, returning the backend to its empty initial state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, fs := range b.fileSystems.All() {
		fs.Tags.Close()
	}
	for _, ap := range b.accessPoints.All() {
		ap.Tags.Close()
	}

	b.registry.ResetAll()
	b.fileSystemsByARN.Reset()
	b.mountTargetsByARN.Reset()
	b.accessPointsByARN.Reset()
	b.accessPointsByClientToken.Reset()

	b.initRegionMaps()
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// validateTags returns an error if any tag key/value violates AWS constraints.
func validateTags(kv map[string]string) error {
	if len(kv) > maxTagsPerResource {
		return fmt.Errorf(
			"%w: too many tags: %d (max %d)",
			ErrValidation,
			len(kv),
			maxTagsPerResource,
		)
	}

	for k, v := range kv {
		if len(k) == 0 || len(k) > maxTagKeyLen {
			return fmt.Errorf(
				"%w: tag key length must be 1-%d, got %d",
				ErrValidation,
				maxTagKeyLen,
				len(k),
			)
		}
		if strings.HasPrefix(k, "aws:") {
			return fmt.Errorf("%w: tag key must not start with 'aws:'", ErrValidation)
		}
		if len(v) > maxTagValueLen {
			return fmt.Errorf(
				"%w: tag value length must be 0-%d, got %d",
				ErrValidation,
				maxTagValueLen,
				len(v),
			)
		}
	}

	return nil
}

// CreateFileSystem creates a new EFS file system.
// validateCreateFSRequest validates and normalizes a CreateFileSystemRequest,
// returning the resolved KMS key ID on success.
// validateProvisionedThroughput checks provisioned throughput constraints.
func validateProvisionedThroughput(mode string, mib float64) error {
	if mode == throughputModeProvisioned {
		if mib < 1 || mib > 1024 {
			return fmt.Errorf(
				"%w: ProvisionedThroughputInMibps must be between 1 and 1024 when ThroughputMode is provisioned, got %g",
				ErrValidation,
				mib,
			)
		}
	} else if mib != 0 {
		return fmt.Errorf(
			"%w: ProvisionedThroughputInMibps is only valid when ThroughputMode is provisioned",
			ErrValidation,
		)
	}

	return nil
}

func validateCreateFSRequest(req *CreateFileSystemRequest) (string, error) {
	if len(req.CreationToken) > maxCreationTokenLen {
		return "", fmt.Errorf(
			"%w: CreationToken length must be 1-%d, got %d",
			ErrValidation,
			maxCreationTokenLen,
			len(req.CreationToken),
		)
	}

	if err := validateTags(req.Tags); err != nil {
		return "", err
	}

	if req.PerformanceMode == "" {
		req.PerformanceMode = performanceModeGeneral
	}
	if req.ThroughputMode == "" {
		req.ThroughputMode = throughputModeBursting
	}

	if req.PerformanceMode != performanceModeGeneral &&
		req.PerformanceMode != performanceModeMaxIO {
		return "", fmt.Errorf(
			"%w: invalid PerformanceMode %q, must be generalPurpose or maxIO",
			ErrValidation,
			req.PerformanceMode,
		)
	}
	if req.ThroughputMode != throughputModeBursting &&
		req.ThroughputMode != throughputModeProvisioned &&
		req.ThroughputMode != throughputModeElastic {
		return "", fmt.Errorf(
			"%w: invalid ThroughputMode %q, must be bursting, provisioned, or elastic",
			ErrValidation,
			req.ThroughputMode,
		)
	}

	if err := validateProvisionedThroughput(req.ThroughputMode, req.ProvisionedThroughputMib); err != nil {
		return "", err
	}

	kmsKeyID := req.KmsKeyID
	if req.Encrypted && kmsKeyID == "" {
		kmsKeyID = managedKMSKeyARN
	}
	if !req.Encrypted && kmsKeyID != "" {
		return "", fmt.Errorf(
			"%w: KmsKeyID can only be specified when Encrypted is true",
			ErrValidation,
		)
	}

	return kmsKeyID, nil
}

func (b *InMemoryBackend) CreateFileSystem(
	ctx context.Context,
	req CreateFileSystemRequest,
) (*FileSystem, error) {
	kmsKeyID, err := validateCreateFSRequest(&req)
	if err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateFileSystem")
	defer b.mu.Unlock()

	tokenIdx := b.tokenIdxStore(region)

	// O(1) idempotency check via creation-token index.
	if existingID, ok := tokenIdx[req.CreationToken]; ok {
		fs, _ := b.fileSystems.Get(regionKey(region, existingID))
		cp := *fs

		if fs.PerformanceMode == req.PerformanceMode &&
			fs.ThroughputMode == req.ThroughputMode &&
			fs.Encrypted == req.Encrypted &&
			fs.KmsKeyID == req.KmsKeyID &&
			fs.AvailabilityZoneName == req.AvailabilityZoneName {
			return &cp, fmt.Errorf(
				"%w: file system with token %s already exists (identical args)",
				ErrCreationTokenExists,
				req.CreationToken,
			)
		}

		return &cp, fmt.Errorf(
			"%w: file system with token %s already exists with different parameters (FileSystemId: %s)",
			ErrAlreadyExists,
			req.CreationToken,
			fs.FileSystemID,
		)
	}

	id := "fs-" + uuid.NewString()[:8]
	fsARN := arn.Build("elasticfilesystem", region, b.accountID, "file-system/"+id)
	t := tags.New("efs.filesystem." + id + ".tags")

	tagCopy := make(map[string]string, len(req.Tags))
	maps.Copy(tagCopy, req.Tags)

	if len(tagCopy) > 0 {
		t.Merge(tagCopy)
	}

	name := req.Tags["Name"]

	initialState := statusAvailable
	if b.fsActivationDelay > 0 {
		initialState = statusCreating
	}

	fs := &FileSystem{
		FileSystemID:                   id,
		FileSystemArn:                  fsARN,
		CreationToken:                  req.CreationToken,
		Name:                           name,
		PerformanceMode:                req.PerformanceMode,
		ThroughputMode:                 req.ThroughputMode,
		LifeCycleState:                 initialState,
		Encrypted:                      req.Encrypted,
		KmsKeyID:                       kmsKeyID,
		AvailabilityZoneName:           req.AvailabilityZoneName,
		ProvisionedThroughputMib:       req.ProvisionedThroughputMib,
		ReplicationOverwriteProtection: protectionDisabled,
		AccountID:                      b.accountID,
		Region:                         region,
		CreationTime:                   time.Now().UTC(),
		Tags:                           t,
	}
	b.fileSystems.Put(fs)
	b.fileSystemsByARN.Put(fs)
	tokenIdx[req.CreationToken] = id

	// When a non-zero activation delay is configured, simulate the AWS
	// "creating" → "available" lifecycle transition asynchronously.
	// The goroutine is self-terminating and guards against concurrent deletion.
	if b.fsActivationDelay > 0 {
		delay := b.fsActivationDelay

		go func() {
			time.Sleep(delay)
			b.mu.Lock("CreateFileSystem.activate")
			defer b.mu.Unlock()
			if cur, ok := b.fileSystems.Get(regionKey(region, id)); ok && cur.LifeCycleState == statusCreating {
				cur.LifeCycleState = statusAvailable
			}
		}()
	}

	cp := *fs

	return &cp, nil
}

// DescribeFileSystems returns file systems, optionally filtered by ID or creation token, with pagination support.
func (b *InMemoryBackend) DescribeFileSystems(
	ctx context.Context,
	fileSystemID, creationToken, marker string,
	maxItems int,
) ([]*FileSystem, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeFileSystems")
	defer b.mu.RUnlock()

	if fileSystemID != "" {
		fs, ok := b.fileSystems.Get(regionKey(region, fileSystemID))
		if !ok {
			return nil, "", fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
		}
		cp := *fs

		return []*FileSystem{&cp}, "", nil
	}

	regionFS := b.fileSystemsByRegion.Get(region)

	if creationToken != "" {
		for _, fs := range regionFS {
			if fs.CreationToken == creationToken {
				cp := *fs

				return []*FileSystem{&cp}, "", nil
			}
		}

		return []*FileSystem{}, "", nil
	}

	all := make([]*FileSystem, 0, len(regionFS))
	for _, fs := range regionFS {
		cp := *fs
		all = append(all, &cp)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].FileSystemID < all[j].FileSystemID })

	return paginate(all, marker, maxItems, func(fs *FileSystem) string { return fs.FileSystemID })
}

// DeleteFileSystem deletes a file system by ID.
// Returns ErrFileSystemInUse if any mount targets exist.
func (b *InMemoryBackend) DeleteFileSystem(ctx context.Context, fileSystemID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteFileSystem")
	defer b.mu.Unlock()

	fs, ok := b.fileSystems.Get(regionKey(region, fileSystemID))
	if !ok {
		return fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	// O(1) conflict check via indexes: reject delete if mount targets or access points exist.
	if b.mtSubnetIdx[region] != nil && len(b.mtSubnetIdx[region][fileSystemID]) > 0 {
		return fmt.Errorf(
			"%w: file system %s has existing mount targets",
			ErrFileSystemInUse,
			fileSystemID,
		)
	}

	if b.apByFS[region] != nil && len(b.apByFS[region][fileSystemID]) > 0 {
		return fmt.Errorf(
			"%w: file system %s has existing access points",
			ErrFileSystemInUse,
			fileSystemID,
		)
	}

	b.fileSystemsByARN.Delete(regionKey(region, fs.FileSystemArn))
	// Remove from creation-token index so the token can be reused.
	if b.creationTokenIdx[region] != nil {
		delete(b.creationTokenIdx[region], fs.CreationToken)
	}

	fs.Tags.Close()
	b.fileSystems.Delete(regionKey(region, fileSystemID))
	delete(b.lifecycleStore(region), fileSystemID)
	delete(b.backupStore(region), fileSystemID)
	delete(b.fsPolicyStore(region), fileSystemID)
	b.replicationConfigs.Delete(regionKey(region, fileSystemID))

	return nil
}

// TagResource adds or updates tags on a resource (file system or access point) by ARN or ID.
func (b *InMemoryBackend) TagResource(ctx context.Context, resourceID string, kv map[string]string) error {
	if err := validateTags(kv); err != nil {
		return err
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if fs, ok := b.fileSystems.Get(regionKey(region, resourceID)); ok {
		fs.Tags.Merge(kv)

		return nil
	}
	if fs, ok := b.fileSystemsByARN.Get(regionKey(region, resourceID)); ok {
		fs.Tags.Merge(kv)

		return nil
	}

	if ap, ok := b.accessPoints.Get(regionKey(region, resourceID)); ok {
		ap.Tags.Merge(kv)

		return nil
	}
	if ap, ok := b.accessPointsByARN.Get(regionKey(region, resourceID)); ok {
		ap.Tags.Merge(kv)

		return nil
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceID)
}

// UntagResource removes tags from a resource (file system or access point) by ARN or ID.
func (b *InMemoryBackend) UntagResource(ctx context.Context, resourceID string, tagKeys []string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if fs, ok := b.fileSystems.Get(regionKey(region, resourceID)); ok {
		fs.Tags.DeleteKeys(tagKeys)

		return nil
	}
	if fs, ok := b.fileSystemsByARN.Get(regionKey(region, resourceID)); ok {
		fs.Tags.DeleteKeys(tagKeys)

		return nil
	}

	if ap, ok := b.accessPoints.Get(regionKey(region, resourceID)); ok {
		ap.Tags.DeleteKeys(tagKeys)

		return nil
	}
	if ap, ok := b.accessPointsByARN.Get(regionKey(region, resourceID)); ok {
		ap.Tags.DeleteKeys(tagKeys)

		return nil
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceID)
}

// ListTagsForResource lists tags for a resource by ID or ARN.
func (b *InMemoryBackend) ListTagsForResource(
	ctx context.Context,
	resourceID string,
) (map[string]string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if fs, ok := b.fileSystems.Get(regionKey(region, resourceID)); ok {
		return fs.Tags.Clone(), nil
	}
	if fs, ok := b.fileSystemsByARN.Get(regionKey(region, resourceID)); ok {
		return fs.Tags.Clone(), nil
	}

	if ap, ok := b.accessPoints.Get(regionKey(region, resourceID)); ok {
		return ap.Tags.Clone(), nil
	}
	if ap, ok := b.accessPointsByARN.Get(regionKey(region, resourceID)); ok {
		return ap.Tags.Clone(), nil
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceID)
}

// CreateMountTarget creates a mount target for a file system.
// Returns ErrMountTargetConflict if a mount target already exists in the same subnet.
func (b *InMemoryBackend) CreateMountTarget(
	ctx context.Context,
	req CreateMountTargetRequest,
) (*MountTarget, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateMountTarget")
	defer b.mu.Unlock()

	fs, ok := b.fileSystems.Get(regionKey(region, req.FileSystemID))
	if !ok {
		return nil, fmt.Errorf("%w: file system %s not found", ErrNotFound, req.FileSystemID)
	}

	// O(1) subnet conflict check via index: one mount target per subnet per file system.
	if req.SubnetID != "" {
		if b.mtSubnetIdx[region] != nil {
			if _, dup := b.mtSubnetIdx[region][req.FileSystemID][req.SubnetID]; dup {
				return nil, fmt.Errorf(
					"%w: mount target already exists for file system %s in subnet %s",
					ErrMountTargetConflict,
					req.FileSystemID,
					req.SubnetID,
				)
			}
		}
	}

	if len(req.SecurityGroups) > maxSecurityGroups {
		return nil, fmt.Errorf(
			"%w: too many security groups: %d (max %d)",
			ErrSecurityGroupLimitExceeded,
			len(req.SecurityGroups),
			maxSecurityGroups,
		)
	}

	id := "fsmt-" + uuid.NewString()[:8]
	mtARN := arn.Build("elasticfilesystem", region, b.accountID, "mount-target/"+id)
	eniID := "eni-" + uuid.NewString()[:8]

	sgs := make([]string, len(req.SecurityGroups))
	copy(sgs, req.SecurityGroups)

	// Synthesise VPC and AZ fields from the subnet ID and file system config.
	// AWS derives these from real VPC/subnet metadata; the mock approximates them
	// deterministically so callers receive non-empty, stable values.
	vpcID := subnetDerivedVpcID(req.SubnetID)
	azName := mountTargetAZName(fs, region)
	azID := azNameToID(azName)

	mt := &MountTarget{
		region:               region,
		MountTargetID:        id,
		MountTargetArn:       mtARN,
		FileSystemID:         req.FileSystemID,
		SubnetID:             req.SubnetID,
		VpcID:                vpcID,
		AvailabilityZoneName: azName,
		AvailabilityZoneID:   azID,
		IPAddress:            req.IPAddress,
		NetworkInterfaceID:   eniID,
		LifeCycleState:       statusAvailable,
		OwnerID:              b.accountID,
		SecurityGroups:       sgs,
	}
	b.mountTargets.Put(mt)
	b.mountTargetsByARN.Put(mt)
	// Update subnet index for O(1) conflict detection.
	b.mtSubnetStore(region, req.FileSystemID)[req.SubnetID] = id
	fs.NumberOfMountTargets++

	cp := *mt
	cp.SecurityGroups = make([]string, len(mt.SecurityGroups))
	copy(cp.SecurityGroups, mt.SecurityGroups)

	return &cp, nil
}

// describeByIDOrFilter is a generic helper for Describe* methods that look up
// a single item by ID via getByID, or filter allInRegion by file-system ID,
// then paginate.
func describeByIDOrFilter[T any](
	getByID func(id string) (*T, bool),
	allInRegion []*T,
	singleID string,
	notFoundErr error,
	fileSystemID string,
	fsIDOf func(*T) string,
	copyFn func(*T) *T,
	idOf func(*T) string,
	marker string,
	maxItems int,
) ([]*T, string, error) {
	if singleID != "" {
		item, ok := getByID(singleID)
		if !ok {
			return nil, "", fmt.Errorf("%w: %s not found", notFoundErr, singleID)
		}

		return []*T{copyFn(item)}, "", nil
	}

	all := make([]*T, 0, len(allInRegion))
	for _, item := range allInRegion {
		if fileSystemID != "" && fsIDOf(item) != fileSystemID {
			continue
		}
		all = append(all, copyFn(item))
	}
	sort.Slice(all, func(i, j int) bool { return idOf(all[i]) < idOf(all[j]) })

	return paginate(all, marker, maxItems, idOf)
}

// DescribeMountTargets returns mount targets, optionally filtered by file system ID or mount target ID.
func (b *InMemoryBackend) DescribeMountTargets(
	ctx context.Context,
	fileSystemID, mountTargetID, marker string, maxItems int,
) ([]*MountTarget, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeMountTargets")
	defer b.mu.RUnlock()

	return describeByIDOrFilter(
		func(id string) (*MountTarget, bool) { return b.mountTargets.Get(regionKey(region, id)) },
		b.mountTargetsByRegion.Get(region),
		mountTargetID, ErrMountTargetNotFound,
		fileSystemID,
		func(mt *MountTarget) string { return mt.FileSystemID },
		copyMountTarget,
		func(mt *MountTarget) string { return mt.MountTargetID },
		marker, maxItems,
	)
}

func copyMountTarget(mt *MountTarget) *MountTarget {
	cp := *mt
	cp.SecurityGroups = make([]string, len(mt.SecurityGroups))
	copy(cp.SecurityGroups, mt.SecurityGroups)

	return &cp
}

// DeleteMountTarget deletes a mount target by ID.
func (b *InMemoryBackend) DeleteMountTarget(ctx context.Context, mountTargetID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteMountTarget")
	defer b.mu.Unlock()

	mt, ok := b.mountTargets.Get(regionKey(region, mountTargetID))
	if !ok {
		return fmt.Errorf("%w: mount target %s not found", ErrMountTargetNotFound, mountTargetID)
	}
	if fs, found := b.fileSystems.Get(regionKey(region, mt.FileSystemID)); found {
		fs.NumberOfMountTargets--
	}
	b.mountTargetsByARN.Delete(regionKey(region, mt.MountTargetArn))
	b.mountTargets.Delete(regionKey(region, mountTargetID))
	// Clean up subnet index.
	if b.mtSubnetIdx[region] != nil && b.mtSubnetIdx[region][mt.FileSystemID] != nil {
		delete(b.mtSubnetIdx[region][mt.FileSystemID], mt.SubnetID)
	}

	return nil
}

// CreateAccessPoint creates an access point for a file system.
// Supports ClientToken idempotency.
func (b *InMemoryBackend) CreateAccessPoint(
	ctx context.Context,
	req CreateAccessPointRequest,
) (*AccessPoint, error) {
	if err := validateTags(req.Tags); err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateAccessPoint")
	defer b.mu.Unlock()

	// ClientToken idempotency.
	if req.ClientToken != "" {
		if existing, ok := b.accessPointsByClientToken.Get(regionKey(region, req.ClientToken)); ok {
			cp := copyAccessPoint(existing)

			return cp, nil
		}
	}

	if _, ok := b.fileSystems.Get(regionKey(region, req.FileSystemID)); !ok {
		return nil, fmt.Errorf("%w: file system %s not found", ErrNotFound, req.FileSystemID)
	}

	// Validate RootDirectory: require CreationInfo when path != "/".
	if req.RootDirectory != nil && req.RootDirectory.Path != "" && req.RootDirectory.Path != "/" {
		if req.RootDirectory.CreationInfo == nil {
			return nil, fmt.Errorf(
				"%w: CreationInfo is required when RootDirectory.Path is not /",
				ErrValidation,
			)
		}
	}

	id := "fsap-" + uuid.NewString()[:8]
	apARN := arn.Build("elasticfilesystem", region, b.accountID, "access-point/"+id)
	t := tags.New("efs.accesspoint." + id + ".tags")

	tagCopy := make(map[string]string, len(req.Tags))
	maps.Copy(tagCopy, req.Tags)

	if len(tagCopy) > 0 {
		t.Merge(tagCopy)
	}
	name := req.Tags["Name"]

	ap := &AccessPoint{
		region:         region,
		AccessPointID:  id,
		AccessPointArn: apARN,
		FileSystemID:   req.FileSystemID,
		ClientToken:    req.ClientToken,
		Name:           name,
		LifeCycleState: statusAvailable,
		Tags:           t,
		PosixUser:      req.PosixUser,
		RootDirectory:  req.RootDirectory,
		OwnerID:        b.accountID,
	}
	b.accessPoints.Put(ap)
	b.accessPointsByARN.Put(ap)
	if req.ClientToken != "" {
		b.accessPointsByClientToken.Put(ap)
	}
	b.apFSStore(region, req.FileSystemID)[id] = struct{}{}
	cp := copyAccessPoint(ap)

	return cp, nil
}

func copyAccessPoint(ap *AccessPoint) *AccessPoint {
	cp := *ap

	if ap.PosixUser != nil {
		pu := *ap.PosixUser
		if len(ap.PosixUser.SecondaryGids) > 0 {
			pu.SecondaryGids = make([]int64, len(ap.PosixUser.SecondaryGids))
			copy(pu.SecondaryGids, ap.PosixUser.SecondaryGids)
		}
		cp.PosixUser = &pu
	}

	if ap.RootDirectory != nil {
		rd := *ap.RootDirectory
		if ap.RootDirectory.CreationInfo != nil {
			ci := *ap.RootDirectory.CreationInfo
			rd.CreationInfo = &ci
		}
		cp.RootDirectory = &rd
	}

	return &cp
}

// DescribeAccessPoints returns access points, optionally filtered by file system ID or access point ID.
func (b *InMemoryBackend) DescribeAccessPoints(
	ctx context.Context,
	fileSystemID, accessPointID, marker string, maxItems int,
) ([]*AccessPoint, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeAccessPoints")
	defer b.mu.RUnlock()

	return describeByIDOrFilter(
		func(id string) (*AccessPoint, bool) { return b.accessPoints.Get(regionKey(region, id)) },
		b.accessPointsByRegion.Get(region),
		accessPointID, ErrAccessPointNotFound,
		fileSystemID,
		func(ap *AccessPoint) string { return ap.FileSystemID },
		copyAccessPoint,
		func(ap *AccessPoint) string { return ap.AccessPointID },
		marker, maxItems,
	)
}

// DeleteAccessPoint deletes an access point by ID.
func (b *InMemoryBackend) DeleteAccessPoint(ctx context.Context, accessPointID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteAccessPoint")
	defer b.mu.Unlock()

	ap, ok := b.accessPoints.Get(regionKey(region, accessPointID))
	if !ok {
		return fmt.Errorf("%w: access point %s not found", ErrAccessPointNotFound, accessPointID)
	}
	b.accessPointsByARN.Delete(regionKey(region, ap.AccessPointArn))
	if ap.ClientToken != "" {
		b.accessPointsByClientToken.Delete(regionKey(region, ap.ClientToken))
	}
	// Clean up apByFS index.
	if b.apByFS[region] != nil && b.apByFS[region][ap.FileSystemID] != nil {
		delete(b.apByFS[region][ap.FileSystemID], accessPointID)
	}

	ap.Tags.Close()
	b.accessPoints.Delete(regionKey(region, accessPointID))

	return nil
}

// DescribeLifecycleConfiguration returns lifecycle policies for a file system.
func (b *InMemoryBackend) DescribeLifecycleConfiguration(
	ctx context.Context,
	fileSystemID string,
) ([]LifecyclePolicy, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeLifecycleConfiguration")
	defer b.mu.RUnlock()

	if _, ok := b.fileSystems.Get(regionKey(region, fileSystemID)); !ok {
		return nil, fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	policies := b.lifecycleStore(region)[fileSystemID]
	if policies == nil {
		return []LifecyclePolicy{}, nil
	}

	result := make([]LifecyclePolicy, len(policies))
	copy(result, policies)

	return result, nil
}

// validateLifecyclePolicies checks that each policy's transition fields are valid AWS enum values.
func validateLifecyclePolicies(policies []LifecyclePolicy) error {
	for i, p := range policies {
		if p.TransitionToIA != "" && !isValidTransitionToIA(p.TransitionToIA) {
			return fmt.Errorf(
				"%w: invalid TransitionToIA value %q at index %d",
				ErrValidation,
				p.TransitionToIA,
				i,
			)
		}
		if p.TransitionToPrimaryStorageClass != "" &&
			!isValidTransitionToPrimary(p.TransitionToPrimaryStorageClass) {
			return fmt.Errorf(
				"%w: invalid TransitionToPrimaryStorageClass value %q at index %d",
				ErrValidation,
				p.TransitionToPrimaryStorageClass,
				i,
			)
		}
		if p.TransitionToArchive != "" && !isValidTransitionToArchive(p.TransitionToArchive) {
			return fmt.Errorf(
				"%w: invalid TransitionToArchive value %q at index %d",
				ErrValidation,
				p.TransitionToArchive,
				i,
			)
		}
	}

	return nil
}

// PutLifecycleConfiguration sets lifecycle policies for a file system.
func (b *InMemoryBackend) PutLifecycleConfiguration(
	ctx context.Context,
	fileSystemID string,
	policies []LifecyclePolicy,
) ([]LifecyclePolicy, error) {
	if err := validateLifecyclePolicies(policies); err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("PutLifecycleConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.fileSystems.Get(regionKey(region, fileSystemID)); !ok {
		return nil, fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	stored := make([]LifecyclePolicy, len(policies))
	copy(stored, policies)
	b.lifecycleStore(region)[fileSystemID] = stored

	result := make([]LifecyclePolicy, len(stored))
	copy(result, stored)

	return result, nil
}

// CreateReplicationConfiguration creates a replication configuration for a file system.
func (b *InMemoryBackend) CreateReplicationConfiguration(
	ctx context.Context,
	sourceFileSystemID string,
	destinations []ReplicationDestination,
) (*ReplicationConfiguration, error) {
	if len(destinations) != maxReplicationDestinations {
		return nil, fmt.Errorf(
			"%w: exactly %d destination is required, got %d",
			ErrValidation,
			maxReplicationDestinations,
			len(destinations),
		)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateReplicationConfiguration")
	defer b.mu.Unlock()

	fs, ok := b.fileSystems.Get(regionKey(region, sourceFileSystemID))
	if !ok {
		return nil, fmt.Errorf("%w: file system %s not found", ErrNotFound, sourceFileSystemID)
	}

	if _, exists := b.replicationConfigs.Get(regionKey(region, sourceFileSystemID)); exists {
		return nil, fmt.Errorf(
			"%w: replication configuration already exists for file system %s",
			ErrAlreadyExists,
			sourceFileSystemID,
		)
	}

	dests := make([]ReplicationDestination, len(destinations))
	copy(dests, destinations)
	for i := range dests {
		if dests[i].Status == "" {
			dests[i].Status = "ENABLED"
		}
		if dests[i].OwnerID == "" {
			dests[i].OwnerID = b.accountID
		}
		// Assign a destination file-system ID and ARN when not provided by the caller.
		// Real AWS creates a read-only replica; we record a synthetic ID here.
		if dests[i].FileSystemID == "" {
			destRegion := dests[i].Region
			if destRegion == "" {
				destRegion = region
			}
			destFSID := "fs-" + uuid.NewString()[:8]
			dests[i].FileSystemID = destFSID
			dests[i].FileSystemArn = arn.Build("elasticfilesystem", destRegion, b.accountID, "file-system/"+destFSID)
		} else if dests[i].FileSystemArn == "" {
			destRegion := dests[i].Region
			if destRegion == "" {
				destRegion = region
			}
			dests[i].FileSystemArn = arn.Build(
				"elasticfilesystem", destRegion, b.accountID, "file-system/"+dests[i].FileSystemID,
			)
		}
	}

	rc := &ReplicationConfiguration{
		OriginalSourceFileSystemARN: fs.FileSystemArn,
		SourceFileSystemARN:         fs.FileSystemArn,
		SourceFileSystemID:          sourceFileSystemID,
		SourceFileSystemOwnerID:     b.accountID,
		SourceFileSystemRegion:      region,
		CreationTime:                time.Now().UTC().Unix(),
		Destinations:                dests,
	}
	b.replicationConfigs.Put(rc)

	// Mark source file system as replicating.
	fs.ReplicationOverwriteProtection = protectionReplicating

	cp := *rc
	cp.Destinations = make([]ReplicationDestination, len(rc.Destinations))
	copy(cp.Destinations, rc.Destinations)

	return &cp, nil
}

// DeleteReplicationConfiguration deletes the replication configuration for a file system.
// The destination file system (if tracked) becomes a standalone writable file system
// with ReplicationOverwriteProtection set to ENABLED.
func (b *InMemoryBackend) DeleteReplicationConfiguration(
	ctx context.Context,
	sourceFileSystemID string,
) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteReplicationConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.fileSystems.Get(regionKey(region, sourceFileSystemID)); !ok {
		return fmt.Errorf("%w: file system %s not found", ErrNotFound, sourceFileSystemID)
	}

	if _, exists := b.replicationConfigs.Get(regionKey(region, sourceFileSystemID)); !exists {
		return fmt.Errorf(
			"%w: replication configuration not found for file system %s",
			ErrNotFound,
			sourceFileSystemID,
		)
	}

	b.replicationConfigs.Delete(regionKey(region, sourceFileSystemID))

	// Reset source protection to DISABLED.
	if fs, ok := b.fileSystems.Get(regionKey(region, sourceFileSystemID)); ok {
		fs.ReplicationOverwriteProtection = protectionDisabled
	}

	return nil
}

// DescribeReplicationConfigurations returns replication configurations, optionally filtered by file system ID.
func (b *InMemoryBackend) DescribeReplicationConfigurations(
	ctx context.Context,
	fileSystemID string,
) ([]*ReplicationConfiguration, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeReplicationConfigurations")
	defer b.mu.RUnlock()

	regionRCs := b.replicationConfigsByRegion.Get(region)

	if fileSystemID != "" {
		// Match source OR destination file system ID, matching real AWS behaviour.
		if rc, ok := b.replicationConfigs.Get(regionKey(region, fileSystemID)); ok {
			cp := *rc
			cp.Destinations = make([]ReplicationDestination, len(rc.Destinations))
			copy(cp.Destinations, rc.Destinations)

			return []*ReplicationConfiguration{&cp}, nil
		}

		for _, rc := range regionRCs {
			for _, d := range rc.Destinations {
				if d.FileSystemID == fileSystemID {
					cp := *rc
					cp.Destinations = make([]ReplicationDestination, len(rc.Destinations))
					copy(cp.Destinations, rc.Destinations)

					return []*ReplicationConfiguration{&cp}, nil
				}
			}
		}

		return []*ReplicationConfiguration{}, nil
	}

	list := make([]*ReplicationConfiguration, 0, len(regionRCs))
	for _, rc := range regionRCs {
		cp := *rc
		cp.Destinations = make([]ReplicationDestination, len(rc.Destinations))
		copy(cp.Destinations, rc.Destinations)
		list = append(list, &cp)
	}
	sort.Slice(
		list,
		func(i, j int) bool { return list[i].SourceFileSystemID < list[j].SourceFileSystemID },
	)

	return list, nil
}

// CreateTags adds tags to a file system (legacy operation, delegates to TagResource).
func (b *InMemoryBackend) CreateTags(ctx context.Context, fileSystemID string, kv map[string]string) error {
	if err := validateTags(kv); err != nil {
		return err
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateTags")
	defer b.mu.Unlock()

	fs, ok := b.fileSystems.Get(regionKey(region, fileSystemID))
	if !ok {
		return fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	fs.Tags.Merge(kv)

	return nil
}

// DeleteTags removes tags from a file system by key (legacy operation).
func (b *InMemoryBackend) DeleteTags(ctx context.Context, fileSystemID string, tagKeys []string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteTags")
	defer b.mu.Unlock()

	fs, ok := b.fileSystems.Get(regionKey(region, fileSystemID))
	if !ok {
		return fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	fs.Tags.DeleteKeys(tagKeys)

	return nil
}

// DescribeFileSystemPolicy returns the resource-based policy for a file system.
func (b *InMemoryBackend) DescribeFileSystemPolicy(ctx context.Context, fileSystemID string) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeFileSystemPolicy")
	defer b.mu.RUnlock()

	if _, ok := b.fileSystems.Get(regionKey(region, fileSystemID)); !ok {
		return "", fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	policy, ok := b.fsPolicyStore(region)[fileSystemID]
	if !ok {
		return "", fmt.Errorf("%w: no policy found for file system %s", ErrPolicyNotFound, fileSystemID)
	}

	return policy, nil
}

// DeleteFileSystemPolicy removes the resource-based policy from a file system.
func (b *InMemoryBackend) DeleteFileSystemPolicy(ctx context.Context, fileSystemID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteFileSystemPolicy")
	defer b.mu.Unlock()

	if _, ok := b.fileSystems.Get(regionKey(region, fileSystemID)); !ok {
		return fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	delete(b.fsPolicyStore(region), fileSystemID)

	return nil
}

// DescribeAccountPreferences returns the current account preferences.
func (b *InMemoryBackend) DescribeAccountPreferences() AccountPreferences {
	b.mu.RLock("DescribeAccountPreferences")
	defer b.mu.RUnlock()

	return b.accountPreferences
}

// DescribeBackupPolicy returns the backup policy for a file system.
func (b *InMemoryBackend) DescribeBackupPolicy(ctx context.Context, fileSystemID string) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeBackupPolicy")
	defer b.mu.RUnlock()

	if _, ok := b.fileSystems.Get(regionKey(region, fileSystemID)); !ok {
		return "", fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	status, ok := b.backupStore(region)[fileSystemID]
	if !ok {
		return backupStatusDisabled, nil
	}

	return status, nil
}

// DescribeMountTargetSecurityGroups returns the security groups for a mount target.
func (b *InMemoryBackend) DescribeMountTargetSecurityGroups(
	ctx context.Context,
	mountTargetID string,
) ([]string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeMountTargetSecurityGroups")
	defer b.mu.RUnlock()

	mt, ok := b.mountTargets.Get(regionKey(region, mountTargetID))
	if !ok {
		return nil, fmt.Errorf(
			"%w: mount target %s not found",
			ErrMountTargetNotFound,
			mountTargetID,
		)
	}

	if mt.SecurityGroups == nil {
		return []string{}, nil
	}

	result := make([]string, len(mt.SecurityGroups))
	copy(result, mt.SecurityGroups)

	return result, nil
}

// PutBackupPolicy sets the backup policy status for a file system.
// Valid values: ENABLED, ENABLING, DISABLED, DISABLING.
func (b *InMemoryBackend) PutBackupPolicy(ctx context.Context, fileSystemID, status string) error {
	switch status {
	case backupStatusEnabled, backupStatusEnabling, backupStatusDisabled, "DISABLING":
		// valid
	default:
		return fmt.Errorf(
			"%w: invalid backup policy status %q, must be ENABLED, ENABLING, DISABLED, or DISABLING",
			ErrValidation,
			status,
		)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("PutBackupPolicy")
	defer b.mu.Unlock()

	if _, ok := b.fileSystems.Get(regionKey(region, fileSystemID)); !ok {
		return fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	b.backupStore(region)[fileSystemID] = status

	return nil
}

// PutFileSystemPolicy sets the resource-based policy for a file system.
// The policy must be valid JSON and no larger than 20 KB.
func (b *InMemoryBackend) PutFileSystemPolicy(ctx context.Context, fileSystemID, policy string) error {
	if !json.Valid([]byte(policy)) {
		return fmt.Errorf("%w: FileSystemPolicy is not valid JSON", ErrValidation)
	}
	if len(policy) > maxFileSystemPolicyBytes {
		return fmt.Errorf(
			"%w: FileSystemPolicy exceeds maximum size of %d bytes, got %d",
			ErrValidation,
			maxFileSystemPolicyBytes,
			len(policy),
		)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("PutFileSystemPolicy")
	defer b.mu.Unlock()

	if _, ok := b.fileSystems.Get(regionKey(region, fileSystemID)); !ok {
		return fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	b.fsPolicyStore(region)[fileSystemID] = policy

	return nil
}

// applyThroughputModeChange validates and applies a throughput mode change to
// a file system. Must be called under b.mu write lock.
func (b *InMemoryBackend) applyThroughputModeChange(
	fs *FileSystem,
	req UpdateFileSystemRequest,
) error {
	if req.ThroughputMode != throughputModeBursting &&
		req.ThroughputMode != throughputModeProvisioned &&
		req.ThroughputMode != throughputModeElastic {
		return fmt.Errorf(
			"%w: invalid ThroughputMode %q, must be bursting, provisioned, or elastic",
			ErrValidation,
			req.ThroughputMode,
		)
	}

	if !fs.LastThroughputChange.IsZero() &&
		time.Since(fs.LastThroughputChange) < throughputCooldown {
		return fmt.Errorf(
			"%w: throughput mode was last changed at %s; must wait 24 hours between changes",
			ErrTooManyRequests,
			fs.LastThroughputChange.Format(time.RFC3339),
		)
	}

	if req.ThroughputMode == throughputModeProvisioned {
		if req.ProvisionedThroughputMib < 1 || req.ProvisionedThroughputMib > 1024 {
			return fmt.Errorf(
				"%w: ProvisionedThroughputInMibps must be between 1 and 1024 when ThroughputMode is provisioned, got %g",
				ErrValidation,
				req.ProvisionedThroughputMib,
			)
		}
	}

	fs.ThroughputMode = req.ThroughputMode
	fs.LastThroughputChange = time.Now().UTC()

	return nil
}

// UpdateFileSystem updates throughput settings for a file system.
// Enforces a 24-hour cooldown between throughput mode changes.
func (b *InMemoryBackend) UpdateFileSystem(
	ctx context.Context,
	fileSystemID string,
	req UpdateFileSystemRequest,
) (*FileSystem, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateFileSystem")
	defer b.mu.Unlock()

	fs, ok := b.fileSystems.Get(regionKey(region, fileSystemID))
	if !ok {
		return nil, fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	if req.ThroughputMode != "" {
		if err := b.applyThroughputModeChange(fs, req); err != nil {
			return nil, err
		}
	}

	if req.ProvisionedThroughputMib != 0 {
		if fs.ThroughputMode != throughputModeProvisioned {
			return nil, fmt.Errorf(
				"%w: ProvisionedThroughputInMibps is only valid when ThroughputMode is provisioned",
				ErrValidation,
			)
		}
		if req.ProvisionedThroughputMib < 1 || req.ProvisionedThroughputMib > 1024 {
			return nil, fmt.Errorf(
				"%w: ProvisionedThroughputInMibps must be between 1 and 1024, got %g",
				ErrValidation,
				req.ProvisionedThroughputMib,
			)
		}
		fs.ProvisionedThroughputMib = req.ProvisionedThroughputMib
	}

	cp := *fs

	return &cp, nil
}

// ModifyMountTargetSecurityGroups replaces the security groups for a mount target.
// Enforces a maximum of 5 security groups.
func (b *InMemoryBackend) ModifyMountTargetSecurityGroups(
	ctx context.Context,
	mountTargetID string,
	securityGroups []string,
) error {
	if len(securityGroups) > maxSecurityGroups {
		return fmt.Errorf(
			"%w: too many security groups: %d (max %d)",
			ErrSecurityGroupLimitExceeded,
			len(securityGroups),
			maxSecurityGroups,
		)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("ModifyMountTargetSecurityGroups")
	defer b.mu.Unlock()

	mt, ok := b.mountTargets.Get(regionKey(region, mountTargetID))
	if !ok {
		return fmt.Errorf("%w: mount target %s not found", ErrMountTargetNotFound, mountTargetID)
	}

	groups := make([]string, len(securityGroups))
	copy(groups, securityGroups)
	mt.SecurityGroups = groups

	return nil
}

// PutAccountPreferences sets the account-level resource ID preference.
func (b *InMemoryBackend) PutAccountPreferences(resourceIDType string) (AccountPreferences, error) {
	b.mu.Lock("PutAccountPreferences")
	defer b.mu.Unlock()

	if resourceIDType != "LONG_ID" && resourceIDType != "SHORT_ID" {
		return AccountPreferences{}, fmt.Errorf(
			"%w: invalid ResourceIdType %q, must be LONG_ID or SHORT_ID",
			ErrValidation,
			resourceIDType,
		)
	}

	b.accountPreferences.ResourceIDType = resourceIDType

	return b.accountPreferences, nil
}

// UpdateFileSystemProtection sets the replication overwrite protection for a file system.
func (b *InMemoryBackend) UpdateFileSystemProtection(
	ctx context.Context,
	fileSystemID, replicationOverwriteProtection string,
) error {
	switch replicationOverwriteProtection {
	case protectionEnabled, protectionDisabled, protectionReplicating:
		// valid
	default:
		return fmt.Errorf(
			"%w: invalid ReplicationOverwriteProtection value %q, must be ENABLED, DISABLED, or REPLICATING",
			ErrValidation,
			replicationOverwriteProtection,
		)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateFileSystemProtection")
	defer b.mu.Unlock()

	fs, ok := b.fileSystems.Get(regionKey(region, fileSystemID))
	if !ok {
		return fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	fs.ReplicationOverwriteProtection = replicationOverwriteProtection

	return nil
}

// paginate applies cursor-based pagination to a sorted slice.
// Items after marker are returned up to maxItems. nextToken is non-empty when more items remain.
// Marker lookup uses binary search (O(log n)) since the slice is already sorted by keyFn.
func paginate[T any](
	items []T,
	marker string,
	maxItems int,
	keyFn func(T) string,
) ([]T, string, error) {
	if marker != "" {
		// Binary search: find the leftmost index where keyFn(items[i]) >= marker.
		idx := sort.Search(len(items), func(i int) bool { return keyFn(items[i]) >= marker })
		if idx >= len(items) || keyFn(items[idx]) != marker {
			return nil, "", fmt.Errorf("%w: invalid pagination marker", ErrValidation)
		}
		items = items[idx+1:]
	}

	if maxItems <= 0 || maxItems >= len(items) {
		return items, "", nil
	}

	page := items[:maxItems]
	next := keyFn(items[maxItems])

	return page, next, nil
}

// regionFromARN extracts the region component (index 3) from an AWS ARN
// (arn:partition:service:region:account:resource), falling back to defaultRegion.
func regionFromARN(resourceARN, defaultRegion string) string {
	parts := strings.Split(resourceARN, ":")
	const regionIndex = 3
	if len(parts) > regionIndex && parts[regionIndex] != "" {
		return parts[regionIndex]
	}

	return defaultRegion
}

// AddFileSystemInternal inserts a pre-built FileSystem directly into the backend (test seed helper).
func (b *InMemoryBackend) AddFileSystemInternal(fs *FileSystem) {
	b.mu.Lock("AddFileSystemInternal")
	defer b.mu.Unlock()

	region := fs.Region
	if region == "" {
		region = regionFromARN(fs.FileSystemArn, b.region)
		fs.Region = region
	}

	b.fileSystems.Put(fs)
	b.fileSystemsByARN.Put(fs)
}

// AddMountTargetInternal inserts a pre-built MountTarget directly into the backend (test seed helper).
func (b *InMemoryBackend) AddMountTargetInternal(mt *MountTarget) {
	b.mu.Lock("AddMountTargetInternal")
	defer b.mu.Unlock()

	mt.region = regionFromARN(mt.MountTargetArn, b.region)
	b.mountTargets.Put(mt)
	b.mountTargetsByARN.Put(mt)
}

// AddAccessPointInternal inserts a pre-built AccessPoint directly into the backend (test seed helper).
func (b *InMemoryBackend) AddAccessPointInternal(ap *AccessPoint) {
	b.mu.Lock("AddAccessPointInternal")
	defer b.mu.Unlock()

	ap.region = regionFromARN(ap.AccessPointArn, b.region)
	b.accessPoints.Put(ap)
	b.accessPointsByARN.Put(ap)
}
