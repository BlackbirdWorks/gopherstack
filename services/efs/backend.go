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
// All resource maps are nested by region (outer key = region) so that
// same-named resources in different regions are fully isolated. The
// cross-index maps (by ARN, by client token) are likewise region-scoped.
// accountPreferences is account-level state in AWS and so is not region-nested.
type InMemoryBackend struct {
	fileSystems               map[string]map[string]*FileSystem
	mountTargets              map[string]map[string]*MountTarget
	accessPoints              map[string]map[string]*AccessPoint
	lifecyclePolicies         map[string]map[string][]LifecyclePolicy
	replicationConfigs        map[string]map[string]*ReplicationConfiguration
	backupPolicies            map[string]map[string]string
	fileSystemPolicies        map[string]map[string]string
	fileSystemsByARN          map[string]map[string]*FileSystem
	mountTargetsByARN         map[string]map[string]*MountTarget
	accessPointsByARN         map[string]map[string]*AccessPoint
	accessPointsByClientToken map[string]map[string]*AccessPoint
	accountPreferences        AccountPreferences
	mu                        *lockmetrics.RWMutex
	accountID                 string
	region                    string
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
	}
	b.initRegionMaps()

	return b
}

// initRegionMaps allocates the (empty) outer per-region map for every resource kind.
func (b *InMemoryBackend) initRegionMaps() {
	b.fileSystems = make(map[string]map[string]*FileSystem)
	b.mountTargets = make(map[string]map[string]*MountTarget)
	b.accessPoints = make(map[string]map[string]*AccessPoint)
	b.lifecyclePolicies = make(map[string]map[string][]LifecyclePolicy)
	b.replicationConfigs = make(map[string]map[string]*ReplicationConfiguration)
	b.backupPolicies = make(map[string]map[string]string)
	b.fileSystemPolicies = make(map[string]map[string]string)
	b.fileSystemsByARN = make(map[string]map[string]*FileSystem)
	b.mountTargetsByARN = make(map[string]map[string]*MountTarget)
	b.accessPointsByARN = make(map[string]map[string]*AccessPoint)
	b.accessPointsByClientToken = make(map[string]map[string]*AccessPoint)
}

// The following per-region store helpers return the inner map for region,
// lazily creating it on first access. Callers must hold b.mu.

func (b *InMemoryBackend) fsStore(region string) map[string]*FileSystem {
	if b.fileSystems[region] == nil {
		b.fileSystems[region] = make(map[string]*FileSystem)
	}

	return b.fileSystems[region]
}

func (b *InMemoryBackend) mtStore(region string) map[string]*MountTarget {
	if b.mountTargets[region] == nil {
		b.mountTargets[region] = make(map[string]*MountTarget)
	}

	return b.mountTargets[region]
}

func (b *InMemoryBackend) apStore(region string) map[string]*AccessPoint {
	if b.accessPoints[region] == nil {
		b.accessPoints[region] = make(map[string]*AccessPoint)
	}

	return b.accessPoints[region]
}

func (b *InMemoryBackend) lifecycleStore(region string) map[string][]LifecyclePolicy {
	if b.lifecyclePolicies[region] == nil {
		b.lifecyclePolicies[region] = make(map[string][]LifecyclePolicy)
	}

	return b.lifecyclePolicies[region]
}

func (b *InMemoryBackend) replicationStore(region string) map[string]*ReplicationConfiguration {
	if b.replicationConfigs[region] == nil {
		b.replicationConfigs[region] = make(map[string]*ReplicationConfiguration)
	}

	return b.replicationConfigs[region]
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

func (b *InMemoryBackend) fsARNStore(region string) map[string]*FileSystem {
	if b.fileSystemsByARN[region] == nil {
		b.fileSystemsByARN[region] = make(map[string]*FileSystem)
	}

	return b.fileSystemsByARN[region]
}

func (b *InMemoryBackend) mtARNStore(region string) map[string]*MountTarget {
	if b.mountTargetsByARN[region] == nil {
		b.mountTargetsByARN[region] = make(map[string]*MountTarget)
	}

	return b.mountTargetsByARN[region]
}

func (b *InMemoryBackend) apARNStore(region string) map[string]*AccessPoint {
	if b.accessPointsByARN[region] == nil {
		b.accessPointsByARN[region] = make(map[string]*AccessPoint)
	}

	return b.accessPointsByARN[region]
}

func (b *InMemoryBackend) apClientTokenStore(region string) map[string]*AccessPoint {
	if b.accessPointsByClientToken[region] == nil {
		b.accessPointsByClientToken[region] = make(map[string]*AccessPoint)
	}

	return b.accessPointsByClientToken[region]
}

// Reset clears all stored resources, returning the backend to its empty initial state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, regionFS := range b.fileSystems {
		for _, fs := range regionFS {
			fs.Tags.Close()
		}
	}
	for _, regionAP := range b.accessPoints {
		for _, ap := range regionAP {
			ap.Tags.Close()
		}
	}

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

	fileSystems := b.fsStore(region)

	// Idempotency: if creationToken already used, compare args.
	for _, fs := range fileSystems {
		if fs.CreationToken == req.CreationToken {
			if fs.PerformanceMode == req.PerformanceMode &&
				fs.ThroughputMode == req.ThroughputMode &&
				fs.Encrypted == req.Encrypted &&
				fs.KmsKeyID == req.KmsKeyID &&
				fs.AvailabilityZoneName == req.AvailabilityZoneName {
				cp := *fs

				return &cp, fmt.Errorf(
					"%w: file system with token %s already exists (identical args)",
					ErrCreationTokenExists,
					req.CreationToken,
				)
			}

			cp := *fs

			return &cp, fmt.Errorf(
				"%w: file system with token %s already exists with different parameters (FileSystemId: %s)",
				ErrAlreadyExists,
				req.CreationToken,
				fs.FileSystemID,
			)
		}
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

	fs := &FileSystem{
		FileSystemID:                   id,
		FileSystemArn:                  fsARN,
		CreationToken:                  req.CreationToken,
		Name:                           name,
		PerformanceMode:                req.PerformanceMode,
		ThroughputMode:                 req.ThroughputMode,
		LifeCycleState:                 statusAvailable,
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
	fileSystems[id] = fs
	b.fsARNStore(region)[fsARN] = fs
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

	fileSystems := b.fsStore(region)

	if fileSystemID != "" {
		fs, ok := fileSystems[fileSystemID]
		if !ok {
			return nil, "", fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
		}
		cp := *fs

		return []*FileSystem{&cp}, "", nil
	}

	if creationToken != "" {
		for _, fs := range fileSystems {
			if fs.CreationToken == creationToken {
				cp := *fs

				return []*FileSystem{&cp}, "", nil
			}
		}

		return []*FileSystem{}, "", nil
	}

	all := make([]*FileSystem, 0, len(fileSystems))
	for _, fs := range fileSystems {
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

	fileSystems := b.fsStore(region)
	fs, ok := fileSystems[fileSystemID]
	if !ok {
		return fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	// Reject delete if mount targets or access points exist (AWS: FileSystemInUse).
	for _, mt := range b.mtStore(region) {
		if mt.FileSystemID == fileSystemID {
			return fmt.Errorf(
				"%w: file system %s has existing mount targets",
				ErrFileSystemInUse,
				fileSystemID,
			)
		}
	}
	for _, ap := range b.apStore(region) {
		if ap.FileSystemID == fileSystemID {
			return fmt.Errorf(
				"%w: file system %s has existing access points",
				ErrFileSystemInUse,
				fileSystemID,
			)
		}
	}

	delete(b.fsARNStore(region), fs.FileSystemArn)
	fs.Tags.Close()
	delete(fileSystems, fileSystemID)
	delete(b.lifecycleStore(region), fileSystemID)
	delete(b.backupStore(region), fileSystemID)
	delete(b.fsPolicyStore(region), fileSystemID)
	delete(b.replicationStore(region), fileSystemID)

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

	if fs, ok := b.fsStore(region)[resourceID]; ok {
		fs.Tags.Merge(kv)

		return nil
	}
	if fs, ok := b.fsARNStore(region)[resourceID]; ok {
		fs.Tags.Merge(kv)

		return nil
	}

	if ap, ok := b.apStore(region)[resourceID]; ok {
		ap.Tags.Merge(kv)

		return nil
	}
	if ap, ok := b.apARNStore(region)[resourceID]; ok {
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

	if fs, ok := b.fsStore(region)[resourceID]; ok {
		fs.Tags.DeleteKeys(tagKeys)

		return nil
	}
	if fs, ok := b.fsARNStore(region)[resourceID]; ok {
		fs.Tags.DeleteKeys(tagKeys)

		return nil
	}

	if ap, ok := b.apStore(region)[resourceID]; ok {
		ap.Tags.DeleteKeys(tagKeys)

		return nil
	}
	if ap, ok := b.apARNStore(region)[resourceID]; ok {
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

	if fs, ok := b.fsStore(region)[resourceID]; ok {
		return fs.Tags.Clone(), nil
	}
	if fs, ok := b.fsARNStore(region)[resourceID]; ok {
		return fs.Tags.Clone(), nil
	}

	if ap, ok := b.apStore(region)[resourceID]; ok {
		return ap.Tags.Clone(), nil
	}
	if ap, ok := b.apARNStore(region)[resourceID]; ok {
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

	mountTargets := b.mtStore(region)

	fs, ok := b.fsStore(region)[req.FileSystemID]
	if !ok {
		return nil, fmt.Errorf("%w: file system %s not found", ErrNotFound, req.FileSystemID)
	}

	// One mount target per subnet per file system.
	if req.SubnetID != "" {
		for _, mt := range mountTargets {
			if mt.FileSystemID == req.FileSystemID && mt.SubnetID == req.SubnetID {
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

	mt := &MountTarget{
		MountTargetID:      id,
		MountTargetArn:     mtARN,
		FileSystemID:       req.FileSystemID,
		SubnetID:           req.SubnetID,
		IPAddress:          req.IPAddress,
		NetworkInterfaceID: eniID,
		LifeCycleState:     statusAvailable,
		OwnerID:            b.accountID,
		SecurityGroups:     sgs,
	}
	mountTargets[id] = mt
	b.mtARNStore(region)[mtARN] = mt
	fs.NumberOfMountTargets++

	cp := *mt
	cp.SecurityGroups = make([]string, len(mt.SecurityGroups))
	copy(cp.SecurityGroups, mt.SecurityGroups)

	return &cp, nil
}

// describeByIDOrFilter is a generic helper for Describe* methods that look up
// a single item by ID or filter a map by file-system ID, then paginate.
func describeByIDOrFilter[T any](
	items map[string]*T,
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
		item, ok := items[singleID]
		if !ok {
			return nil, "", fmt.Errorf("%w: %s not found", notFoundErr, singleID)
		}

		return []*T{copyFn(item)}, "", nil
	}

	all := make([]*T, 0, len(items))
	for _, item := range items {
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
		b.mtStore(region), mountTargetID, ErrMountTargetNotFound,
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

	mountTargets := b.mtStore(region)
	mt, ok := mountTargets[mountTargetID]
	if !ok {
		return fmt.Errorf("%w: mount target %s not found", ErrMountTargetNotFound, mountTargetID)
	}
	if fs, found := b.fsStore(region)[mt.FileSystemID]; found {
		fs.NumberOfMountTargets--
	}
	delete(b.mtARNStore(region), mt.MountTargetArn)
	delete(mountTargets, mountTargetID)

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
		if existing, ok := b.apClientTokenStore(region)[req.ClientToken]; ok {
			cp := copyAccessPoint(existing)

			return cp, nil
		}
	}

	if _, ok := b.fsStore(region)[req.FileSystemID]; !ok {
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
	b.apStore(region)[id] = ap
	b.apARNStore(region)[apARN] = ap
	if req.ClientToken != "" {
		b.apClientTokenStore(region)[req.ClientToken] = ap
	}
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
		b.apStore(region), accessPointID, ErrAccessPointNotFound,
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

	accessPoints := b.apStore(region)
	ap, ok := accessPoints[accessPointID]
	if !ok {
		return fmt.Errorf("%w: access point %s not found", ErrAccessPointNotFound, accessPointID)
	}
	delete(b.apARNStore(region), ap.AccessPointArn)
	if ap.ClientToken != "" {
		delete(b.apClientTokenStore(region), ap.ClientToken)
	}
	ap.Tags.Close()
	delete(accessPoints, accessPointID)

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

	if _, ok := b.fsStore(region)[fileSystemID]; !ok {
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

	if _, ok := b.fsStore(region)[fileSystemID]; !ok {
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

	replicationConfigs := b.replicationStore(region)

	fs, ok := b.fsStore(region)[sourceFileSystemID]
	if !ok {
		return nil, fmt.Errorf("%w: file system %s not found", ErrNotFound, sourceFileSystemID)
	}

	if _, exists := replicationConfigs[sourceFileSystemID]; exists {
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
	replicationConfigs[sourceFileSystemID] = rc

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

	fileSystems := b.fsStore(region)
	if _, ok := fileSystems[sourceFileSystemID]; !ok {
		return fmt.Errorf("%w: file system %s not found", ErrNotFound, sourceFileSystemID)
	}

	replicationConfigs := b.replicationStore(region)
	if _, exists := replicationConfigs[sourceFileSystemID]; !exists {
		return fmt.Errorf(
			"%w: replication configuration not found for file system %s",
			ErrNotFound,
			sourceFileSystemID,
		)
	}

	delete(replicationConfigs, sourceFileSystemID)

	// Reset source protection to DISABLED.
	if fs, ok := fileSystems[sourceFileSystemID]; ok {
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

	replicationConfigs := b.replicationStore(region)

	if fileSystemID != "" {
		// Match source OR destination file system ID, matching real AWS behaviour.
		if rc, ok := replicationConfigs[fileSystemID]; ok {
			cp := *rc
			cp.Destinations = make([]ReplicationDestination, len(rc.Destinations))
			copy(cp.Destinations, rc.Destinations)

			return []*ReplicationConfiguration{&cp}, nil
		}
		for _, rc := range replicationConfigs {
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

	list := make([]*ReplicationConfiguration, 0, len(replicationConfigs))
	for _, rc := range replicationConfigs {
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

	fs, ok := b.fsStore(region)[fileSystemID]
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

	fs, ok := b.fsStore(region)[fileSystemID]
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

	if _, ok := b.fsStore(region)[fileSystemID]; !ok {
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

	if _, ok := b.fsStore(region)[fileSystemID]; !ok {
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

	if _, ok := b.fsStore(region)[fileSystemID]; !ok {
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

	mt, ok := b.mtStore(region)[mountTargetID]
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

	if _, ok := b.fsStore(region)[fileSystemID]; !ok {
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

	if _, ok := b.fsStore(region)[fileSystemID]; !ok {
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

	fs, ok := b.fsStore(region)[fileSystemID]
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

	mt, ok := b.mtStore(region)[mountTargetID]
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

	fs, ok := b.fsStore(region)[fileSystemID]
	if !ok {
		return fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	fs.ReplicationOverwriteProtection = replicationOverwriteProtection

	return nil
}

// paginate applies cursor-based pagination to a sorted slice.
// Items after marker are returned up to maxItems. nextToken is non-empty when more items remain.
func paginate[T any](
	items []T,
	marker string,
	maxItems int,
	keyFn func(T) string,
) ([]T, string, error) {
	if marker != "" {
		start := -1
		for i, item := range items {
			if keyFn(item) == marker {
				start = i + 1

				break
			}
		}
		if start == -1 {
			return nil, "", fmt.Errorf("%w: invalid pagination marker", ErrValidation)
		}
		items = items[start:]
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

	b.fsStore(region)[fs.FileSystemID] = fs
	b.fsARNStore(region)[fs.FileSystemArn] = fs
}

// AddMountTargetInternal inserts a pre-built MountTarget directly into the backend (test seed helper).
func (b *InMemoryBackend) AddMountTargetInternal(mt *MountTarget) {
	b.mu.Lock("AddMountTargetInternal")
	defer b.mu.Unlock()

	region := regionFromARN(mt.MountTargetArn, b.region)
	b.mtStore(region)[mt.MountTargetID] = mt
	b.mtARNStore(region)[mt.MountTargetArn] = mt
}

// AddAccessPointInternal inserts a pre-built AccessPoint directly into the backend (test seed helper).
func (b *InMemoryBackend) AddAccessPointInternal(ap *AccessPoint) {
	b.mu.Lock("AddAccessPointInternal")
	defer b.mu.Unlock()

	region := regionFromARN(ap.AccessPointArn, b.region)
	b.apStore(region)[ap.AccessPointID] = ap
	b.apARNStore(region)[ap.AccessPointArn] = ap
}
