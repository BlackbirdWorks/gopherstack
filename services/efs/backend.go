package efs

import (
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
)

var validTransitionToIA = map[string]bool{
	"AFTER_7_DAYS":   true,
	"AFTER_14_DAYS":  true,
	"AFTER_30_DAYS":  true,
	"AFTER_60_DAYS":  true,
	"AFTER_90_DAYS":  true,
	"AFTER_180_DAYS": true,
	"AFTER_270_DAYS": true,
	"AFTER_365_DAYS": true,
	"NONE":           true,
}

var validTransitionToPrimary = map[string]bool{
	"AFTER_1_ACCESS": true,
}

var validTransitionToArchive = map[string]bool{
	"AFTER_1_ACCESS":  true,
	"AFTER_7_DAYS":    true,
	"AFTER_14_DAYS":   true,
	"AFTER_30_DAYS":   true,
	"AFTER_60_DAYS":   true,
	"AFTER_90_DAYS":   true,
	"AFTER_180_DAYS":  true,
	"AFTER_270_DAYS":  true,
	"AFTER_365_DAYS":  true,
	"AFTER_90_DAYS_1": true,
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
	Uid           int64   `json:"Uid"`
	Gid           int64   `json:"Gid"`
	SecondaryGids []int64 `json:"SecondaryGids,omitempty"`
}

// CreationInfo specifies the POSIX IDs and permissions to apply to the access
// point's RootDirectory when it does not exist.
type CreationInfo struct {
	OwnerUid    int64  `json:"OwnerUid"`
	OwnerGid    int64  `json:"OwnerGid"`
	Permissions string `json:"Permissions"`
}

// RootDirectory specifies the directory on the Amazon EFS file system that the
// access point provides access to.
type RootDirectory struct {
	Path         string        `json:"Path,omitempty"`
	CreationInfo *CreationInfo `json:"CreationInfo,omitempty"`
}

// FileSystem represents an EFS file system.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via TagResource / CreateFileSystem.
type FileSystem struct {
	CreationTime                   time.Time  `json:"creationTime"`
	Tags                           *tags.Tags `json:"tags,omitempty"`
	LastThroughputChange           time.Time  `json:"lastThroughputChange,omitempty"`
	PerformanceMode                string     `json:"performanceMode"`
	FileSystemArn                  string     `json:"fileSystemArn"`
	CreationToken                  string     `json:"creationToken"`
	Name                           string     `json:"name,omitempty"`
	FileSystemID                   string     `json:"fileSystemId"`
	ThroughputMode                 string     `json:"throughputMode"`
	LifeCycleState                 string     `json:"lifeCycleState"`
	AccountID                      string     `json:"accountId"`
	Region                         string     `json:"region"`
	KmsKeyId                       string     `json:"kmsKeyId,omitempty"`
	AvailabilityZoneName           string     `json:"availabilityZoneName,omitempty"`
	AvailabilityZoneId             string     `json:"availabilityZoneId,omitempty"`
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
	AvailabilityZoneId   string   `json:"availabilityZoneId"`
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
	FileSystemID         string `json:"FileSystemId,omitempty"`
	Region               string `json:"Region,omitempty"`
	AvailabilityZoneName string `json:"AvailabilityZoneName,omitempty"`
	KmsKeyID             string `json:"KmsKeyId,omitempty"`
	Status               string `json:"Status,omitempty"`
}

// ReplicationConfiguration represents an EFS replication configuration.
type ReplicationConfiguration struct {
	OriginalSourceFileSystemARN string                   `json:"OriginalSourceFileSystemArn"`
	SourceFileSystemARN         string                   `json:"SourceFileSystemArn"`
	SourceFileSystemID          string                   `json:"SourceFileSystemId"`
	SourceFileSystemRegion      string                   `json:"SourceFileSystemRegion"`
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
	CreationToken            string
	PerformanceMode          string
	ThroughputMode           string
	KmsKeyId                 string
	AvailabilityZoneName     string
	ProvisionedThroughputMib float64
	Tags                     map[string]string
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
	FileSystemID  string
	ClientToken   string
	Tags          map[string]string
	PosixUser     *PosixUser
	RootDirectory *RootDirectory
}

// InMemoryBackend is the in-memory store for EFS resources.
type InMemoryBackend struct {
	fileSystems               map[string]*FileSystem
	mountTargets              map[string]*MountTarget
	accessPoints              map[string]*AccessPoint
	lifecyclePolicies         map[string][]LifecyclePolicy
	replicationConfigs        map[string]*ReplicationConfiguration
	backupPolicies            map[string]string
	fileSystemPolicies        map[string]string
	fileSystemsByARN          map[string]*FileSystem
	mountTargetsByARN         map[string]*MountTarget
	accessPointsByARN         map[string]*AccessPoint
	accessPointsByClientToken map[string]*AccessPoint
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
	return &InMemoryBackend{
		fileSystems:               make(map[string]*FileSystem),
		mountTargets:              make(map[string]*MountTarget),
		accessPoints:              make(map[string]*AccessPoint),
		lifecyclePolicies:         make(map[string][]LifecyclePolicy),
		replicationConfigs:        make(map[string]*ReplicationConfiguration),
		backupPolicies:            make(map[string]string),
		fileSystemPolicies:        make(map[string]string),
		fileSystemsByARN:          make(map[string]*FileSystem),
		mountTargetsByARN:         make(map[string]*MountTarget),
		accessPointsByARN:         make(map[string]*AccessPoint),
		accessPointsByClientToken: make(map[string]*AccessPoint),
		accountPreferences:        AccountPreferences{ResourceIDType: "LONG_ID"},
		accountID:                 accountID,
		region:                    region,
		mu:                        lockmetrics.New("efs"),
	}
}

// Reset clears all stored resources, returning the backend to its empty initial state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, fs := range b.fileSystems {
		fs.Tags.Close()
	}
	for _, ap := range b.accessPoints {
		ap.Tags.Close()
	}

	b.fileSystems = make(map[string]*FileSystem)
	b.mountTargets = make(map[string]*MountTarget)
	b.accessPoints = make(map[string]*AccessPoint)
	b.lifecyclePolicies = make(map[string][]LifecyclePolicy)
	b.replicationConfigs = make(map[string]*ReplicationConfiguration)
	b.backupPolicies = make(map[string]string)
	b.fileSystemPolicies = make(map[string]string)
	b.fileSystemsByARN = make(map[string]*FileSystem)
	b.mountTargetsByARN = make(map[string]*MountTarget)
	b.accessPointsByARN = make(map[string]*AccessPoint)
	b.accessPointsByClientToken = make(map[string]*AccessPoint)
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
func (b *InMemoryBackend) CreateFileSystem(req CreateFileSystemRequest) (*FileSystem, error) {
	if err := validateTags(req.Tags); err != nil {
		return nil, err
	}

	// Normalize defaults before idempotency comparison.
	if req.PerformanceMode == "" {
		req.PerformanceMode = performanceModeGeneral
	}
	if req.ThroughputMode == "" {
		req.ThroughputMode = throughputModeBursting
	}

	b.mu.Lock("CreateFileSystem")
	defer b.mu.Unlock()

	// Idempotency: if creationToken already used, compare args.
	for _, fs := range b.fileSystems {
		if fs.CreationToken == req.CreationToken {
			// Identical args -> return existing fs with ErrCreationTokenExists (caller maps to 200).
			if fs.PerformanceMode == req.PerformanceMode &&
				fs.ThroughputMode == req.ThroughputMode &&
				fs.Encrypted == req.Encrypted &&
				fs.KmsKeyId == req.KmsKeyId &&
				fs.AvailabilityZoneName == req.AvailabilityZoneName {
				cp := *fs
				return &cp, fmt.Errorf(
					"%w: file system with token %s already exists (identical args)",
					ErrCreationTokenExists,
					req.CreationToken,
				)
			}
			// Different args -> 409 conflict with file system ID.
			cp := *fs
			return &cp, fmt.Errorf(
				"%w: file system with token %s already exists with different parameters (FileSystemId: %s)",
				ErrAlreadyExists,
				req.CreationToken,
				fs.FileSystemID,
			)
		}
	}

	if req.PerformanceMode != performanceModeGeneral && req.PerformanceMode != performanceModeMaxIO {
		return nil, fmt.Errorf(
			"%w: invalid PerformanceMode %q, must be generalPurpose or maxIO",
			ErrValidation,
			req.PerformanceMode,
		)
	}
	if req.ThroughputMode != throughputModeBursting &&
		req.ThroughputMode != throughputModeProvisioned &&
		req.ThroughputMode != throughputModeElastic {
		return nil, fmt.Errorf(
			"%w: invalid ThroughputMode %q, must be bursting, provisioned, or elastic",
			ErrValidation,
			req.ThroughputMode,
		)
	}

	if req.ThroughputMode == throughputModeProvisioned {
		if req.ProvisionedThroughputMib < 1 || req.ProvisionedThroughputMib > 1024 {
			return nil, fmt.Errorf(
				"%w: ProvisionedThroughputInMibps must be between 1 and 1024 when ThroughputMode is provisioned, got %g",
				ErrValidation,
				req.ProvisionedThroughputMib,
			)
		}
	} else if req.ProvisionedThroughputMib != 0 {
		return nil, fmt.Errorf(
			"%w: ProvisionedThroughputInMibps is only valid when ThroughputMode is provisioned",
			ErrValidation,
		)
	}

	// KmsKeyId handling: auto-fill managed key when Encrypted=true, reject when Encrypted=false.
	kmsKeyId := req.KmsKeyId
	if req.Encrypted && kmsKeyId == "" {
		kmsKeyId = managedKMSKeyARN
	}
	if !req.Encrypted && kmsKeyId != "" {
		return nil, fmt.Errorf(
			"%w: KmsKeyId can only be specified when Encrypted is true",
			ErrValidation,
		)
	}

	id := "fs-" + uuid.NewString()[:8]
	fsARN := arn.Build("elasticfilesystem", b.region, b.accountID, "file-system/"+id)
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
		KmsKeyId:                       kmsKeyId,
		AvailabilityZoneName:           req.AvailabilityZoneName,
		ProvisionedThroughputMib:       req.ProvisionedThroughputMib,
		ReplicationOverwriteProtection: protectionDisabled,
		AccountID:                      b.accountID,
		Region:                         b.region,
		CreationTime:                   time.Now().UTC(),
		Tags:                           t,
	}
	b.fileSystems[id] = fs
	b.fileSystemsByARN[fsARN] = fs
	cp := *fs

	return &cp, nil
}

// DescribeFileSystems returns file systems, optionally filtered by ID, with pagination support.
func (b *InMemoryBackend) DescribeFileSystems(fileSystemID, marker string, maxItems int) ([]*FileSystem, string, error) {
	b.mu.RLock("DescribeFileSystems")
	defer b.mu.RUnlock()

	if fileSystemID != "" {
		fs, ok := b.fileSystems[fileSystemID]
		if !ok {
			return []*FileSystem{}, "", nil
		}
		cp := *fs

		return []*FileSystem{&cp}, "", nil
	}

	all := make([]*FileSystem, 0, len(b.fileSystems))
	for _, fs := range b.fileSystems {
		cp := *fs
		all = append(all, &cp)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].FileSystemID < all[j].FileSystemID })

	return paginate(all, marker, maxItems, func(fs *FileSystem) string { return fs.FileSystemID })
}

// DeleteFileSystem deletes a file system by ID.
// Returns ErrFileSystemInUse if any mount targets exist.
func (b *InMemoryBackend) DeleteFileSystem(fileSystemID string) error {
	b.mu.Lock("DeleteFileSystem")
	defer b.mu.Unlock()

	fs, ok := b.fileSystems[fileSystemID]
	if !ok {
		return fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	// Reject delete if mount targets or access points exist (AWS: FileSystemInUse).
	for _, mt := range b.mountTargets {
		if mt.FileSystemID == fileSystemID {
			return fmt.Errorf(
				"%w: file system %s has existing mount targets",
				ErrFileSystemInUse,
				fileSystemID,
			)
		}
	}
	for _, ap := range b.accessPoints {
		if ap.FileSystemID == fileSystemID {
			return fmt.Errorf(
				"%w: file system %s has existing access points",
				ErrFileSystemInUse,
				fileSystemID,
			)
		}
	}

	delete(b.fileSystemsByARN, fs.FileSystemArn)
	fs.Tags.Close()
	delete(b.fileSystems, fileSystemID)
	delete(b.lifecyclePolicies, fileSystemID)
	delete(b.backupPolicies, fileSystemID)
	delete(b.fileSystemPolicies, fileSystemID)
	delete(b.replicationConfigs, fileSystemID)

	return nil
}

// TagResource adds or updates tags on a resource (file system or access point) by ARN or ID.
func (b *InMemoryBackend) TagResource(resourceID string, kv map[string]string) error {
	if err := validateTags(kv); err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if fs, ok := b.fileSystems[resourceID]; ok {
		fs.Tags.Merge(kv)

		return nil
	}
	if fs, ok := b.fileSystemsByARN[resourceID]; ok {
		fs.Tags.Merge(kv)

		return nil
	}

	if ap, ok := b.accessPoints[resourceID]; ok {
		ap.Tags.Merge(kv)

		return nil
	}
	if ap, ok := b.accessPointsByARN[resourceID]; ok {
		ap.Tags.Merge(kv)

		return nil
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceID)
}

// UntagResource removes tags from a resource (file system or access point) by ARN or ID.
func (b *InMemoryBackend) UntagResource(resourceID string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if fs, ok := b.fileSystems[resourceID]; ok {
		fs.Tags.DeleteKeys(tagKeys)

		return nil
	}
	if fs, ok := b.fileSystemsByARN[resourceID]; ok {
		fs.Tags.DeleteKeys(tagKeys)

		return nil
	}

	if ap, ok := b.accessPoints[resourceID]; ok {
		ap.Tags.DeleteKeys(tagKeys)

		return nil
	}
	if ap, ok := b.accessPointsByARN[resourceID]; ok {
		ap.Tags.DeleteKeys(tagKeys)

		return nil
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceID)
}

// ListTagsForResource lists tags for a resource by ID or ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceID string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if fs, ok := b.fileSystems[resourceID]; ok {
		return fs.Tags.Clone(), nil
	}
	if fs, ok := b.fileSystemsByARN[resourceID]; ok {
		return fs.Tags.Clone(), nil
	}

	if ap, ok := b.accessPoints[resourceID]; ok {
		return ap.Tags.Clone(), nil
	}
	if ap, ok := b.accessPointsByARN[resourceID]; ok {
		return ap.Tags.Clone(), nil
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceID)
}

// CreateMountTarget creates a mount target for a file system.
// Returns ErrMountTargetConflict if a mount target already exists in the same subnet.
func (b *InMemoryBackend) CreateMountTarget(req CreateMountTargetRequest) (*MountTarget, error) {
	b.mu.Lock("CreateMountTarget")
	defer b.mu.Unlock()

	fs, ok := b.fileSystems[req.FileSystemID]
	if !ok {
		return nil, fmt.Errorf("%w: file system %s not found", ErrNotFound, req.FileSystemID)
	}

	// One mount target per subnet per file system.
	if req.SubnetID != "" {
		for _, mt := range b.mountTargets {
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
	mtARN := arn.Build("elasticfilesystem", b.region, b.accountID, "mount-target/"+id)
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
	b.mountTargets[id] = mt
	b.mountTargetsByARN[mtARN] = mt
	fs.NumberOfMountTargets++

	cp := *mt
	cp.SecurityGroups = make([]string, len(mt.SecurityGroups))
	copy(cp.SecurityGroups, mt.SecurityGroups)

	return &cp, nil
}

// DescribeMountTargets returns mount targets, optionally filtered by file system ID or mount target ID.
func (b *InMemoryBackend) DescribeMountTargets(
	fileSystemID, mountTargetID, marker string, maxItems int,
) ([]*MountTarget, string, error) {
	b.mu.RLock("DescribeMountTargets")
	defer b.mu.RUnlock()

	if mountTargetID != "" {
		mt, ok := b.mountTargets[mountTargetID]
		if !ok {
			return nil, "", fmt.Errorf(
				"%w: mount target %s not found",
				ErrMountTargetNotFound,
				mountTargetID,
			)
		}
		cp := copyMountTarget(mt)

		return []*MountTarget{cp}, "", nil
	}

	all := make([]*MountTarget, 0, len(b.mountTargets))
	for _, mt := range b.mountTargets {
		if fileSystemID != "" && mt.FileSystemID != fileSystemID {
			continue
		}
		all = append(all, copyMountTarget(mt))
	}
	sort.Slice(all, func(i, j int) bool { return all[i].MountTargetID < all[j].MountTargetID })

	return paginate(all, marker, maxItems, func(mt *MountTarget) string { return mt.MountTargetID })
}

func copyMountTarget(mt *MountTarget) *MountTarget {
	cp := *mt
	cp.SecurityGroups = make([]string, len(mt.SecurityGroups))
	copy(cp.SecurityGroups, mt.SecurityGroups)

	return &cp
}

// DeleteMountTarget deletes a mount target by ID.
func (b *InMemoryBackend) DeleteMountTarget(mountTargetID string) error {
	b.mu.Lock("DeleteMountTarget")
	defer b.mu.Unlock()

	mt, ok := b.mountTargets[mountTargetID]
	if !ok {
		return fmt.Errorf("%w: mount target %s not found", ErrMountTargetNotFound, mountTargetID)
	}
	if fs, found := b.fileSystems[mt.FileSystemID]; found {
		fs.NumberOfMountTargets--
	}
	delete(b.mountTargetsByARN, mt.MountTargetArn)
	delete(b.mountTargets, mountTargetID)

	return nil
}

// CreateAccessPoint creates an access point for a file system.
// Supports ClientToken idempotency.
func (b *InMemoryBackend) CreateAccessPoint(req CreateAccessPointRequest) (*AccessPoint, error) {
	if err := validateTags(req.Tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateAccessPoint")
	defer b.mu.Unlock()

	// ClientToken idempotency.
	if req.ClientToken != "" {
		if existing, ok := b.accessPointsByClientToken[req.ClientToken]; ok {
			cp := copyAccessPoint(existing)
			return cp, nil
		}
	}

	if _, ok := b.fileSystems[req.FileSystemID]; !ok {
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
	apARN := arn.Build("elasticfilesystem", b.region, b.accountID, "access-point/"+id)
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
	b.accessPoints[id] = ap
	b.accessPointsByARN[apARN] = ap
	if req.ClientToken != "" {
		b.accessPointsByClientToken[req.ClientToken] = ap
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
	fileSystemID, accessPointID, marker string, maxItems int,
) ([]*AccessPoint, string, error) {
	b.mu.RLock("DescribeAccessPoints")
	defer b.mu.RUnlock()

	if accessPointID != "" {
		ap, ok := b.accessPoints[accessPointID]
		if !ok {
			return nil, "", fmt.Errorf(
				"%w: access point %s not found",
				ErrAccessPointNotFound,
				accessPointID,
			)
		}
		cp := copyAccessPoint(ap)

		return []*AccessPoint{cp}, "", nil
	}

	all := make([]*AccessPoint, 0, len(b.accessPoints))
	for _, ap := range b.accessPoints {
		if fileSystemID != "" && ap.FileSystemID != fileSystemID {
			continue
		}
		all = append(all, copyAccessPoint(ap))
	}
	sort.Slice(all, func(i, j int) bool { return all[i].AccessPointID < all[j].AccessPointID })

	return paginate(all, marker, maxItems, func(ap *AccessPoint) string { return ap.AccessPointID })
}

// DeleteAccessPoint deletes an access point by ID.
func (b *InMemoryBackend) DeleteAccessPoint(accessPointID string) error {
	b.mu.Lock("DeleteAccessPoint")
	defer b.mu.Unlock()

	ap, ok := b.accessPoints[accessPointID]
	if !ok {
		return fmt.Errorf("%w: access point %s not found", ErrAccessPointNotFound, accessPointID)
	}
	delete(b.accessPointsByARN, ap.AccessPointArn)
	if ap.ClientToken != "" {
		delete(b.accessPointsByClientToken, ap.ClientToken)
	}
	ap.Tags.Close()
	delete(b.accessPoints, accessPointID)

	return nil
}

// DescribeLifecycleConfiguration returns lifecycle policies for a file system.
func (b *InMemoryBackend) DescribeLifecycleConfiguration(
	fileSystemID string,
) ([]LifecyclePolicy, error) {
	b.mu.RLock("DescribeLifecycleConfiguration")
	defer b.mu.RUnlock()

	if _, ok := b.fileSystems[fileSystemID]; !ok {
		return nil, fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	policies := b.lifecyclePolicies[fileSystemID]
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
		if p.TransitionToIA != "" && !validTransitionToIA[p.TransitionToIA] {
			return fmt.Errorf(
				"%w: invalid TransitionToIA value %q at index %d",
				ErrValidation,
				p.TransitionToIA,
				i,
			)
		}
		if p.TransitionToPrimaryStorageClass != "" && !validTransitionToPrimary[p.TransitionToPrimaryStorageClass] {
			return fmt.Errorf(
				"%w: invalid TransitionToPrimaryStorageClass value %q at index %d",
				ErrValidation,
				p.TransitionToPrimaryStorageClass,
				i,
			)
		}
		if p.TransitionToArchive != "" && !validTransitionToArchive[p.TransitionToArchive] {
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
	fileSystemID string,
	policies []LifecyclePolicy,
) ([]LifecyclePolicy, error) {
	if err := validateLifecyclePolicies(policies); err != nil {
		return nil, err
	}

	b.mu.Lock("PutLifecycleConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.fileSystems[fileSystemID]; !ok {
		return nil, fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	stored := make([]LifecyclePolicy, len(policies))
	copy(stored, policies)
	b.lifecyclePolicies[fileSystemID] = stored

	result := make([]LifecyclePolicy, len(stored))
	copy(result, stored)

	return result, nil
}

// CreateReplicationConfiguration creates a replication configuration for a file system.
func (b *InMemoryBackend) CreateReplicationConfiguration(
	sourceFileSystemID string,
	destinations []ReplicationDestination,
) (*ReplicationConfiguration, error) {
	b.mu.Lock("CreateReplicationConfiguration")
	defer b.mu.Unlock()

	fs, ok := b.fileSystems[sourceFileSystemID]
	if !ok {
		return nil, fmt.Errorf("%w: file system %s not found", ErrNotFound, sourceFileSystemID)
	}

	if _, exists := b.replicationConfigs[sourceFileSystemID]; exists {
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
	}

	rc := &ReplicationConfiguration{
		OriginalSourceFileSystemARN: fs.FileSystemArn,
		SourceFileSystemARN:         fs.FileSystemArn,
		SourceFileSystemID:          sourceFileSystemID,
		SourceFileSystemRegion:      b.region,
		CreationTime:                time.Now().UTC().Unix(),
		Destinations:                dests,
	}
	b.replicationConfigs[sourceFileSystemID] = rc

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
func (b *InMemoryBackend) DeleteReplicationConfiguration(sourceFileSystemID string) error {
	b.mu.Lock("DeleteReplicationConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.fileSystems[sourceFileSystemID]; !ok {
		return fmt.Errorf("%w: file system %s not found", ErrNotFound, sourceFileSystemID)
	}

	if _, exists := b.replicationConfigs[sourceFileSystemID]; !exists {
		return fmt.Errorf(
			"%w: replication configuration not found for file system %s",
			ErrNotFound,
			sourceFileSystemID,
		)
	}

	delete(b.replicationConfigs, sourceFileSystemID)

	// Reset source protection to DISABLED.
	if fs, ok := b.fileSystems[sourceFileSystemID]; ok {
		fs.ReplicationOverwriteProtection = protectionDisabled
	}

	return nil
}

// DescribeReplicationConfigurations returns replication configurations, optionally filtered by file system ID.
func (b *InMemoryBackend) DescribeReplicationConfigurations(
	fileSystemID string,
) ([]*ReplicationConfiguration, error) {
	b.mu.RLock("DescribeReplicationConfigurations")
	defer b.mu.RUnlock()

	if fileSystemID != "" {
		rc, ok := b.replicationConfigs[fileSystemID]
		if !ok {
			return []*ReplicationConfiguration{}, nil
		}

		cp := *rc
		cp.Destinations = make([]ReplicationDestination, len(rc.Destinations))
		copy(cp.Destinations, rc.Destinations)

		return []*ReplicationConfiguration{&cp}, nil
	}

	list := make([]*ReplicationConfiguration, 0, len(b.replicationConfigs))
	for _, rc := range b.replicationConfigs {
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
func (b *InMemoryBackend) CreateTags(fileSystemID string, kv map[string]string) error {
	b.mu.Lock("CreateTags")
	defer b.mu.Unlock()

	fs, ok := b.fileSystems[fileSystemID]
	if !ok {
		return fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	fs.Tags.Merge(kv)

	return nil
}

// DeleteTags removes tags from a file system by key (legacy operation).
func (b *InMemoryBackend) DeleteTags(fileSystemID string, tagKeys []string) error {
	b.mu.Lock("DeleteTags")
	defer b.mu.Unlock()

	fs, ok := b.fileSystems[fileSystemID]
	if !ok {
		return fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	fs.Tags.DeleteKeys(tagKeys)

	return nil
}

// DescribeFileSystemPolicy returns the resource-based policy for a file system.
func (b *InMemoryBackend) DescribeFileSystemPolicy(fileSystemID string) (string, error) {
	b.mu.RLock("DescribeFileSystemPolicy")
	defer b.mu.RUnlock()

	if _, ok := b.fileSystems[fileSystemID]; !ok {
		return "", fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	policy, ok := b.fileSystemPolicies[fileSystemID]
	if !ok {
		return "", fmt.Errorf("%w: no policy found for file system %s", ErrNotFound, fileSystemID)
	}

	return policy, nil
}

// DeleteFileSystemPolicy removes the resource-based policy from a file system.
func (b *InMemoryBackend) DeleteFileSystemPolicy(fileSystemID string) error {
	b.mu.Lock("DeleteFileSystemPolicy")
	defer b.mu.Unlock()

	if _, ok := b.fileSystems[fileSystemID]; !ok {
		return fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	delete(b.fileSystemPolicies, fileSystemID)

	return nil
}

// DescribeAccountPreferences returns the current account preferences.
func (b *InMemoryBackend) DescribeAccountPreferences() AccountPreferences {
	b.mu.RLock("DescribeAccountPreferences")
	defer b.mu.RUnlock()

	return b.accountPreferences
}

// DescribeBackupPolicy returns the backup policy for a file system.
func (b *InMemoryBackend) DescribeBackupPolicy(fileSystemID string) (string, error) {
	b.mu.RLock("DescribeBackupPolicy")
	defer b.mu.RUnlock()

	if _, ok := b.fileSystems[fileSystemID]; !ok {
		return "", fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	status, ok := b.backupPolicies[fileSystemID]
	if !ok {
		return backupStatusDisabled, nil
	}

	return status, nil
}

// DescribeMountTargetSecurityGroups returns the security groups for a mount target.
func (b *InMemoryBackend) DescribeMountTargetSecurityGroups(
	mountTargetID string,
) ([]string, error) {
	b.mu.RLock("DescribeMountTargetSecurityGroups")
	defer b.mu.RUnlock()

	mt, ok := b.mountTargets[mountTargetID]
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
func (b *InMemoryBackend) PutBackupPolicy(fileSystemID, status string) error {
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

	b.mu.Lock("PutBackupPolicy")
	defer b.mu.Unlock()

	if _, ok := b.fileSystems[fileSystemID]; !ok {
		return fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	b.backupPolicies[fileSystemID] = status

	return nil
}

// PutFileSystemPolicy sets the resource-based policy for a file system.
// The policy must be valid JSON and no larger than 20 KB.
func (b *InMemoryBackend) PutFileSystemPolicy(fileSystemID, policy string) error {
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

	b.mu.Lock("PutFileSystemPolicy")
	defer b.mu.Unlock()

	if _, ok := b.fileSystems[fileSystemID]; !ok {
		return fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	b.fileSystemPolicies[fileSystemID] = policy

	return nil
}

// UpdateFileSystem updates throughput settings for a file system.
// Enforces a 24-hour cooldown between throughput mode changes.
func (b *InMemoryBackend) UpdateFileSystem(
	fileSystemID string,
	req UpdateFileSystemRequest,
) (*FileSystem, error) {
	b.mu.Lock("UpdateFileSystem")
	defer b.mu.Unlock()

	fs, ok := b.fileSystems[fileSystemID]
	if !ok {
		return nil, fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	if req.ThroughputMode != "" {
		if req.ThroughputMode != throughputModeBursting &&
			req.ThroughputMode != throughputModeProvisioned &&
			req.ThroughputMode != throughputModeElastic {
			return nil, fmt.Errorf(
				"%w: invalid ThroughputMode %q, must be bursting, provisioned, or elastic",
				ErrValidation,
				req.ThroughputMode,
			)
		}

		// Enforce cooldown on throughput changes.
		if !fs.LastThroughputChange.IsZero() &&
			time.Since(fs.LastThroughputChange) < throughputCooldown {
			return nil, fmt.Errorf(
				"%w: throughput mode was last changed at %s; must wait 24 hours between changes",
				ErrTooManyRequests,
				fs.LastThroughputChange.Format(time.RFC3339),
			)
		}

		if req.ThroughputMode == throughputModeProvisioned {
			if req.ProvisionedThroughputMib < 1 || req.ProvisionedThroughputMib > 1024 {
				return nil, fmt.Errorf(
					"%w: ProvisionedThroughputInMibps must be between 1 and 1024 when ThroughputMode is provisioned, got %g",
					ErrValidation,
					req.ProvisionedThroughputMib,
				)
			}
		}

		fs.ThroughputMode = req.ThroughputMode
		fs.LastThroughputChange = time.Now().UTC()
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

	b.mu.Lock("ModifyMountTargetSecurityGroups")
	defer b.mu.Unlock()

	mt, ok := b.mountTargets[mountTargetID]
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

	b.mu.Lock("UpdateFileSystemProtection")
	defer b.mu.Unlock()

	fs, ok := b.fileSystems[fileSystemID]
	if !ok {
		return fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	fs.ReplicationOverwriteProtection = replicationOverwriteProtection

	return nil
}

// paginate applies cursor-based pagination to a sorted slice.
// Items after marker are returned up to maxItems. nextToken is non-empty when more items remain.
func paginate[T any](items []T, marker string, maxItems int, keyFn func(T) string) ([]T, string, error) {
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

// AddFileSystemInternal inserts a pre-built FileSystem directly into the backend (test seed helper).
func (b *InMemoryBackend) AddFileSystemInternal(fs *FileSystem) {
	b.mu.Lock("AddFileSystemInternal")
	defer b.mu.Unlock()

	b.fileSystems[fs.FileSystemID] = fs
	b.fileSystemsByARN[fs.FileSystemArn] = fs
}

// AddMountTargetInternal inserts a pre-built MountTarget directly into the backend (test seed helper).
func (b *InMemoryBackend) AddMountTargetInternal(mt *MountTarget) {
	b.mu.Lock("AddMountTargetInternal")
	defer b.mu.Unlock()

	b.mountTargets[mt.MountTargetID] = mt
	b.mountTargetsByARN[mt.MountTargetArn] = mt
}

// AddAccessPointInternal inserts a pre-built AccessPoint directly into the backend (test seed helper).
func (b *InMemoryBackend) AddAccessPointInternal(ap *AccessPoint) {
	b.mu.Lock("AddAccessPointInternal")
	defer b.mu.Unlock()

	b.accessPoints[ap.AccessPointID] = ap
	b.accessPointsByARN[ap.AccessPointArn] = ap
}
