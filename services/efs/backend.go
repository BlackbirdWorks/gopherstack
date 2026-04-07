package efs

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("FileSystemNotFound", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource with the same token already exists.
	ErrAlreadyExists = awserr.New("FileSystemAlreadyExists", awserr.ErrConflict)
	// ErrMountTargetNotFound is returned when a requested mount target does not exist.
	ErrMountTargetNotFound = awserr.New("MountTargetNotFound", awserr.ErrNotFound)
	// ErrAccessPointNotFound is returned when a requested access point does not exist.
	ErrAccessPointNotFound = awserr.New("AccessPointNotFound", awserr.ErrNotFound)
)

// FileSystem represents an EFS file system.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via TagResource / CreateFileSystem.
type FileSystem struct {
	CreationTime         time.Time  `json:"creationTime"`
	Tags                 *tags.Tags `json:"tags,omitempty"`
	PerformanceMode      string     `json:"performanceMode"`
	FileSystemArn        string     `json:"fileSystemArn"`
	CreationToken        string     `json:"creationToken"`
	Name                 string     `json:"name,omitempty"`
	FileSystemID         string     `json:"fileSystemId"`
	ThroughputMode       string     `json:"throughputMode"`
	LifeCycleState       string     `json:"lifeCycleState"`
	AccountID            string     `json:"accountId"`
	Region               string     `json:"region"`
	NumberOfMountTargets int32      `json:"numberOfMountTargets"`
	Encrypted            bool       `json:"encrypted"`
}

// MountTarget represents an EFS mount target.
type MountTarget struct {
	MountTargetID        string `json:"mountTargetId"`
	FileSystemID         string `json:"fileSystemId"`
	SubnetID             string `json:"subnetId"`
	VPCID                string `json:"vpcId"`
	AvailabilityZoneName string `json:"availabilityZoneName"`
	IPAddress            string `json:"ipAddress"`
	LifeCycleState       string `json:"lifeCycleState"`
	OwnerID              string `json:"ownerId"`
}

// AccessPoint represents an EFS access point.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via TagResource.
type AccessPoint struct {
	AccessPointID  string     `json:"accessPointId"`
	AccessPointArn string     `json:"accessPointArn"`
	FileSystemID   string     `json:"fileSystemId"`
	Name           string     `json:"name,omitempty"`
	LifeCycleState string     `json:"lifeCycleState"`
	Tags           *tags.Tags `json:"tags,omitempty"`
	OwnerID        string     `json:"ownerId"`
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

// InMemoryBackend is the in-memory store for EFS resources.
type InMemoryBackend struct {
	fileSystems               map[string]*FileSystem
	mountTargets              map[string]*MountTarget
	accessPoints              map[string]*AccessPoint
	lifecyclePolicies         map[string][]LifecyclePolicy
	replicationConfigs        map[string]*ReplicationConfiguration
	backupPolicies            map[string]string
	fileSystemPolicies        map[string]string
	mountTargetSecurityGroups map[string][]string
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
		mountTargetSecurityGroups: make(map[string][]string),
		accountPreferences:        AccountPreferences{ResourceIDType: "LONG_ID"},
		accountID:                 accountID,
		region:                    region,
		mu:                        lockmetrics.New("efs"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// CreateFileSystem creates a new EFS file system.
func (b *InMemoryBackend) CreateFileSystem(
	creationToken, performanceMode, throughputMode string,
	encrypted bool,
	kv map[string]string,
) (*FileSystem, error) {
	b.mu.Lock("CreateFileSystem")
	defer b.mu.Unlock()

	// Idempotency: if creationToken already used, return the existing file system.
	for _, fs := range b.fileSystems {
		if fs.CreationToken == creationToken {
			cp := *fs

			return &cp, fmt.Errorf("%w: file system with token %s already exists", ErrAlreadyExists, creationToken)
		}
	}

	if performanceMode == "" {
		performanceMode = "generalPurpose"
	}
	if throughputMode == "" {
		throughputMode = "bursting"
	}

	id := "fs-" + uuid.NewString()[:8]
	fsARN := arn.Build("elasticfilesystem", b.region, b.accountID, "file-system/"+id)
	t := tags.New("efs.filesystem." + id + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	// Derive name from tags if present.
	name := kv["Name"]

	fs := &FileSystem{
		FileSystemID:    id,
		FileSystemArn:   fsARN,
		CreationToken:   creationToken,
		Name:            name,
		PerformanceMode: performanceMode,
		ThroughputMode:  throughputMode,
		LifeCycleState:  "available",
		Encrypted:       encrypted,
		AccountID:       b.accountID,
		Region:          b.region,
		CreationTime:    time.Now().UTC(),
		Tags:            t,
	}
	b.fileSystems[id] = fs
	cp := *fs

	return &cp, nil
}

// DescribeFileSystems returns all file systems, optionally filtered by ID.
func (b *InMemoryBackend) DescribeFileSystems(fileSystemID string) ([]*FileSystem, error) {
	b.mu.RLock("DescribeFileSystems")
	defer b.mu.RUnlock()

	if fileSystemID != "" {
		fs, ok := b.fileSystems[fileSystemID]
		if !ok {
			return nil, fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
		}
		cp := *fs

		return []*FileSystem{&cp}, nil
	}

	list := make([]*FileSystem, 0, len(b.fileSystems))
	for _, fs := range b.fileSystems {
		cp := *fs
		list = append(list, &cp)
	}

	return list, nil
}

// DeleteFileSystem deletes a file system by ID.
func (b *InMemoryBackend) DeleteFileSystem(fileSystemID string) error {
	b.mu.Lock("DeleteFileSystem")
	defer b.mu.Unlock()

	fs, ok := b.fileSystems[fileSystemID]
	if !ok {
		return fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}
	fs.Tags.Close()
	delete(b.fileSystems, fileSystemID)
	delete(b.lifecyclePolicies, fileSystemID)

	for id, mt := range b.mountTargets {
		if mt.FileSystemID == fileSystemID {
			delete(b.mountTargets, id)
		}
	}

	return nil
}

// TagResource adds or updates tags on a resource (file system or access point) by ARN or ID.
func (b *InMemoryBackend) TagResource(resourceID string, kv map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if fs, ok := b.fileSystems[resourceID]; ok {
		fs.Tags.Merge(kv)

		return nil
	}
	for _, fs := range b.fileSystems {
		if fs.FileSystemArn == resourceID {
			fs.Tags.Merge(kv)

			return nil
		}
	}

	if ap, ok := b.accessPoints[resourceID]; ok {
		ap.Tags.Merge(kv)

		return nil
	}
	for _, ap := range b.accessPoints {
		if ap.AccessPointArn == resourceID {
			ap.Tags.Merge(kv)

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceID)
}

// ListTagsForResource lists tags for a resource by ID.
func (b *InMemoryBackend) ListTagsForResource(resourceID string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if fs, ok := b.fileSystems[resourceID]; ok {
		return fs.Tags.Clone(), nil
	}
	for _, fs := range b.fileSystems {
		if fs.FileSystemArn == resourceID {
			return fs.Tags.Clone(), nil
		}
	}

	if ap, ok := b.accessPoints[resourceID]; ok {
		return ap.Tags.Clone(), nil
	}
	for _, ap := range b.accessPoints {
		if ap.AccessPointArn == resourceID {
			return ap.Tags.Clone(), nil
		}
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceID)
}

// CreateMountTarget creates a mount target for a file system.
func (b *InMemoryBackend) CreateMountTarget(fileSystemID, subnetID, ipAddress string) (*MountTarget, error) {
	b.mu.Lock("CreateMountTarget")
	defer b.mu.Unlock()

	fs, ok := b.fileSystems[fileSystemID]
	if !ok {
		return nil, fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	id := "fsmt-" + uuid.NewString()[:8]
	mt := &MountTarget{
		MountTargetID:  id,
		FileSystemID:   fileSystemID,
		SubnetID:       subnetID,
		IPAddress:      ipAddress,
		LifeCycleState: "available",
		OwnerID:        b.accountID,
	}
	b.mountTargets[id] = mt
	fs.NumberOfMountTargets++

	cp := *mt

	return &cp, nil
}

// DescribeMountTargets returns mount targets, optionally filtered by file system ID or mount target ID.
func (b *InMemoryBackend) DescribeMountTargets(fileSystemID, mountTargetID string) ([]*MountTarget, error) {
	b.mu.RLock("DescribeMountTargets")
	defer b.mu.RUnlock()

	if mountTargetID != "" {
		mt, ok := b.mountTargets[mountTargetID]
		if !ok {
			return nil, fmt.Errorf("%w: mount target %s not found", ErrMountTargetNotFound, mountTargetID)
		}
		cp := *mt

		return []*MountTarget{&cp}, nil
	}

	list := make([]*MountTarget, 0, len(b.mountTargets))
	for _, mt := range b.mountTargets {
		if fileSystemID != "" && mt.FileSystemID != fileSystemID {
			continue
		}
		cp := *mt
		list = append(list, &cp)
	}

	return list, nil
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
	delete(b.mountTargets, mountTargetID)

	return nil
}

// CreateAccessPoint creates an access point for a file system.
func (b *InMemoryBackend) CreateAccessPoint(fileSystemID string, kv map[string]string) (*AccessPoint, error) {
	b.mu.Lock("CreateAccessPoint")
	defer b.mu.Unlock()

	if _, ok := b.fileSystems[fileSystemID]; !ok {
		return nil, fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	id := "fsap-" + uuid.NewString()[:8]
	apARN := arn.Build("elasticfilesystem", b.region, b.accountID, "access-point/"+id)
	t := tags.New("efs.accesspoint." + id + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	name := kv["Name"]

	ap := &AccessPoint{
		AccessPointID:  id,
		AccessPointArn: apARN,
		FileSystemID:   fileSystemID,
		Name:           name,
		LifeCycleState: "available",
		Tags:           t,
		OwnerID:        b.accountID,
	}
	b.accessPoints[id] = ap
	cp := *ap

	return &cp, nil
}

// DescribeAccessPoints returns access points, optionally filtered by file system ID or access point ID.
func (b *InMemoryBackend) DescribeAccessPoints(fileSystemID, accessPointID string) ([]*AccessPoint, error) {
	b.mu.RLock("DescribeAccessPoints")
	defer b.mu.RUnlock()

	if accessPointID != "" {
		ap, ok := b.accessPoints[accessPointID]
		if !ok {
			return nil, fmt.Errorf("%w: access point %s not found", ErrAccessPointNotFound, accessPointID)
		}
		cp := *ap

		return []*AccessPoint{&cp}, nil
	}

	list := make([]*AccessPoint, 0, len(b.accessPoints))
	for _, ap := range b.accessPoints {
		if fileSystemID != "" && ap.FileSystemID != fileSystemID {
			continue
		}
		cp := *ap
		list = append(list, &cp)
	}

	return list, nil
}

// DeleteAccessPoint deletes an access point by ID.
func (b *InMemoryBackend) DeleteAccessPoint(accessPointID string) error {
	b.mu.Lock("DeleteAccessPoint")
	defer b.mu.Unlock()

	ap, ok := b.accessPoints[accessPointID]
	if !ok {
		return fmt.Errorf("%w: access point %s not found", ErrAccessPointNotFound, accessPointID)
	}
	ap.Tags.Close()
	delete(b.accessPoints, accessPointID)

	return nil
}

// DescribeLifecycleConfiguration returns lifecycle policies for a file system.
func (b *InMemoryBackend) DescribeLifecycleConfiguration(fileSystemID string) ([]LifecyclePolicy, error) {
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

// PutLifecycleConfiguration sets lifecycle policies for a file system.
func (b *InMemoryBackend) PutLifecycleConfiguration(
	fileSystemID string,
	policies []LifecyclePolicy,
) ([]LifecyclePolicy, error) {
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

	cp := *rc
	cp.Destinations = make([]ReplicationDestination, len(rc.Destinations))
	copy(cp.Destinations, rc.Destinations)

	return &cp, nil
}

// DeleteReplicationConfiguration deletes the replication configuration for a file system.
func (b *InMemoryBackend) DeleteReplicationConfiguration(sourceFileSystemID string) error {
	b.mu.Lock("DeleteReplicationConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.fileSystems[sourceFileSystemID]; !ok {
		return fmt.Errorf("%w: file system %s not found", ErrNotFound, sourceFileSystemID)
	}

	if _, exists := b.replicationConfigs[sourceFileSystemID]; !exists {
		return fmt.Errorf("%w: replication configuration not found for file system %s", ErrNotFound, sourceFileSystemID)
	}

	delete(b.replicationConfigs, sourceFileSystemID)

	return nil
}

// DescribeReplicationConfigurations returns replication configurations, optionally filtered by file system ID.
func (b *InMemoryBackend) DescribeReplicationConfigurations(fileSystemID string) ([]*ReplicationConfiguration, error) {
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
		return "DISABLED", nil
	}

	return status, nil
}

// DescribeMountTargetSecurityGroups returns the security groups for a mount target.
func (b *InMemoryBackend) DescribeMountTargetSecurityGroups(mountTargetID string) ([]string, error) {
	b.mu.RLock("DescribeMountTargetSecurityGroups")
	defer b.mu.RUnlock()

	if _, ok := b.mountTargets[mountTargetID]; !ok {
		return nil, fmt.Errorf("%w: mount target %s not found", ErrMountTargetNotFound, mountTargetID)
	}

	groups := b.mountTargetSecurityGroups[mountTargetID]
	if groups == nil {
		return []string{}, nil
	}

	result := make([]string, len(groups))
	copy(result, groups)

	return result, nil
}
