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

// InMemoryBackend is the in-memory store for EFS resources.
type InMemoryBackend struct {
	fileSystems       map[string]*FileSystem
	mountTargets      map[string]*MountTarget
	accessPoints      map[string]*AccessPoint
	lifecyclePolicies map[string][]LifecyclePolicy
	mu                *lockmetrics.RWMutex
	accountID         string
	region            string
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
		fileSystems:       make(map[string]*FileSystem),
		mountTargets:      make(map[string]*MountTarget),
		accessPoints:      make(map[string]*AccessPoint),
		lifecyclePolicies: make(map[string][]LifecyclePolicy),
		accountID:         accountID,
		region:            region,
		mu:                lockmetrics.New("efs"),
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

	if _, ok := b.fileSystems[fileSystemID]; !ok {
		return fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}
	delete(b.fileSystems, fileSystemID)

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

	if _, ok := b.accessPoints[accessPointID]; !ok {
		return fmt.Errorf("%w: access point %s not found", ErrAccessPointNotFound, accessPointID)
	}
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
