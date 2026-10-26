package dlm

import (
	"errors"
	"time"
)

const (
	kindSnapshot = "snapshot"
	kindImage    = "image"
)

// ErrResourceNotFound is returned by a ComputeService when the resource is already gone.
var ErrResourceNotFound = errors.New("dlm: resource not found")

// Target is a volume or instance a policy can act on.
type Target struct {
	Tags       map[string]string
	ID         string
	VolumeType string
	InstanceID string
	VolumeIDs  []string
}

// ComputeService is the EC2 seam DLM drives: it lists targets and creates, tags and deletes snapshots and AMIs.
type ComputeService interface {
	Targets(region, resourceType string) []Target
	CreateSnapshot(region, volumeID, description string, tags map[string]string) (string, error)
	CreateImage(region, instanceID, name, description string, tags map[string]string) (string, error)
	Exists(region, kind, id string) bool
	Delete(region, kind, id string) error
	// CopySnapshot copies a snapshot of srcRegion into dstRegion and tags the copy.
	CopySnapshot(
		srcRegion, dstRegion, snapshotID, description string, encrypted bool, kmsKey string, tags map[string]string,
	) (string, error)
	// CopyImage copies an AMI of srcRegion into dstRegion and tags the copy.
	CopyImage(
		srcRegion, dstRegion, imageID, name, description string, encrypted bool, kmsKey string, tags map[string]string,
	) (string, error)
	SetFastRestore(region, snapshotID string, zones []string, enable bool) error
	SetSnapshotSharing(region, snapshotID string, accounts []string, share bool) error
	ArchiveSnapshot(region, snapshotID string) error
	DeprecateImage(region, imageID string, at time.Time) error
}

// SetCompute wires the EC2 seam; without it schedules are evaluated but nothing is created.
func (b *InMemoryBackend) SetCompute(c ComputeService) {
	b.mu.Lock("SetCompute")
	defer b.mu.Unlock()

	b.compute = c
}

func (b *InMemoryBackend) computeService() ComputeService {
	b.mu.RLock("computeService")
	defer b.mu.RUnlock()

	return b.compute
}
