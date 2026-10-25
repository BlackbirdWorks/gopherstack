package dlm

import "errors"

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
