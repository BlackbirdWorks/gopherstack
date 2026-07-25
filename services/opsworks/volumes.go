package opsworks

import (
	"time"

	"github.com/google/uuid"
)

// RegisterVolume registers an EBS volume with a stack.
func (b *InMemoryBackend) RegisterVolume(ec2VolumeID, stackID string) (string, error) {
	b.mu.Lock("RegisterVolume")
	defer b.mu.Unlock()

	if !b.stacks.Has(stackID) {
		return "", ErrStackNotFound
	}

	id := uuid.NewString()
	v := &storedVolume{
		RegisteredAt: time.Now().UTC(),
		VolumeID:     id,
		Ec2VolumeID:  ec2VolumeID,
		StackID:      stackID,
		Status:       volumeStatusRegistered,
		Region:       b.region,
	}
	b.volumes.Put(v)

	return id, nil
}

// DeregisterVolume removes a registered volume.
func (b *InMemoryBackend) DeregisterVolume(volumeID string) error {
	b.mu.Lock("DeregisterVolume")
	defer b.mu.Unlock()

	if !b.volumes.Delete(volumeID) {
		return ErrVolumeNotFound
	}

	return nil
}

// AssignVolume assigns a registered volume to an instance. The instance
// must belong to the same stack the volume was registered with -- a volume
// can only be registered with one stack at a time (see RegisterVolume), so
// assigning it to an instance from a different stack would silently create
// a cross-stack resource association AWS does not document as valid.
func (b *InMemoryBackend) AssignVolume(volumeID, instanceID string) error {
	b.mu.Lock("AssignVolume")
	defer b.mu.Unlock()

	v, ok := b.volumes.Get(volumeID)
	if !ok {
		return ErrVolumeNotFound
	}

	i, ok := b.instances.Get(instanceID)
	if !ok {
		return ErrInstanceNotFound
	}

	if i.StackID != v.StackID {
		return ErrValidation
	}

	v.InstanceID = instanceID

	return nil
}

// UnassignVolume removes a volume's instance assignment.
func (b *InMemoryBackend) UnassignVolume(volumeID string) error {
	b.mu.Lock("UnassignVolume")
	defer b.mu.Unlock()

	v, ok := b.volumes.Get(volumeID)
	if !ok {
		return ErrVolumeNotFound
	}

	v.InstanceID = ""

	return nil
}

// DescribeVolumes returns volumes filtered by stack, instance, RAID array,
// or IDs. RaidArrayId is accepted (matching the real DescribeVolumesInput
// shape) but never filters anything: this backend does not model RAID
// arrays at all (DescribeRaidArrays always returns empty, by design -- see
// PARITY.md's Misc family note), so no volume ever carries a RAID array
// association to filter on.
func (b *InMemoryBackend) DescribeVolumes(stackID, instanceID, _ string, volumeIDs []string) ([]*Volume, error) {
	b.mu.RLock("DescribeVolumes")
	defer b.mu.RUnlock()

	if len(volumeIDs) > 0 {
		result := make([]*Volume, 0, len(volumeIDs))
		for _, id := range volumeIDs {
			v, ok := b.volumes.Get(id)
			if !ok {
				return nil, ErrVolumeNotFound
			}
			result = append(result, v.toVolume())
		}

		return result, nil
	}

	source := stackScoped(stackID, b.volumes.All, b.volumesByStack.Get)

	result := make([]*Volume, 0, len(source))
	for _, v := range source {
		if instanceID != "" && v.InstanceID != instanceID {
			continue
		}
		result = append(result, v.toVolume())
	}

	return result, nil
}

// UpdateVolume updates a volume's name and mount point.
func (b *InMemoryBackend) UpdateVolume(volumeID, name, mountPoint string) error {
	b.mu.Lock("UpdateVolume")
	defer b.mu.Unlock()

	v, ok := b.volumes.Get(volumeID)
	if !ok {
		return ErrVolumeNotFound
	}

	if name != "" {
		v.Name = name
	}
	if mountPoint != "" {
		v.MountPoint = mountPoint
	}

	return nil
}
