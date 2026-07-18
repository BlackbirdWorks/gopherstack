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

// AssignVolume assigns a registered volume to an instance.
func (b *InMemoryBackend) AssignVolume(volumeID, instanceID string) error {
	b.mu.Lock("AssignVolume")
	defer b.mu.Unlock()

	v, ok := b.volumes.Get(volumeID)
	if !ok {
		return ErrVolumeNotFound
	}

	if !b.instances.Has(instanceID) {
		return ErrInstanceNotFound
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

// DescribeVolumes returns volumes filtered by instance, RAID array, or IDs.
func (b *InMemoryBackend) DescribeVolumes(instanceID, _ string, volumeIDs []string) ([]*Volume, error) {
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

	result := make([]*Volume, 0)
	for _, v := range b.volumes.All() {
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
