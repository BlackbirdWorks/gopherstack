package ec2

import (
	"fmt"
	"time"
)

const defaultRootDeviceName = "/dev/xvda"

// LaunchVolumeSpec is one RunInstances BlockDeviceMapping.Ebs entry.
type LaunchVolumeSpec struct {
	DeviceName          string
	VolumeType          string
	SnapshotID          string
	KmsKeyID            string
	Size                int
	Iops                int
	Throughput          int
	Encrypted           bool
	DeleteOnTermination bool
	// DeleteOnTerminationUnset defaults the flag to true for the root device only.
	DeleteOnTerminationUnset bool
}

// AttachLaunchVolumes creates and attaches the EBS volumes of a launch: one per spec, plus a
// default 8 GiB root volume when no spec targets the image's root device.
func (b *InMemoryBackend) AttachLaunchVolumes(
	instanceID string, specs []LaunchVolumeSpec, volumeTags map[string]string,
) error {
	b.mu.Lock("AttachLaunchVolumes")
	defer b.mu.Unlock()

	inst, ok := b.instances.Get(instanceID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	root := defaultRootDeviceName
	if img := b.lookupImageLocked(inst.ImageID); img != nil && img.RootDeviceName != "" {
		root = img.RootDeviceName
	}

	inst.RootDeviceName = root

	hasRoot := false

	for _, s := range specs {
		hasRoot = hasRoot || s.DeviceName == root
	}

	if !hasRoot {
		specs = append([]LaunchVolumeSpec{{DeviceName: root, DeleteOnTermination: true}}, specs...)
	}

	az := inst.Placement.AvailabilityZone
	if az == "" {
		az = b.Region + "a"
	}

	for _, s := range specs {
		volType := s.VolumeType
		if volType == "" {
			volType = volTypeDefaultGP2
		}

		size := s.Size
		if size <= 0 {
			size = 8
		}

		vol := &Volume{
			ID:         newVolumeID(),
			AZ:         az,
			VolumeType: volType,
			Size:       size,
			Iops:       s.Iops,
			Throughput: s.Throughput,
			Encrypted:  s.Encrypted,
			KmsKeyID:   s.KmsKeyID,
			SnapshotID: s.SnapshotID,
			State:      stateInUse,
			CreateTime: time.Now(),
			Attachment: &VolumeAttachment{
				InstanceID:          instanceID,
				Device:              s.DeviceName,
				State:               attachmentStateAttached,
				AttachTime:          time.Now(),
				DeleteOnTermination: s.DeleteOnTermination || (s.DeleteOnTerminationUnset && s.DeviceName == root),
			},
		}
		vol.Attachment.VolumeID = vol.ID
		b.volumes.Put(vol)
		b.setTagsLocked(vol.ID, volumeTags)
	}

	return nil
}
