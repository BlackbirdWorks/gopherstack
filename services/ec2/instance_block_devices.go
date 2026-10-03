package ec2

import "fmt"

// BlockDeviceUpdate is one ModifyInstanceAttribute BlockDeviceMapping entry.
type BlockDeviceUpdate struct {
	DeleteOnTermination *bool
	DeviceName          string
	VolumeID            string
}

// SetInstancesHibernation records RunInstances HibernationOptions.Configured.
func (b *InMemoryBackend) SetInstancesHibernation(ids []string, configured bool) error {
	b.mu.Lock("SetInstancesHibernation")
	defer b.mu.Unlock()

	for _, id := range ids {
		inst, ok := b.instances.Get(id)
		if !ok {
			return fmt.Errorf("%w: %s", ErrInstanceNotFound, id)
		}

		inst.HibernationConfigured = configured
	}

	return nil
}

// ModifyInstanceBlockDeviceMappings applies Ebs.DeleteOnTermination to the
// volumes attached to the instance, matched by device name or volume ID.
func (b *InMemoryBackend) ModifyInstanceBlockDeviceMappings(instanceID string, updates []BlockDeviceUpdate) error {
	b.mu.Lock("ModifyInstanceBlockDeviceMappings")
	defer b.mu.Unlock()

	if _, ok := b.instances.Get(instanceID); !ok {
		return fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	targets := make([]*VolumeAttachment, len(updates))

	for i, u := range updates {
		att := b.findInstanceAttachmentLocked(instanceID, u)
		if att == nil {
			return fmt.Errorf("%w: no block device %q is attached to instance %s",
				ErrInvalidParameter, u.DeviceName, instanceID)
		}

		targets[i] = att
	}

	for i, u := range updates {
		if u.DeleteOnTermination != nil {
			targets[i].DeleteOnTermination = *u.DeleteOnTermination
		}
	}

	return nil
}

func (b *InMemoryBackend) findInstanceAttachmentLocked(instanceID string, u BlockDeviceUpdate) *VolumeAttachment {
	for _, vol := range b.volumes.All() {
		att := vol.Attachment
		if att == nil || att.InstanceID != instanceID {
			continue
		}

		if (u.VolumeID != "" && att.VolumeID == u.VolumeID) ||
			(u.VolumeID == "" && att.Device == u.DeviceName) {
			return att
		}
	}

	return nil
}

// InstanceBlockDevices returns the volume attachments of the given instances
// (copies), keyed by instance ID.
func (b *InMemoryBackend) InstanceBlockDevices(instanceIDs []string) map[string][]VolumeAttachment {
	b.mu.RLock("InstanceBlockDevices")
	defer b.mu.RUnlock()

	if b.volumes.Len() == 0 || len(instanceIDs) == 0 {
		return nil
	}

	want := make(map[string]struct{}, len(instanceIDs))
	for _, id := range instanceIDs {
		want[id] = struct{}{}
	}

	out := make(map[string][]VolumeAttachment)

	for _, vol := range b.volumes.All() {
		att := vol.Attachment
		if att == nil {
			continue
		}

		if _, ok := want[att.InstanceID]; ok {
			out[att.InstanceID] = append(out[att.InstanceID], *att)
		}
	}

	return out
}
