package ec2

import (
	"context"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// ComputeOwner scopes Docker resources to this backend's account and region.
func (b *InMemoryBackend) ComputeOwner() string { return b.AccountID + "/" + b.Region }

// LaunchVolumeSpecs returns the non-root EBS volumes attached to an instance.
func (b *InMemoryBackend) LaunchVolumeSpecs(instanceID string) []VolumeAttachSpec {
	b.mu.RLock("LaunchVolumeSpecs")
	defer b.mu.RUnlock()

	inst, ok := b.instances.Get(instanceID)
	if !ok {
		return nil
	}

	var out []VolumeAttachSpec

	for _, vol := range b.volumes.All() {
		att := vol.Attachment
		if att == nil || att.InstanceID != instanceID || att.Device == inst.RootDeviceName {
			continue
		}

		out = append(out, VolumeAttachSpec{VolumeID: vol.ID, Device: att.Device, Owner: b.AccountID + "/" + b.Region})
	}

	return out
}

// AttachedVolumeSpec returns a volume's attachment and the instance it is attached to.
func (b *InMemoryBackend) AttachedVolumeSpec(volumeID string) (VolumeAttachSpec, string, bool) {
	b.mu.RLock("AttachedVolumeSpec")
	defer b.mu.RUnlock()

	vol, ok := b.volumes.Get(volumeID)
	if !ok || vol.Attachment == nil {
		return VolumeAttachSpec{}, "", false
	}

	return VolumeAttachSpec{
		VolumeID: volumeID, Device: vol.Attachment.Device, Owner: b.AccountID + "/" + b.Region,
	}, vol.Attachment.InstanceID, true
}

// LiveVolumeIDs returns the ID of every EBS volume that exists.
func (b *InMemoryBackend) LiveVolumeIDs() map[string]struct{} {
	b.mu.RLock("LiveVolumeIDs")
	defer b.mu.RUnlock()

	out := make(map[string]struct{}, b.volumes.Len())
	for _, vol := range b.volumes.All() {
		out[vol.ID] = struct{}{}
	}

	return out
}

// ebsCompute returns the volume-capable provider and backend when EBS mapping is enabled.
func (h *Handler) ebsCompute() (computeCapableBackend, volumeCompute, volumeBackend) {
	cb, c := h.computeBackend()

	vc, ok := c.(volumeCompute)
	if !ok || !vc.EBSEnabled() {
		return nil, nil, nil
	}

	vb, ok := h.Backend.(volumeBackend)
	if !ok {
		return nil, nil, nil
	}

	return cb, vc, vb
}

// attachVolumeOnCompute mirrors an AttachVolume onto the instance's container.
func (h *Handler) attachVolumeOnCompute(ctx context.Context, volumeID string) error {
	cb, vc, vb := h.ebsCompute()
	if vc == nil {
		return nil
	}

	spec, instanceID, ok := vb.AttachedVolumeSpec(volumeID)
	if !ok {
		return nil
	}

	providerID := cb.LookupInstanceProviderID(instanceID)
	if providerID == "" {
		return nil
	}

	if err := vc.AttachEBSVolume(ctx, providerID, spec); err != nil {
		return fmt.Errorf("attach volume %s to container: %w", volumeID, err)
	}

	return nil
}

// detachVolumeOnCompute writes a volume's container data back before the volume is detached.
func (h *Handler) detachVolumeOnCompute(ctx context.Context, volumeID string) error {
	cb, vc, vb := h.ebsCompute()
	if vc == nil {
		return nil
	}

	spec, instanceID, ok := vb.AttachedVolumeSpec(volumeID)
	if !ok {
		return nil
	}

	providerID := cb.LookupInstanceProviderID(instanceID)
	if providerID == "" {
		return nil
	}

	if err := vc.DetachEBSVolume(ctx, providerID, spec); err != nil {
		return fmt.Errorf("detach volume %s from container: %w", volumeID, err)
	}

	return nil
}

// reapComputeVolumes removes the Docker volumes of EBS volumes that no longer exist.
func (h *Handler) reapComputeVolumes(ctx context.Context) {
	_, vc, vb := h.ebsCompute()
	if vc == nil {
		return
	}

	if removed := vc.ReapVolumes(ctx, vb.ComputeOwner(), vb.LiveVolumeIDs()); len(removed) > 0 {
		logger.Load(ctx).InfoContext(ctx, "ec2 removed docker volumes", "volumes", removed)
	}
}

// bindRegisteredImage binds a docker:// ImageLocation to a freshly registered AMI.
func (h *Handler) bindRegisteredImage(imageID, location string) error {
	ref, ok := strings.CutPrefix(location, dockerImageLocationPrefix)
	if !ok {
		return nil
	}

	ib, isIB := h.Backend.(imageBindingBackend)
	if !isIB {
		return nil
	}

	return ib.BindDockerImage(imageID, ref)
}
