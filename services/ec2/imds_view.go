package ec2

import (
	"maps"
	"slices"
	"strings"
)

// imdsInterface is one attached network interface as IMDS reports it.
type imdsInterface struct {
	SubnetCIDR string
	VPCCIDR    string
	NetworkInterface
}

// imdsView is a consistent snapshot of everything IMDS reports for one instance.
type imdsView struct {
	Tags        map[string]string
	SGNames     map[string]string
	ProfileARN  string
	KeyPublic   string
	AccountID   string
	Region      string
	Interfaces  []imdsInterface
	BlockDevice []VolumeAttachment
	Instance    Instance
}

// imdsViewOf snapshots instanceID; only pending and running instances have a reachable metadata service.
func (b *InMemoryBackend) imdsViewOf(instanceID string) (*imdsView, bool) {
	b.mu.RLock("imdsViewOf")
	defer b.mu.RUnlock()

	inst, ok := b.instances.Get(instanceID)
	if !ok || inst.State.Name != StatePending.Name && inst.State.Name != StateRunning.Name {
		return nil, false
	}

	v := &imdsView{
		Instance:  *inst,
		AccountID: b.AccountID,
		Region:    b.Region,
		Tags:      map[string]string{},
		SGNames:   map[string]string{},
	}

	maps.Copy(v.Tags, b.tags[instanceID])

	for _, sgID := range inst.SecurityGroups {
		if sg, found := b.securityGroups.Get(sgID); found {
			v.SGNames[sgID] = sg.Name
		}
	}

	v.Interfaces = b.imdsInterfacesLocked(instanceID)
	v.ProfileARN = b.associatedProfileLocked(instanceID)

	if kp, found := b.keyPairs.Get(inst.KeyName); found {
		v.KeyPublic = kp.PublicKey
	}

	for _, vol := range b.volumes.All() {
		if vol.Attachment != nil && vol.Attachment.InstanceID == instanceID {
			v.BlockDevice = append(v.BlockDevice, *vol.Attachment)
		}
	}

	slices.SortFunc(v.BlockDevice, func(a, c VolumeAttachment) int { return strings.Compare(a.Device, c.Device) })

	return v, true
}

func (b *InMemoryBackend) imdsInterfacesLocked(instanceID string) []imdsInterface {
	var out []imdsInterface

	for eniID := range b.eniIDsByInstance[instanceID] {
		eni, ok := b.networkInterfaces.Get(eniID)
		if !ok {
			continue
		}

		ifc := imdsInterface{NetworkInterface: *eni}
		if sub, found := b.subnets.Get(eni.SubnetID); found {
			ifc.SubnetCIDR = sub.CIDRBlock
		}

		if vpc, found := b.vpcs.Get(eni.VPCID); found {
			ifc.VPCCIDR = vpc.CIDRBlock
		}

		out = append(out, ifc)
	}

	slices.SortFunc(out, func(a, c imdsInterface) int {
		if a.DeviceIndex != c.DeviceIndex {
			return a.DeviceIndex - c.DeviceIndex
		}

		return strings.Compare(a.ID, c.ID)
	})

	return out
}

func (b *InMemoryBackend) associatedProfileLocked(instanceID string) string {
	for _, assoc := range b.iamAssociations.All() {
		if assoc.InstanceID == instanceID && assoc.State == stateAssociated {
			return assoc.IamInstanceProfile
		}
	}

	return ""
}
