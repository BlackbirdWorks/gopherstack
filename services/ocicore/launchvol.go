package ocicore

import "slices"

const volumeCreationAttributes = "ATTRIBUTES"

// LaunchCreateVolumeIn is launchCreateVolumeDetails of a launch volume attachment.
type LaunchCreateVolumeIn struct {
	VpusPerGB          *int64 `json:"vpusPerGB"`
	SizeInGBs          *int64 `json:"sizeInGBs"`
	VolumeCreationType string `json:"volumeCreationType"`
	CompartmentID      string `json:"compartmentId"`
	DisplayName        string `json:"displayName"`
	KmsKeyID           string `json:"kmsKeyId"`
}

// LaunchVolumeIn is one launchVolumeAttachments entry of LaunchInstance.
type LaunchVolumeIn struct {
	CreateVolume                 *LaunchCreateVolumeIn `json:"launchCreateVolumeDetails"`
	IsReadOnly                   *bool                 `json:"isReadOnly"`
	IsShareable                  *bool                 `json:"isShareable"`
	UseChap                      *bool                 `json:"useChap"`
	IsAgentAutoIscsiLoginEnabled *bool                 `json:"isAgentAutoIscsiLoginEnabled"`
	IsPvEncryptionInTransit      *bool                 `json:"isPvEncryptionInTransitEnabled"`
	Type                         string                `json:"type"`
	Device                       string                `json:"device"`
	DisplayName                  string                `json:"displayName"`
	VolumeID                     string                `json:"volumeId"`
}

func (b *InMemoryBackend) checkLaunchVolume(in LaunchInstanceIn, v LaunchVolumeIn, devices *[]string) *APIError {
	if v.Type != attachISCSI && v.Type != attachPara {
		return errParam("launchVolumeAttachments type must be iscsi or paravirtualized")
	}

	if v.Device != "" {
		if !deviceRe.MatchString(v.Device) {
			return errParam("device must be /dev/oracleoci/oraclevd[b-z]")
		}

		if slices.Contains(*devices, v.Device) {
			return errConflict("The device " + v.Device + " is listed twice")
		}

		*devices = append(*devices, v.Device)
	}

	if aerr := validName(v.DisplayName); aerr != nil {
		return aerr
	}

	if (v.VolumeID == "") == (v.CreateVolume == nil) {
		return errParam("exactly one of volumeId and launchCreateVolumeDetails is required")
	}

	if v.CreateVolume != nil {
		return b.checkLaunchCreate(*v.CreateVolume)
	}

	vol, aerr := getLive[*Volume](b, v.VolumeID)
	switch {
	case aerr != nil:
		return aerr
	case vol.LifecycleState != statAvailable:
		return errState("The volume " + vol.ID + " is " + vol.LifecycleState)
	case vol.AvailabilityDomain != in.AvailabilityDomain:
		return errParam("The volume " + vol.ID + " is in a different availability domain than the instance")
	case len(liveOf(b, func(a *VolumeAttachment) bool { return a.VolumeID == vol.ID })) > 0 &&
		(!flag(v.IsShareable) || !flag(v.IsReadOnly)):
		return errConflict("The volume " + vol.ID + " is attached elsewhere; sharing needs isShareable and isReadOnly")
	}

	return nil
}

func (b *InMemoryBackend) checkLaunchCreate(c LaunchCreateVolumeIn) *APIError {
	if c.VolumeCreationType != volumeCreationAttributes {
		return errParam("launchCreateVolumeDetails.volumeCreationType must be ATTRIBUTES")
	}

	if c.SizeInGBs == nil {
		return errMissing("launchCreateVolumeDetails.sizeInGBs is required")
	}

	_, _, aerr := b.volumeParams(CreateVolumeIn{SizeInGBs: c.SizeInGBs, VpusPerGB: c.VpusPerGB}, 0)
	if aerr != nil {
		return aerr
	}

	return b.checkKmsKey(c.KmsKeyID)
}

func (b *InMemoryBackend) checkLaunchVolumes(in LaunchInstanceIn) *APIError {
	if len(in.LaunchVolumeAttachments) > maxVolAttached {
		return errLimit("An instance can have at most 32 volume attachments")
	}

	var devices []string

	for _, v := range in.LaunchVolumeAttachments {
		if aerr := b.checkLaunchVolume(in, v, &devices); aerr != nil {
			return aerr
		}
	}

	return nil
}

// attachLaunchVolumes creates and attaches the volumes of a validated launch request.
func (b *InMemoryBackend) attachLaunchVolumes(inst *Instance, items []LaunchVolumeIn) {
	for _, it := range items {
		vol, _ := b.res[it.VolumeID].(*Volume)

		if it.CreateVolume != nil {
			c := it.CreateVolume
			created, _, _ := b.CreateVolume(CreateVolumeIn{
				CompartmentID: cmpOrStr(
					c.CompartmentID,
					inst.CompartmentID,
				),
				AvailabilityDomain: inst.AvailabilityDomain,
				SizeInGBs:          c.SizeInGBs,
				VpusPerGB:          c.VpusPerGB,
				DisplayName:        c.DisplayName,
				KmsKeyID:           c.KmsKeyID,
			})

			vol, _ = b.res[created.base().ID].(*Volume)
			vol.Internal.LaunchInstanceID = inst.ID
			b.ready(vol)
		}

		if vol == nil {
			continue
		}

		b.newAttachment(inst, vol, AttachVolumeIn{
			Type:                         it.Type,
			Device:                       it.Device,
			DisplayName:                  it.DisplayName,
			IsReadOnly:                   it.IsReadOnly,
			IsShareable:                  it.IsShareable,
			UseChap:                      it.UseChap,
			IsAgentAutoIscsiLoginEnabled: it.IsAgentAutoIscsiLoginEnabled,
			IsPvEncryptionInTransit:      it.IsPvEncryptionInTransit,
		})
	}
}

// dropLaunchVolumes ends the data volumes a launch created for the instance.
func (b *InMemoryBackend) dropLaunchVolumes(instID string) {
	for _, v := range liveOf(b, func(x *Volume) bool { return x.Internal.LaunchInstanceID == instID }) {
		b.dropAssignments(v.ID)
		b.endReplicas(v)
		b.markTerminating(v)
	}
}
