package ocicore

import "net/http"

// AttachBootVolumeIn is the AttachBootVolume request body.
type AttachBootVolumeIn struct {
	BootVolumeID string `json:"bootVolumeId"`
	InstanceID   string `json:"instanceId"`
	DisplayName  string `json:"displayName"`
}

// AttachBootVolume attaches a standalone boot volume to an instance of the same availability domain.
func (b *InMemoryBackend) AttachBootVolume(in AttachBootVolumeIn) (Resource, string, *APIError) {
	if aerr := validName(in.DisplayName); aerr != nil {
		return nil, "", aerr
	}

	if in.BootVolumeID == "" || in.InstanceID == "" {
		return nil, "", errMissing("bootVolumeId and instanceId are required")
	}

	inst, aerr := getLive[*Instance](b, in.InstanceID)
	if aerr != nil {
		return nil, "", aerr
	}

	bv, aerr := getLive[*BootVolume](b, in.BootVolumeID)
	if aerr != nil {
		return nil, "", aerr
	}

	switch {
	case inst.LifecycleState != statRunning && inst.LifecycleState != statStopped:
		return nil, "", errState("The instance " + inst.ID + " is " + inst.LifecycleState)
	case bv.LifecycleState != statAvailable:
		return nil, "", errState("The boot volume " + bv.ID + " is " + bv.LifecycleState)
	case bv.AvailabilityDomain != inst.AvailabilityDomain:
		return nil, "", errParam("The boot volume and the instance are in different availability domains")
	}

	if on := firstID(liveOf(b, func(a *BootVolumeAttachment) bool { return a.BootVolumeID == bv.ID })); on != "" {
		return nil, "", errConflict("The boot volume " + bv.ID + " is already attached (" + on + ")")
	}

	if len(liveOf(b, func(a *BootVolumeAttachment) bool { return a.InstanceID == inst.ID })) >= maxVolAttached {
		return nil, "", errLimit("An instance can have at most 32 volume attachments")
	}

	a := &BootVolumeAttachment{
		Base:               b.newBase(kindBootAttach, inst.CompartmentID, in.DisplayName, TagsIn{}),
		AvailabilityDomain: inst.AvailabilityDomain, BootVolumeID: bv.ID, InstanceID: inst.ID,
	}
	b.moveTo(a, statAttaching, statAttached)
	b.add(a)

	return a.view(), a.Internal.Etag, nil
}

// DetachBootVolume detaches a boot volume from a stopped instance.
func (b *InMemoryBackend) DetachBootVolume(id, ifMatch string) *APIError {
	b.mu.Lock("DetachBootVolume")
	defer b.mu.Unlock()

	a, aerr := get[*BootVolumeAttachment](b, id)
	if aerr != nil {
		return aerr
	}

	if !live(a) {
		return nil
	}

	if aerr = checkEtag(a, ifMatch); aerr != nil {
		return aerr
	}

	if inst, ok := b.res[a.InstanceID].(*Instance); ok && live(inst) && inst.LifecycleState != statStopped {
		return errState("A boot volume can only be detached from a STOPPED instance; instance " + inst.ID + " is " +
			inst.LifecycleState)
	}

	b.moveTo(a, statDetaching, statDetached)

	return nil
}

func (b *InMemoryBackend) hasBootVolume(instID string) bool {
	return len(liveOf(b, func(a *BootVolumeAttachment) bool { return a.InstanceID == instID })) > 0
}

// UpdateSourceIn is UpdateInstanceDetails.sourceDetails: replace the boot volume with a boot volume or an image.
type UpdateSourceIn struct {
	IsPreserveBootVolumeEnabled *bool  `json:"isPreserveBootVolumeEnabled"`
	BootVolumeSizeInGBs         *int64 `json:"bootVolumeSizeInGBs"`
	SourceType                  string `json:"sourceType"`
	BootVolumeID                string `json:"bootVolumeId"`
	ImageID                     string `json:"imageId"`
	KmsKeyID                    string `json:"kmsKeyId"`
}

func (b *InMemoryBackend) replacementSource(inst *Instance, in *UpdateSourceIn) (instanceSource, *APIError) {
	if aerr := b.checkKmsKey(in.KmsKeyID); aerr != nil {
		return instanceSource{}, aerr
	}

	sh, found := shapeByName(inst.Shape)
	if !found {
		return instanceSource{}, errParam("Shape " + inst.Shape + " is not a valid shape")
	}

	var (
		src  instanceSource
		aerr *APIError
	)

	switch in.SourceType {
	case srcImage:
		src, aerr = b.imageSource(SourceDetails{
			SourceType:          srcImage,
			ImageID:             in.ImageID,
			BootVolumeSizeInGBs: in.BootVolumeSizeInGBs,
			KmsKeyID:            in.KmsKeyID,
		}, sh)
	case srcBootVolume:
		src, aerr = b.bootVolumeSource(
			SourceDetails{SourceType: srcBootVolume, BootVolumeID: in.BootVolumeID}, inst.AvailabilityDomain,
		)
	default:
		aerr = errParam("sourceDetails.sourceType must be image or bootVolume")
	}

	if aerr == nil && src.arch != "" && src.arch != sh.arch {
		aerr = errParam("The boot source is not compatible with shape " + inst.Shape)
	}

	return src, aerr
}

// replaceBootVolume swaps the boot volume of an instance for another boot volume or a fresh one from an image.
func (b *InMemoryBackend) replaceBootVolume(inst *Instance, in *UpdateSourceIn) *APIError {
	if inst.LifecycleState != statRunning && inst.LifecycleState != statStopped {
		return errState(
			"The boot volume of instance " + inst.ID + " can only be replaced while it is RUNNING or STOPPED",
		)
	}

	src, aerr := b.replacementSource(inst, in)
	if aerr != nil {
		return aerr
	}

	preserve := in.IsPreserveBootVolumeEnabled != nil && *in.IsPreserveBootVolumeEnabled

	for _, a := range liveOf(b, func(x *BootVolumeAttachment) bool { return x.InstanceID == inst.ID }) {
		b.moveTo(a, statDetaching, statDetached)

		if bv, isBoot := b.res[a.BootVolumeID].(*BootVolume); isBoot && !preserve && live(bv) {
			b.endBootVolume(bv)
		}
	}

	b.attachBootVolume(inst, src)
	inst.ImageID, inst.SourceDetails = src.imageID, &src.details

	return nil
}

func bootAttachEntries() []entry {
	return []entry{
		{createOp((*InMemoryBackend).AttachBootVolume), http.MethodPost, "/bootVolumeAttachments", "AttachBootVolume"},
		{
			func(h *Handler, q *req) error {
				return noBody(q, h.Backend.DetachBootVolume(q.param("bootVolumeAttachmentId"), q.ifMatch()))
			},
			http.MethodDelete, idPath("/bootVolumeAttachments", "bootVolumeAttachmentId"), "DetachBootVolume",
		},
	}
}
