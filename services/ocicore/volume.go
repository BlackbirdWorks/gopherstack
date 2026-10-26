package ocicore

import (
	"fmt"
	"regexp"
	"slices"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

const (
	attachISCSI     = "iscsi"
	attachPara      = "paravirtualized"
	maxVolAttached  = 32
	iscsiPort       = 3260
	encNone         = "NONE"
	chapUserLen     = 8
	chapSecretLen   = 20
	iscsiHostBase   = 2
	iscsiHostSpan   = 250
	srcVolume       = "volume"
	srcVolumeBackup = "volumeBackup"
	srcBootVolume   = "bootVolume"
)

var deviceRe = regexp.MustCompile(`^/dev/oracleoci/oraclevd[b-z]$`)

// CreateVolumeIn is the CreateVolume request body.
type CreateVolumeIn struct {
	TagsIn
	SizeInGBs           *int64        `json:"sizeInGBs"`
	SizeInMBs           *int64        `json:"sizeInMBs"`
	VpusPerGB           *int64        `json:"vpusPerGB"`
	IsAutoTuneEnabled   *bool         `json:"isAutoTuneEnabled"`
	SourceDetails       *VolumeSource `json:"sourceDetails"`
	AvailabilityDomain  string        `json:"availabilityDomain"`
	CompartmentID       string        `json:"compartmentId"`
	DisplayName         string        `json:"displayName"`
	KmsKeyID            string        `json:"kmsKeyId"`
	VolumeBackupID      string        `json:"volumeBackupId"`
	BackupPolicyID      string        `json:"backupPolicyId"`
	AutotunePolicies    []any         `json:"autotunePolicies"`
	BlockVolumeReplicas []any         `json:"blockVolumeReplicas"`
}

// volumeSize resolves the size from the request and the source (a volume or backup in GB).
func volumeSize(gbs, mbs *int64, srcGBs int64) (int64, *APIError) {
	size := int64(minVolumeGBs)

	switch {
	case gbs != nil:
		size = *gbs
	case mbs != nil:
		if *mbs%bytesGB != 0 {
			return 0, errParam("sizeInMBs must be a multiple of 1024")
		}

		size = *mbs / bytesGB
	case srcGBs > 0:
		size = srcGBs
	}

	if size < minVolumeGBs || size > maxVolumeGBs {
		return 0, errParam("The volume size must be between 50 and 32768 GB")
	}

	if srcGBs > 0 && size < srcGBs {
		return 0, errParam("The volume size cannot be smaller than its source")
	}

	return size, nil
}

// volumeSourceSize validates a volume source and returns its size in GB.
func (b *InMemoryBackend) volumeSourceSize(src *VolumeSource, ad string) (int64, string, *APIError) {
	if src == nil {
		return 0, "", nil
	}

	switch src.Type {
	case srcVolume:
		v, aerr := getLive[*Volume](b, src.ID)
		if aerr != nil {
			return 0, "", aerr
		}

		if v.LifecycleState != statAvailable {
			return 0, "", errState("The source volume " + v.ID + " is " + v.LifecycleState)
		}

		if v.AvailabilityDomain != ad {
			return 0, "", errParam("A volume can only be cloned within its availability domain")
		}

		return v.SizeInGBs, statProvisioning, nil
	case srcVolumeBackup:
		bk, aerr := getLive[*VolumeBackup](b, src.ID)
		if aerr != nil {
			return 0, "", aerr
		}

		if bk.LifecycleState != statAvailable {
			return 0, "", errState("The volume backup " + bk.ID + " is " + bk.LifecycleState)
		}

		return bk.SizeInGBs, statRestoring, nil
	}

	return 0, "", errParam("sourceDetails.type must be volume or volumeBackup")
}

func (b *InMemoryBackend) volumeParams(in CreateVolumeIn, srcGBs int64) (int64, int64, *APIError) {
	size, aerr := volumeSize(in.SizeInGBs, in.SizeInMBs, srcGBs)
	if aerr != nil {
		return 0, 0, aerr
	}

	vpus := int64(defaultVpus)

	if in.VpusPerGB != nil {
		if aerr = checkVpus(*in.VpusPerGB); aerr != nil {
			return 0, 0, aerr
		}

		vpus = *in.VpusPerGB
	}

	if in.BackupPolicyID != "" {
		_, aerr = b.findPolicy(in.BackupPolicyID)
	}

	return size, vpus, aerr
}

// CreateVolume creates a block volume, optionally from a volume or backup.
func (b *InMemoryBackend) CreateVolume(in CreateVolumeIn) (resource, string, *APIError) {
	b.mu.Lock("CreateVolume")
	defer b.mu.Unlock()

	if aerr := b.checkCompartment(in.CompartmentID); aerr != nil {
		return nil, "", aerr
	}

	if aerr := validName(in.DisplayName); aerr != nil {
		return nil, "", aerr
	}

	if in.AvailabilityDomain == "" {
		return nil, "", errMissing("availabilityDomain is required")
	}

	if !adRe.MatchString(in.AvailabilityDomain) {
		return nil, "", errParam("availabilityDomain " + in.AvailabilityDomain + " is not a valid availability domain")
	}

	if len(in.BlockVolumeReplicas) > 0 || len(in.AutotunePolicies) > 0 {
		return nil, "", errParam("blockVolumeReplicas and autotunePolicies are not supported")
	}

	src := in.SourceDetails
	if src == nil && in.VolumeBackupID != "" {
		src = &VolumeSource{Type: srcVolumeBackup, ID: in.VolumeBackupID}
	}

	srcGBs, state, aerr := b.volumeSourceSize(src, in.AvailabilityDomain)
	if aerr != nil {
		return nil, "", aerr
	}

	size, vpus, aerr := b.volumeParams(in, srcGBs)
	if aerr != nil {
		return nil, "", aerr
	}

	v := &Volume{
		Base:               b.newBase(kindVolume, in.CompartmentID, in.DisplayName, in.TagsIn),
		AvailabilityDomain: in.AvailabilityDomain, SizeInGBs: size, SizeInMBs: size * bytesGB, VpusPerGB: vpus,
		SourceDetails: src, KmsKeyID: in.KmsKeyID, IsHydrated: src == nil,
		IsAutoTuneEnabled: in.IsAutoTuneEnabled != nil && *in.IsAutoTuneEnabled,
	}

	if state == statRestoring {
		b.moveTo(v, statRestoring, statAvailable)
	}

	if in.DisplayName == "" {
		v.DisplayName = "volume" + b.nowUTC().Format("20060102-150405")
	}

	b.add(v)

	if in.BackupPolicyID != "" {
		b.assignPolicy(v.ID, in.BackupPolicyID, in.CompartmentID)
	}

	return v.view(), v.Internal.Etag, nil
}

// UpdateVolumeIn is the UpdateVolume request body.
type UpdateVolumeIn struct {
	SizeInGBs         *int64 `json:"sizeInGBs"`
	VpusPerGB         *int64 `json:"vpusPerGB"`
	IsAutoTuneEnabled *bool  `json:"isAutoTuneEnabled"`
	UpdateIn
}

// UpdateVolume changes a volume; the size can only grow.
func (b *InMemoryBackend) UpdateVolume(id, ifMatch string, in UpdateVolumeIn) (resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in.UpdateIn, func(v *Volume) *APIError {
		return applyVolumeChanges(&v.SizeInGBs, &v.SizeInMBs, &v.VpusPerGB, &v.IsAutoTuneEnabled, in)
	})
}

func applyVolumeChanges(gbs, mbs, vpus *int64, auto *bool, in UpdateVolumeIn) *APIError {
	if in.SizeInGBs != nil {
		if *in.SizeInGBs < *gbs || *in.SizeInGBs > maxVolumeGBs {
			return errParam("sizeInGBs can only be increased, up to 32768")
		}
	}

	if in.VpusPerGB != nil {
		if aerr := checkVpus(*in.VpusPerGB); aerr != nil {
			return aerr
		}

		*vpus = *in.VpusPerGB
	}

	if in.SizeInGBs != nil {
		*gbs, *mbs = *in.SizeInGBs, *in.SizeInGBs*bytesGB
	}

	if in.IsAutoTuneEnabled != nil {
		*auto = *in.IsAutoTuneEnabled
	}

	return nil
}

// CreateBootVolumeIn is the CreateBootVolume request body.
type CreateBootVolumeIn struct {
	TagsIn
	SizeInGBs          *int64        `json:"sizeInGBs"`
	VpusPerGB          *int64        `json:"vpusPerGB"`
	SourceDetails      *VolumeSource `json:"sourceDetails"`
	CompartmentID      string        `json:"compartmentId"`
	AvailabilityDomain string        `json:"availabilityDomain"`
	DisplayName        string        `json:"displayName"`
	KmsKeyID           string        `json:"kmsKeyId"`
}

// CreateBootVolume clones an existing boot volume.
func (b *InMemoryBackend) CreateBootVolume(in CreateBootVolumeIn) (resource, string, *APIError) {
	b.mu.Lock("CreateBootVolume")
	defer b.mu.Unlock()

	if aerr := b.checkCompartment(in.CompartmentID); aerr != nil {
		return nil, "", aerr
	}

	if aerr := validName(in.DisplayName); aerr != nil {
		return nil, "", aerr
	}

	if in.SourceDetails == nil || in.SourceDetails.Type != srcBootVolume {
		return nil, "", errMissing("sourceDetails with type bootVolume is required")
	}

	src, aerr := getLive[*BootVolume](b, in.SourceDetails.ID)
	if aerr != nil {
		return nil, "", aerr
	}

	if src.LifecycleState != statAvailable {
		return nil, "", errState("The source boot volume " + src.ID + " is " + src.LifecycleState)
	}

	if in.AvailabilityDomain != "" && in.AvailabilityDomain != src.AvailabilityDomain {
		return nil, "", errParam("A boot volume can only be cloned within its availability domain")
	}

	size, aerr := volumeSize(in.SizeInGBs, nil, src.SizeInGBs)
	if aerr != nil {
		return nil, "", aerr
	}

	vpus := src.VpusPerGB

	if in.VpusPerGB != nil {
		if aerr = checkVpus(*in.VpusPerGB); aerr != nil {
			return nil, "", aerr
		}

		vpus = *in.VpusPerGB
	}

	bv := &BootVolume{
		Base:               b.newBase(kindBootVolume, in.CompartmentID, in.DisplayName, in.TagsIn),
		AvailabilityDomain: src.AvailabilityDomain, ImageID: src.ImageID, SizeInGBs: size, SizeInMBs: size * bytesGB,
		VpusPerGB: vpus, SourceDetails: in.SourceDetails, KmsKeyID: in.KmsKeyID,
	}
	b.add(bv)

	return bv.view(), bv.Internal.Etag, nil
}

// UpdateBootVolume changes a boot volume.
func (b *InMemoryBackend) UpdateBootVolume(id, ifMatch string, in UpdateVolumeIn) (resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in.UpdateIn, func(v *BootVolume) *APIError {
		return applyVolumeChanges(&v.SizeInGBs, &v.SizeInMBs, &v.VpusPerGB, &v.IsAutoTuneEnabled, in)
	})
}

func (b *InMemoryBackend) endBootVolume(bv *BootVolume) {
	b.dropAssignments(bv.ID)
	b.markTerminating(bv)
}

// AttachVolumeIn is the AttachVolume request body (type iscsi or paravirtualized).
type AttachVolumeIn struct {
	IsReadOnly                   *bool  `json:"isReadOnly"`
	IsShareable                  *bool  `json:"isShareable"`
	UseChap                      *bool  `json:"useChap"`
	IsPvEncryptionInTransit      *bool  `json:"isPvEncryptionInTransitEnabled"`
	IsAgentAutoIscsiLoginEnabled *bool  `json:"isAgentAutoIscsiLoginEnabled"`
	Type                         string `json:"type"`
	InstanceID                   string `json:"instanceId"`
	VolumeID                     string `json:"volumeId"`
	Device                       string `json:"device"`
	DisplayName                  string `json:"displayName"`
}

func flag(p *bool) bool { return p != nil && *p }

func (b *InMemoryBackend) attachChecks(in AttachVolumeIn) (*Instance, *Volume, *APIError) {
	if in.InstanceID == "" || in.VolumeID == "" {
		return nil, nil, errMissing("instanceId and volumeId are required")
	}

	if in.Type != attachISCSI && in.Type != attachPara {
		return nil, nil, errParam("type must be iscsi or paravirtualized")
	}

	if in.Device != "" && !deviceRe.MatchString(in.Device) {
		return nil, nil, errParam("device must be /dev/oracleoci/oraclevd[b-z]")
	}

	inst, aerr := getLive[*Instance](b, in.InstanceID)
	if aerr != nil {
		return nil, nil, aerr
	}

	vol, aerr := getLive[*Volume](b, in.VolumeID)
	if aerr != nil {
		return nil, nil, aerr
	}

	if inst.LifecycleState != statRunning && inst.LifecycleState != statStopped {
		return nil, nil, errState("The instance " + inst.ID + " is " + inst.LifecycleState)
	}

	if vol.LifecycleState != statAvailable {
		return nil, nil, errState("The volume " + vol.ID + " is " + vol.LifecycleState)
	}

	if vol.AvailabilityDomain != inst.AvailabilityDomain {
		return nil, nil, errParam("The volume and the instance are in different availability domains")
	}

	return inst, vol, b.attachConflicts(in, inst, vol)
}

func (b *InMemoryBackend) attachConflicts(in AttachVolumeIn, inst *Instance, vol *Volume) *APIError {
	onInst := liveOf(b, func(a *VolumeAttachment) bool { return a.InstanceID == inst.ID })
	onVol := liveOf(b, func(a *VolumeAttachment) bool { return a.VolumeID == vol.ID })

	if slices.ContainsFunc(onVol, func(a *VolumeAttachment) bool { return a.InstanceID == inst.ID }) {
		return errConflict("The volume " + vol.ID + " is already attached to instance " + inst.ID)
	}

	if len(onVol) > 0 && (!flag(in.IsShareable) || !flag(in.IsReadOnly)) {
		return errConflict("The volume " + vol.ID + " is attached elsewhere; sharing needs isShareable and isReadOnly")
	}

	if len(onInst) >= maxVolAttached {
		return errLimit("An instance can have at most 32 volume attachments")
	}

	if in.Device != "" && slices.ContainsFunc(onInst, func(a *VolumeAttachment) bool { return a.Device == in.Device }) {
		return errConflict("The device " + in.Device + " is already in use on instance " + inst.ID)
	}

	return nil
}

// AttachVolume attaches a block volume to an instance.
func (b *InMemoryBackend) AttachVolume(in AttachVolumeIn) (resource, string, *APIError) {
	b.mu.Lock("AttachVolume")
	defer b.mu.Unlock()

	if aerr := validName(in.DisplayName); aerr != nil {
		return nil, "", aerr
	}

	inst, vol, aerr := b.attachChecks(in)
	if aerr != nil {
		return nil, "", aerr
	}

	a := &VolumeAttachment{
		Base:           b.newBase(kindVolumeAttach, inst.CompartmentID, in.DisplayName, TagsIn{}),
		AttachmentType: in.Type, AvailabilityDomain: inst.AvailabilityDomain, InstanceID: inst.ID, VolumeID: vol.ID,
		Device: in.Device, IsReadOnly: flag(in.IsReadOnly), IsShareable: flag(in.IsShareable),
		IsPvEncryptionInTransit: flag(in.IsPvEncryptionInTransit), EncryptionInTransitType: encNone,
	}

	if in.Type == attachISCSI {
		a.IscsiLoginState = "UNKNOWN"
		a.Ipv4 = fmt.Sprintf("169.254.2.%d", iscsiHostBase+a.Internal.Seq%iscsiHostSpan)
		a.Iqn = "iqn.2015-12.com.oracleiaas:" + uuid.NewString()
		a.Port = iscsiPort
		a.IsAgentAutoIscsiLoginEnabled = flag(in.IsAgentAutoIscsiLoginEnabled)

		if flag(in.UseChap) {
			a.ChapUsername = "ocid1.volumeattachment." + awsid.NewN("", chapUserLen)
			a.ChapSecret = awsid.NewN("", chapSecretLen)
		}
	}

	b.moveTo(a, statAttaching, statAttached)
	b.add(a)

	return a.view(), a.Internal.Etag, nil
}

// DetachVolume detaches a volume from an instance.
func (b *InMemoryBackend) DetachVolume(id, ifMatch string) *APIError {
	b.mu.Lock("DetachVolume")
	defer b.mu.Unlock()

	a, aerr := get[*VolumeAttachment](b, id)
	if aerr != nil {
		return aerr
	}

	if !live(a) {
		return nil
	}

	if aerr = checkEtag(a, ifMatch); aerr != nil {
		return aerr
	}

	b.moveTo(a, statDetaching, statDetached)

	return nil
}
