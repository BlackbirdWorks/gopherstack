package ocicore

import (
	"encoding/base64"
	"encoding/json/v2"
	"maps"
	"regexp"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	maxMetadataBytes = 32000
	faultDomains     = 3
	minVolumeGBs     = 50
	maxVolumeGBs     = 32768
	defaultVpus      = 10
	srcImage         = "image"
)

var faultDomainRe = regexp.MustCompile(`^FAULT-DOMAIN-[1-3]$`)

// LaunchInstanceIn is the LaunchInstance request body.
type LaunchInstanceIn struct {
	TagsIn
	AgentConfig        map[string]any    `json:"agentConfig"`
	ShapeConfig        *ShapeConfigIn    `json:"shapeConfig"`
	SourceDetails      *SourceDetails    `json:"sourceDetails"`
	Metadata           map[string]string `json:"metadata"`
	ExtendedMetadata   map[string]any    `json:"extendedMetadata"`
	PlatformConfig     *PlatformConfig   `json:"platformConfig"`
	LaunchOptions      map[string]any    `json:"launchOptions"`
	AvailabilityConfig map[string]any    `json:"availabilityConfig"`
	InstanceOptions    map[string]any    `json:"instanceOptions"`
	CreateVnicDetails  *CreateVnicIn     `json:"createVnicDetails"`
	Preemptible        *PreemptibleIn    `json:"preemptibleInstanceConfig"`
	PlacementIn
	AvailabilityDomain      string           `json:"availabilityDomain"`
	HostnameLabel           string           `json:"hostnameLabel"`
	ImageID                 string           `json:"imageId"`
	Shape                   string           `json:"shape"`
	SubnetID                string           `json:"subnetId"`
	FaultDomain             string           `json:"faultDomain"`
	DisplayName             string           `json:"displayName"`
	CompartmentID           string           `json:"compartmentId"`
	LaunchVolumeAttachments []LaunchVolumeIn `json:"launchVolumeAttachments"`
}

func checkMetadata(md map[string]string, ext map[string]any) *APIError {
	size := 0

	for k, v := range md {
		size += len(k) + len(v)
	}

	for k, v := range ext {
		raw, _ := json.Marshal(v, jsonx.V1())
		size += len(k) + len(raw)
	}

	if size > maxMetadataBytes {
		return errParam("metadata and extendedMetadata are limited to 32,000 bytes in total")
	}

	if ud, ok := md["user_data"]; ok {
		if _, err := base64.StdEncoding.DecodeString(ud); err != nil {
			return errParam("metadata.user_data must be base64 encoded")
		}
	}

	return nil
}

// instanceSource resolves the boot source of a launch: the image (or boot volume) and its architecture.
type instanceSource struct {
	bootVolume *BootVolume
	details    SourceDetails
	imageID    string
	arch       string
	sizeGBs    int64
	vpus       int64
}

func (b *InMemoryBackend) launchSource(in LaunchInstanceIn, sh shapeDef) (instanceSource, *APIError) {
	var src instanceSource

	sd := in.SourceDetails
	if sd == nil {
		if in.ImageID == "" {
			return src, errMissing("sourceDetails is required")
		}

		sd = &SourceDetails{SourceType: srcImage, ImageID: in.ImageID}
	}

	if aerr := b.checkKmsKey(sd.KmsKeyID); aerr != nil {
		return src, aerr
	}

	switch sd.SourceType {
	case srcImage:
		return b.imageSource(*sd, sh)
	case srcBootVolume:
		return b.bootVolumeSource(*sd, in.AvailabilityDomain)
	}

	return src, errParam("sourceDetails.sourceType must be image or bootVolume")
}

func (b *InMemoryBackend) imageSource(sd SourceDetails, sh shapeDef) (instanceSource, *APIError) {
	if sd.ImageID == "" {
		return instanceSource{}, errMissing("sourceDetails.imageId is required")
	}

	img, aerr := b.findImage(sd.ImageID)
	if aerr != nil {
		return instanceSource{}, aerr
	}

	if img.LifecycleState != statAvailable {
		return instanceSource{}, errState("The image " + img.ID + " is " + img.LifecycleState)
	}

	if img.Internal.Arch != sh.arch {
		return instanceSource{}, errParam("The image " + img.ID + " is not compatible with shape " + sh.name)
	}

	size, vpus := bootVolumeGBs(img.SizeInMBs), int64(defaultVpus)

	if sd.BootVolumeSizeInGBs != nil {
		if *sd.BootVolumeSizeInGBs < size || *sd.BootVolumeSizeInGBs > maxVolumeGBs {
			return instanceSource{}, errParam("bootVolumeSizeInGBs must be between the image size and 32768")
		}

		size = *sd.BootVolumeSizeInGBs
	}

	if sd.BootVolumeVpusPerGB != nil {
		if aerr = checkVpus(*sd.BootVolumeVpusPerGB); aerr != nil {
			return instanceSource{}, aerr
		}

		vpus = *sd.BootVolumeVpusPerGB
	}

	d := SourceDetails{SourceType: srcImage, ImageID: img.ID, BootVolumeSizeInGBs: &size, KmsKeyID: sd.KmsKeyID}

	return instanceSource{imageID: img.ID, arch: img.Internal.Arch, sizeGBs: size, vpus: vpus, details: d}, nil
}

func (b *InMemoryBackend) bootVolumeSource(sd SourceDetails, ad string) (instanceSource, *APIError) {
	if sd.BootVolumeID == "" {
		return instanceSource{}, errMissing("sourceDetails.bootVolumeId is required")
	}

	bv, aerr := getLive[*BootVolume](b, sd.BootVolumeID)
	if aerr != nil {
		return instanceSource{}, aerr
	}

	if bv.LifecycleState != statAvailable {
		return instanceSource{}, errState("The boot volume " + bv.ID + " is " + bv.LifecycleState)
	}

	if bv.AvailabilityDomain != ad {
		return instanceSource{}, errParam("The boot volume " + bv.ID + " is in a different availability domain")
	}

	if len(liveOf(b, func(a *BootVolumeAttachment) bool { return a.BootVolumeID == bv.ID })) > 0 {
		return instanceSource{}, errConflict("The boot volume " + bv.ID + " is already attached to an instance")
	}

	src := instanceSource{
		bootVolume: bv,
		imageID:    bv.ImageID,
		details:    SourceDetails{SourceType: srcBootVolume, BootVolumeID: bv.ID},
	}

	if img, err := b.findImage(bv.ImageID); err == nil {
		src.arch = img.Internal.Arch
	}

	return src, nil
}

func checkVpus(v int64) *APIError {
	if v < 0 || v > 120 || v%10 != 0 {
		return errParam("vpusPerGB must be a multiple of 10 between 0 and 120")
	}

	return nil
}

func (b *InMemoryBackend) launchChecks(in LaunchInstanceIn) (shapeDef, *InstanceShapeConfig, *APIError) {
	if aerr := b.checkCompartment(in.CompartmentID); aerr != nil {
		return shapeDef{}, nil, aerr
	}

	if aerr := validName(in.DisplayName); aerr != nil {
		return shapeDef{}, nil, aerr
	}

	if in.AvailabilityDomain == "" {
		return shapeDef{}, nil, errMissing("availabilityDomain is required")
	}

	if !adRe.MatchString(in.AvailabilityDomain) {
		return shapeDef{}, nil, errParam(
			"availabilityDomain " + in.AvailabilityDomain + " is not a valid availability domain",
		)
	}

	if in.FaultDomain != "" && !faultDomainRe.MatchString(in.FaultDomain) {
		return shapeDef{}, nil, errParam("faultDomain must be FAULT-DOMAIN-1, FAULT-DOMAIN-2 or FAULT-DOMAIN-3")
	}

	if aerr := b.checkLaunchVolumes(in); aerr != nil {
		return shapeDef{}, nil, aerr
	}

	if in.Shape == "" {
		return shapeDef{}, nil, errMissing("shape is required")
	}

	sh, ok := shapeByName(in.Shape)
	if !ok {
		return shapeDef{}, nil, errParam("Shape " + in.Shape + " is not a valid shape")
	}

	if aerr := checkPlatform(sh, in.PlatformConfig); aerr != nil {
		return shapeDef{}, nil, aerr
	}

	if aerr := checkPreemptible(in.Preemptible); aerr != nil {
		return shapeDef{}, nil, aerr
	}

	cfg, aerr := sh.resolve(in.ShapeConfig)
	if aerr != nil {
		return shapeDef{}, nil, aerr
	}

	return sh, cfg, checkMetadata(in.Metadata, in.ExtendedMetadata)
}

func agentConfig(in map[string]any) map[string]any {
	out := map[string]any{"isMonitoringDisabled": false, "isManagementDisabled": false, "areAllPluginsDisabled": false}
	maps.Copy(out, in)

	return out
}

// LaunchInstance creates an instance with its boot volume and primary VNIC.
func (b *InMemoryBackend) LaunchInstance(in LaunchInstanceIn) (Resource, string, *APIError) {
	sh, cfg, aerr := b.launchChecks(in)
	if aerr != nil {
		return nil, "", aerr
	}

	src, aerr := b.launchSource(in, sh)
	if aerr != nil {
		return nil, "", aerr
	}

	vd := CreateVnicIn{}
	if in.CreateVnicDetails != nil {
		vd = *in.CreateVnicDetails
	}

	vd.SubnetID = cmpOrStr(vd.SubnetID, in.SubnetID)
	vd.HostnameLabel = cmpOrStr(vd.HostnameLabel, in.HostnameLabel)

	inst := &Instance{
		Base:                      b.newBase(kindInstance, in.CompartmentID, in.DisplayName, in.TagsIn),
		AvailabilityDomain:        in.AvailabilityDomain,
		Region:                    regionName,
		Shape:                     sh.name,
		ShapeConfig:               cfg,
		LaunchMode:                "PARAVIRTUALIZED",
		ImageID:                   src.imageID,
		SourceDetails:             &src.details,
		Metadata:                  nonNilStrings(in.Metadata),
		ExtendedMetadata:          nonNilAny(in.ExtendedMetadata),
		AgentConfig:               agentConfig(in.AgentConfig),
		LaunchOptions:             in.LaunchOptions,
		AvailabilityConfig:        in.AvailabilityConfig,
		InstanceOptions:           in.InstanceOptions,
		PlatformConfig:            in.PlatformConfig,
		PreemptibleInstanceConfig: in.Preemptible,
		FaultDomain:               in.FaultDomain,
	}

	if inst.FaultDomain == "" {
		inst.FaultDomain = "FAULT-DOMAIN-" + string(rune('1'+inst.Internal.Seq%faultDomains))
	}

	if in.DisplayName == "" {
		inst.DisplayName = "instance" + b.nowUTC().Format("20060102-1504")
	}

	if aerr = b.placeInstance(in, sh, cfg, inst); aerr != nil {
		return nil, "", aerr
	}

	vnic, aerr := b.newVnic(vd, in.CompartmentID, in.AvailabilityDomain, true)
	if aerr != nil {
		return nil, "", aerr
	}

	b.stampCredentials(inst)
	b.moveTo(inst, statProvisioning, statRunning)
	b.add(inst)
	b.syncCompute()
	b.newVnicAttachment(inst, vnic, "", 0)
	b.attachBootVolume(inst, src)
	b.attachLaunchVolumes(inst, in.LaunchVolumeAttachments)

	return inst.view(), inst.Internal.Etag, nil
}

func (b *InMemoryBackend) attachBootVolume(inst *Instance, src instanceSource) {
	bv := src.bootVolume

	if bv == nil {
		bv = &BootVolume{
			Base: b.newBase(
				kindBootVolume,
				inst.CompartmentID,
				inst.DisplayName+" (Boot Volume)",
				TagsIn{},
			),
			AvailabilityDomain: inst.AvailabilityDomain,
			ImageID:            src.imageID,
			SizeInGBs:          src.sizeGBs,
			SizeInMBs:          src.sizeGBs * bytesGB,
			VpusPerGB:          src.vpus,
			IsHydrated:         true,
			KmsKeyID:           src.details.KmsKeyID,
		}
		b.add(bv)
	}

	a := &BootVolumeAttachment{
		Base:               b.newBase(kindBootAttach, inst.CompartmentID, "", TagsIn{}),
		AvailabilityDomain: inst.AvailabilityDomain, BootVolumeID: bv.ID, InstanceID: inst.ID,
	}
	b.moveTo(a, statAttaching, statAttached)
	b.add(a)
}

func cmpOrStr(a, def string) string {
	if a != "" {
		return a
	}

	return def
}

func nonNilStrings(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}

	return maps.Clone(m)
}

func nonNilAny(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}

	return maps.Clone(m)
}

// InstanceAction applies a power action.
func (b *InMemoryBackend) InstanceAction(id, action, ifMatch string) (Resource, string, *APIError) {
	b.mu.Lock("InstanceAction")
	defer b.mu.Unlock()

	return b.instanceActionLocked(id, action, ifMatch)
}

func (b *InMemoryBackend) instanceActionLocked(id, action, ifMatch string) (Resource, string, *APIError) {
	inst, aerr := getLive[*Instance](b, id)
	if aerr != nil {
		return nil, "", aerr
	}

	if aerr = checkEtag(inst, ifMatch); aerr != nil {
		return nil, "", aerr
	}

	var need, interim, final string

	switch strings.ToUpper(action) {
	case "STOP", "SOFTSTOP":
		need, interim, final = statRunning, statStopping, statStopped
	case "START":
		need, interim, final = statStopped, statStarting, statRunning
	case "RESET", "SOFTRESET":
		need = statRunning
	case "":
		return nil, "", errMissing("action is required")
	default:
		return nil, "", errParam(
			"action " + action + " is not supported; use STOP, START, SOFTRESET, RESET or SOFTSTOP",
		)
	}

	if inst.LifecycleState != need {
		return nil, "", errState("The action " + strings.ToUpper(action) + " requires the instance to be " + need +
			" but it is " + inst.LifecycleState)
	}

	if strings.EqualFold(action, "START") && !b.hasBootVolume(inst.ID) {
		return nil, "", errState("The instance " + inst.ID + " has no boot volume attached; attach one first")
	}

	if interim == "" {
		b.bump(inst)
	} else {
		b.moveTo(inst, interim, final)
	}

	return inst.view(), inst.Internal.Etag, nil
}

// UpdateInstanceIn is the UpdateInstance request body.
type UpdateInstanceIn struct {
	Shape            *string           `json:"shape"`
	ShapeConfig      *ShapeConfigIn    `json:"shapeConfig"`
	Metadata         map[string]string `json:"metadata"`
	ExtendedMetadata map[string]any    `json:"extendedMetadata"`
	AgentConfig      map[string]any    `json:"agentConfig"`
	SourceDetails    *UpdateSourceIn   `json:"sourceDetails"`
	UpdateIn
}

// UpdateInstance changes an instance; a different shape or shape configuration needs a stopped instance.
func (b *InMemoryBackend) UpdateInstance(id, ifMatch string, in UpdateInstanceIn) (Resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in.UpdateIn, func(inst *Instance) *APIError {
		md, ext := inst.Metadata, inst.ExtendedMetadata

		if in.Metadata != nil {
			if aerr := checkImmutableMetadata(inst.Metadata, in.Metadata); aerr != nil {
				return aerr
			}

			md = maps.Clone(in.Metadata)
		}

		if in.ExtendedMetadata != nil {
			ext = maps.Clone(in.ExtendedMetadata)
		}

		if aerr := checkMetadata(md, ext); aerr != nil {
			return aerr
		}

		cfg, name, aerr := b.resize(inst, in)
		if aerr != nil {
			return aerr
		}

		inst.Metadata, inst.ExtendedMetadata = md, ext
		inst.Shape, inst.ShapeConfig = name, cfg

		if in.SourceDetails != nil {
			if aerr = b.replaceBootVolume(inst, in.SourceDetails); aerr != nil {
				return aerr
			}
		}

		if in.AgentConfig != nil {
			inst.AgentConfig = mergeAny(inst.AgentConfig, in.AgentConfig)
		}

		return nil
	})
}

func (b *InMemoryBackend) resize(inst *Instance, in UpdateInstanceIn) (*InstanceShapeConfig, string, *APIError) {
	if in.Shape == nil && in.ShapeConfig == nil {
		return inst.ShapeConfig, inst.Shape, nil
	}

	name := inst.Shape
	if in.Shape != nil {
		name = *in.Shape
	}

	sh, ok := shapeByName(name)
	if !ok {
		return nil, "", errParam("Shape " + name + " is not a valid shape")
	}

	if name == inst.Shape && !sh.flexible && in.ShapeConfig == nil {
		return inst.ShapeConfig, name, nil
	}

	if inst.LifecycleState != statStopped {
		return nil, "", errState("The shape of instance " + inst.ID + " can only be changed while it is STOPPED")
	}

	if img, err := b.findImage(inst.ImageID); err == nil && img.Internal.Arch != sh.arch {
		return nil, "", errParam("Shape " + name + " is not compatible with the image of the instance")
	}

	if inst.PlatformConfig != nil && platformTypeFor(sh) != inst.PlatformConfig.Type {
		return nil, "", errParam("Shape " + name + " is not compatible with the platformConfig of the instance")
	}

	cfg, aerr := sh.resolve(in.ShapeConfig)

	return cfg, name, aerr
}

// checkImmutableMetadata rejects a metadata update that changes, removes or adds user_data or ssh_authorized_keys
// (UpdateInstanceDetails.Metadata in the OCI core SDK).
func checkImmutableMetadata(current, next map[string]string) *APIError {
	for _, k := range []string{"user_data", "ssh_authorized_keys"} {
		cv, had := current[k]
		nv, has := next[k]

		if had != has || cv != nv {
			return errParam("metadata." + k + " cannot be changed after launch; provide the existing value")
		}
	}

	return nil
}

func mergeAny(base, add map[string]any) map[string]any {
	out := maps.Clone(base)
	if out == nil {
		out = map[string]any{}
	}

	maps.Copy(out, add)

	return out
}

// TerminateInstance starts termination of an instance and releases everything attached to it.
func (b *InMemoryBackend) TerminateInstance(id, ifMatch string, preserveBoot bool) *APIError {
	return b.TerminateInstanceWith(id, ifMatch, TerminateOpts{PreserveBoot: preserveBoot, PreserveLaunchData: true})
}

// TerminateOpts are the query options of TerminateInstance.
type TerminateOpts struct {
	PreserveBoot       bool
	PreserveLaunchData bool
}

// TerminateInstanceWith terminates an instance; data volumes created at launch survive unless told otherwise.
func (b *InMemoryBackend) TerminateInstanceWith(id, ifMatch string, o TerminateOpts) *APIError {
	b.mu.Lock("TerminateInstance")
	defer b.mu.Unlock()

	return b.terminateLocked(id, ifMatch, o)
}

func (b *InMemoryBackend) terminateLocked(id, ifMatch string, o TerminateOpts) *APIError {
	preserveBoot := o.PreserveBoot

	inst, aerr := get[*Instance](b, id)
	if aerr != nil {
		return aerr
	}

	switch inst.LifecycleState {
	case statTerminated:
		return errNotFound(id)
	case statTerminating:
		return nil
	}

	if aerr = checkEtag(inst, ifMatch); aerr != nil {
		return aerr
	}

	for _, a := range liveOf(b, func(x *VnicAttachment) bool { return x.InstanceID == id }) {
		b.detachVnicAttachment(a)
	}

	for _, a := range liveOf(b, func(x *VolumeAttachment) bool { return x.InstanceID == id }) {
		b.moveTo(a, statDetaching, statDetached)
	}

	for _, a := range liveOf(b, func(x *BootVolumeAttachment) bool { return x.InstanceID == id }) {
		b.moveTo(a, statDetaching, statDetached)

		if bv, ok := b.res[a.BootVolumeID].(*BootVolume); ok && !preserveBoot && live(bv) {
			b.endBootVolume(bv)
		}
	}

	for _, c := range liveOf(b, func(x *InstanceConsoleConnection) bool { return x.InstanceID == id }) {
		b.moveTo(c, statDeleting, statDeleted)
	}

	if !o.PreserveLaunchData {
		b.dropLaunchVolumes(id)
	}

	b.markTerminating(inst)
	b.syncCompute()

	return nil
}

// CreateImageIn is the CreateImage request body.
type CreateImageIn struct {
	TagsIn
	CompartmentID string `json:"compartmentId"`
	DisplayName   string `json:"displayName"`
	InstanceID    string `json:"instanceId"`
	LaunchMode    string `json:"launchMode"`
}

// CreateImage captures a custom image from an instance's boot volume.
func (b *InMemoryBackend) CreateImage(in CreateImageIn) (Resource, string, *APIError) {
	if aerr := b.checkCompartment(in.CompartmentID); aerr != nil {
		return nil, "", aerr
	}

	if aerr := validName(in.DisplayName); aerr != nil {
		return nil, "", aerr
	}

	if in.InstanceID == "" {
		return nil, "", errMissing("instanceId is required")
	}

	inst, aerr := getLive[*Instance](b, in.InstanceID)
	if aerr != nil {
		return nil, "", aerr
	}

	if inst.LifecycleState != statRunning && inst.LifecycleState != statStopped {
		return nil, "", errState("The instance " + inst.ID + " is " + inst.LifecycleState)
	}

	base, err := b.findImage(inst.ImageID)
	if err != nil {
		return nil, "", errParam("The instance has no source image to derive an image from")
	}

	size := base.SizeInMBs

	for _, a := range liveOf(b, func(x *BootVolumeAttachment) bool { return x.InstanceID == inst.ID }) {
		if bv, ok := b.res[a.BootVolumeID].(*BootVolume); ok {
			size = bv.SizeInMBs
		}
	}

	img := &Image{
		Base:            b.newBase(kindImage, in.CompartmentID, in.DisplayName, in.TagsIn),
		OperatingSystem: base.OperatingSystem, OperatingSystemVersion: base.OperatingSystemVersion,
		BaseImageID: base.ID, LaunchMode: cmpOrStr(in.LaunchMode, base.LaunchMode), ListingType: listingNone,
		SizeInMBs: size, BillableSizeInGBs: (size + bytesGB - 1) / bytesGB, CreateImageAllowed: true,
	}
	img.Internal.Arch = base.Internal.Arch
	b.add(img)

	return img.view(), img.Internal.Etag, nil
}

// UpdateImage changes a custom image.
func (b *InMemoryBackend) UpdateImage(id, ifMatch string, in UpdateIn) (Resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in, func(*Image) *APIError { return nil })
}

// DeleteImage deletes a custom image; platform images cannot be deleted.
func (b *InMemoryBackend) DeleteImage(id, ifMatch string) *APIError {
	b.mu.Lock("DeleteImage")
	defer b.mu.Unlock()

	img, aerr := get[*Image](b, id)
	if aerr != nil {
		return aerr
	}

	if img.LifecycleState == statDeleted {
		return errNotFound(id)
	}

	if aerr = checkEtag(img, ifMatch); aerr != nil {
		return aerr
	}

	now := b.nowUTC()
	img.LifecycleState, img.Internal.TimeDeleted = statDeleted, &now
	b.bump(img)

	return nil
}
