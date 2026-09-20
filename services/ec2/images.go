package ec2

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// imageOwnerAliasAmazon is the well-known non-numeric OwnerID this backend's
// seeded public AMI catalog uses (imageOwnerAlias on the wire, not
// imageOwnerId -- see handler_images.go's knownImageOwnerAliases).
const imageOwnerAliasAmazon = "amazon"

// AMIStub is a static image entry.
type AMIStub struct {
	ImageID        string `json:"imageID,omitempty"`
	Name           string `json:"name,omitempty"`
	Description    string `json:"description,omitempty"`
	Architecture   string `json:"architecture,omitempty"`
	Platform       string `json:"platform,omitempty"`
	RootDeviceName string `json:"rootDeviceName,omitempty"`
	State          string `json:"state,omitempty"`
	// SourceImageID is the parent AMI this image was copied from via
	// CopyImage, or empty for root images. Used by GetImageAncestry.
	SourceImageID string `json:"sourceImageID,omitempty"`
	// OwnerID is the account ID (or "amazon" for the seeded public catalog
	// entries below) that owns this AMI, used by DescribeImages' Owner.N
	// filter.
	OwnerID string `json:"ownerID,omitempty"`
	// ImdsSupport/VirtualizationType are RegisterImage inputs echoed back
	// on DescribeImages; this backend has no IMDS or hypervisor simulation
	// to enforce either against.
	ImdsSupport        string `json:"imdsSupport,omitempty"`
	VirtualizationType string `json:"virtualizationType,omitempty"`
	// EnaSupportSet/EnaSupport and SriovNetSupport are RegisterImage inputs
	// echoed back on DescribeImages -- terraform-provider-aws's aws_ami
	// resource always sends both (SriovNetSupport defaults to "simple"), and
	// both are ForceNew: never echoing them back left every apply showing
	// permanent drift ("sriov_net_support forces replacement" on every plan).
	EnaSupportSet   bool   `json:"enaSupportSet,omitempty"`
	EnaSupport      bool   `json:"enaSupport,omitempty"`
	SriovNetSupport string `json:"sriovNetSupport,omitempty"`
	// BlockDeviceMappings is RegisterImage's BlockDeviceMapping.N.* echoed back
	// on DescribeImages -- real AWS clients (including the Terraform provider's
	// spot fleet/launch template flattening, which locates the root device's
	// mapping by DeviceName) rely on this list being populated.
	BlockDeviceMappings []ImageBlockDeviceMapping `json:"blockDeviceMappings,omitempty"`
}

// ImageBlockDeviceMapping mirrors ec2@v1.329.0 types.BlockDeviceMapping as
// echoed on an AMI (RegisterImage's request shape, DescribeImages' response
// shape).
type ImageBlockDeviceMapping struct {
	DeviceName          string `json:"deviceName,omitempty"`
	VirtualName         string `json:"virtualName,omitempty"`
	SnapshotID          string `json:"snapshotId,omitempty"`
	VolumeType          string `json:"volumeType,omitempty"`
	VolumeSize          int32  `json:"volumeSize,omitempty"`
	Iops                int32  `json:"iops,omitempty"`
	Throughput          int32  `json:"throughput,omitempty"`
	DeleteOnTermination bool   `json:"deleteOnTermination,omitempty"`
	Encrypted           bool   `json:"encrypted,omitempty"`
	NoDevice            bool   `json:"noDevice,omitempty"`
}

// InstanceTypeSpecification holds the instance type compatibility rules for an AMI
// (ec2@v1.329.0 types.InstanceTypeSpecification), set via ReplaceImageInstanceTypeSpecification.
type InstanceTypeSpecification struct {
	SupportedInstanceTypes   []string `json:"supportedInstanceTypes,omitempty"`
	UnsupportedInstanceTypes []string `json:"unsupportedInstanceTypes,omitempty"`
}

//nolint:gochecknoglobals // package-level stub data for describe operations
var stubAMIs = []AMIStub{
	{
		ImageID:        "ami-0c55b159cbfafe1f0",
		Name:           "amzn2-ami-hvm",
		Description:    "Amazon Linux 2 (x86_64)",
		Architecture:   archX8664,
		RootDeviceName: "/dev/xvda",
		OwnerID:        imageOwnerAliasAmazon,
	},
	{
		ImageID:        "ami-0eb260c4d5475b901",
		Name:           "ubuntu-22.04-lts",
		Description:    "Ubuntu 22.04 LTS (x86_64)",
		Architecture:   archX8664,
		RootDeviceName: "/dev/sda1",
		OwnerID:        imageOwnerAliasAmazon,
	},
	{
		ImageID:        "ami-09d3b3274b6c5d4aa",
		Name:           "windows-server-2022",
		Description:    "Windows Server 2022",
		Architecture:   archX8664,
		Platform:       "windows",
		RootDeviceName: "/dev/sda1",
		OwnerID:        imageOwnerAliasAmazon,
	},
}

// DescribeImages returns stub AMIs, with State overridden to "disabled" for
// any image DisableImage was called on -- DisableImage/EnableImage track
// disabled state in b.imageDisabled rather than on the AMIStub itself, since
// stubAMIs is a shared package-level slice.
func (b *InMemoryBackend) DescribeImages() []AMIStub {
	b.mu.RLock("DescribeImages")
	defer b.mu.RUnlock()

	images := make([]AMIStub, 0, len(stubAMIs)+b.images.Len())
	images = append(images, stubAMIs...)
	for _, img := range b.images.All() {
		cp := *img
		images = append(images, cp)
	}

	for i := range images {
		if b.imageDisabled[images[i].ImageID] {
			images[i].State = stateDisabledImg
		}
	}

	return images
}

// DisableImage sets an AMI to disabled state.
func (b *InMemoryBackend) DisableImage(imageID string) error {
	if imageID == "" {
		return fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisableImage")
	defer b.mu.Unlock()

	b.imageDisabled[imageID] = true

	return nil
}

// EnableImage restores an AMI from disabled state.
func (b *InMemoryBackend) EnableImage(imageID string) error {
	if imageID == "" {
		return fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("EnableImage")
	defer b.mu.Unlock()

	delete(b.imageDisabled, imageID)

	return nil
}

// EnableImageBlockPublicAccess sets the account-level block for public AMI sharing.
func (b *InMemoryBackend) EnableImageBlockPublicAccess(state string) error {
	b.mu.Lock("EnableImageBlockPublicAccess")
	defer b.mu.Unlock()

	b.imageBlockPublicAccess = state

	return nil
}

// DisableImageBlockPublicAccess clears the account-level block.
func (b *InMemoryBackend) DisableImageBlockPublicAccess() {
	b.mu.Lock("DisableImageBlockPublicAccess")
	defer b.mu.Unlock()

	b.imageBlockPublicAccess = stateImageUnblocked
}

// GetImageBlockPublicAccessState returns the current image block public access state.
func (b *InMemoryBackend) GetImageBlockPublicAccessState() string {
	b.mu.RLock("GetImageBlockPublicAccessState")
	defer b.mu.RUnlock()

	if b.imageBlockPublicAccess == "" {
		return stateImageUnblocked
	}

	return b.imageBlockPublicAccess
}

// EnableImageDeprecation sets a deprecation time for an AMI.
func (b *InMemoryBackend) EnableImageDeprecation(imageID, deprecateAt string) error {
	if imageID == "" {
		return fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("EnableImageDeprecation")
	defer b.mu.Unlock()

	b.imageDeprecated[imageID] = deprecateAt

	return nil
}

// ImageDeprecation returns a copy of the imageID -> DeprecateAt map
// EnableImageDeprecation/DisableImageDeprecation maintain, for
// DescribeImages' DeprecationTime echo and IncludeDeprecated filter.
func (b *InMemoryBackend) ImageDeprecation() map[string]string {
	b.mu.RLock("ImageDeprecation")
	defer b.mu.RUnlock()

	out := make(map[string]string, len(b.imageDeprecated))
	maps.Copy(out, b.imageDeprecated)

	return out
}

// DisableImageDeprecation removes deprecation from an AMI.
func (b *InMemoryBackend) DisableImageDeprecation(imageID string) error {
	if imageID == "" {
		return fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisableImageDeprecation")
	defer b.mu.Unlock()

	delete(b.imageDeprecated, imageID)

	return nil
}

// EnableImageDeregistrationProtection protects an AMI from deregistration.
func (b *InMemoryBackend) EnableImageDeregistrationProtection(imageID string) error {
	if imageID == "" {
		return fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("EnableImageDeregistrationProtection")
	defer b.mu.Unlock()

	b.imageDeregistrationProtection[imageID] = true

	return nil
}

// DisableImageDeregistrationProtection removes deregistration protection from an AMI.
func (b *InMemoryBackend) DisableImageDeregistrationProtection(imageID string) error {
	if imageID == "" {
		return fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisableImageDeregistrationProtection")
	defer b.mu.Unlock()

	delete(b.imageDeregistrationProtection, imageID)

	return nil
}

// ModifyImageAttribute modifies a mutable AMI attribute.
func (b *InMemoryBackend) ModifyImageAttribute(imageID, attribute, value string) error {
	if imageID == "" {
		return fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyImageAttribute")
	defer b.mu.Unlock()

	if b.imageAttributes[imageID] == nil {
		b.imageAttributes[imageID] = make(map[string]string)
	}
	b.imageAttributes[imageID][attribute] = value

	return nil
}

// ReplaceImageInstanceTypeSpecification replaces (or, when both lists are empty, removes) the
// instance type compatibility rules for an AMI this account owns. Real AWS restricts this to
// the AMI owner (api_op_ReplaceImageInstanceTypeSpecification.go); this backend's only
// non-owned images are the seeded public catalog (stubAMIs, OwnerID "amazon").
func (b *InMemoryBackend) ReplaceImageInstanceTypeSpecification(
	imageID string, supported, unsupported []string,
) error {
	if imageID == "" {
		return fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ReplaceImageInstanceTypeSpecification")
	defer b.mu.Unlock()

	if _, ok := b.images.Get(imageID); !ok {
		if b.lookupImageLocked(imageID) != nil {
			return fmt.Errorf("%w: %s is not owned by this account", ErrImageNotOwner, imageID)
		}

		return fmt.Errorf("%w: %s", ErrImageNotFound, imageID)
	}

	if len(supported) == 0 && len(unsupported) == 0 {
		delete(b.imageInstanceTypeSpecs, imageID)

		return nil
	}

	b.imageInstanceTypeSpecs[imageID] = &InstanceTypeSpecification{
		SupportedInstanceTypes:   append([]string(nil), supported...),
		UnsupportedInstanceTypes: append([]string(nil), unsupported...),
	}

	return nil
}

// GetImageInstanceTypeSpecification returns the instance type compatibility rules previously
// set by ReplaceImageInstanceTypeSpecification, or nil if none is set. Used by DescribeImages
// to echo the instanceTypeSpecification wire field.
func (b *InMemoryBackend) GetImageInstanceTypeSpecification(imageID string) *InstanceTypeSpecification {
	b.mu.RLock("GetImageInstanceTypeSpecification")
	defer b.mu.RUnlock()

	spec, ok := b.imageInstanceTypeSpecs[imageID]
	if !ok {
		return nil
	}

	cp := *spec
	cp.SupportedInstanceTypes = append([]string(nil), spec.SupportedInstanceTypes...)
	cp.UnsupportedInstanceTypes = append([]string(nil), spec.UnsupportedInstanceTypes...)

	return &cp
}

// ModifyImageLaunchPermission applies LaunchPermission.Add/Remove account IDs
// and the "all" (public) group to an AMI, as tracked per-grantee state --
// real AWS clients (including the Terraform provider's aws_ami_launch_permission
// resource) read back the exact grantee they added via DescribeImageAttribute,
// not a fixed stub.
func (b *InMemoryBackend) ModifyImageLaunchPermission(
	imageID string, addAccountIDs []string, addPublic bool, removeAccountIDs []string, removePublic bool,
) error {
	if imageID == "" {
		return fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyImageLaunchPermission")
	defer b.mu.Unlock()

	if b.imageLaunchPermissions[imageID] == nil {
		b.imageLaunchPermissions[imageID] = make(map[string]bool)
	}

	for _, id := range addAccountIDs {
		b.imageLaunchPermissions[imageID][id] = true
	}

	for _, id := range removeAccountIDs {
		delete(b.imageLaunchPermissions[imageID], id)
	}

	if addPublic {
		b.imageLaunchPermissionPublic[imageID] = true
	}

	if removePublic {
		delete(b.imageLaunchPermissionPublic, imageID)
	}

	return nil
}

// GetImageLaunchPermission returns the account IDs an AMI has been shared
// with and whether it has been made public, as previously set by
// ModifyImageLaunchPermission.
func (b *InMemoryBackend) GetImageLaunchPermission(imageID string) ([]string, bool) {
	b.mu.RLock("GetImageLaunchPermission")
	defer b.mu.RUnlock()

	ids := make([]string, 0, len(b.imageLaunchPermissions[imageID]))
	for id := range b.imageLaunchPermissions[imageID] {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	public := b.imageLaunchPermissionPublic[imageID]
	if !public && len(ids) == 0 {
		// The seeded public catalog (stubAMIs, owned by "amazon") is public
		// by definition and was never individually shared -- default to
		// public rather than reporting a catalog AMI as private.
		if img := b.lookupImageLocked(imageID); img != nil && knownImageOwnerAliases[img.OwnerID] {
			public = true
		}
	}

	return ids, public
}

// GetImageAttribute returns a previously-set simple string AMI attribute
// (as stored by ModifyImageAttribute), or "" if never set.
func (b *InMemoryBackend) GetImageAttribute(imageID, attribute string) string {
	b.mu.RLock("GetImageAttribute")
	defer b.mu.RUnlock()

	return b.imageAttributes[imageID][attribute]
}

// ResetImageAttribute resets an AMI attribute to its default.
func (b *InMemoryBackend) ResetImageAttribute(imageID, attribute string) error {
	if imageID == "" {
		return fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ResetImageAttribute")
	defer b.mu.Unlock()

	if m, ok := b.imageAttributes[imageID]; ok {
		delete(m, attribute)
	}

	return nil
}

// InstanceImageMetadataItem holds image-related metadata for a single instance.
type InstanceImageMetadataItem struct {
	LaunchTime       time.Time
	InstanceID       string `json:"instanceID,omitempty"`
	ImageID          string `json:"imageID,omitempty"`
	ImageName        string `json:"imageName,omitempty"`
	ImageState       string `json:"imageState,omitempty"`
	ImageOwnerID     string `json:"imageOwnerID,omitempty"`
	AvailabilityZone string `json:"availabilityZone,omitempty"`
	ZoneID           string `json:"zoneID,omitempty"`
	InstanceType     string `json:"instanceType,omitempty"`
	OwnerID          string `json:"ownerID,omitempty"`
	StateName        string `json:"stateName,omitempty"`
	StateCode        int    `json:"stateCode,omitempty"`
}

// DescribeInstanceImageMetadata returns image metadata for instances (or all).
func (b *InMemoryBackend) DescribeInstanceImageMetadata(
	instanceIDs []string,
) []InstanceImageMetadataItem {
	b.mu.RLock("DescribeInstanceImageMetadata")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(instanceIDs))
	for _, id := range instanceIDs {
		filter[id] = true
	}

	var out []InstanceImageMetadataItem
	for _, inst := range b.instances.All() {
		if len(filter) > 0 && !filter[inst.ID] {
			continue
		}
		imageState := stateAvailableImg
		if b.imageDisabled[inst.ImageID] {
			imageState = stateDisabledImg
		}

		var imageName string
		if img := b.lookupImageLocked(inst.ImageID); img != nil {
			imageName = img.Name
		}

		az := inst.Placement.AvailabilityZone

		var zoneID string
		if az != "" {
			zoneID = az + "1"
		}

		out = append(out, InstanceImageMetadataItem{
			InstanceID:       inst.ID,
			ImageID:          inst.ImageID,
			ImageName:        imageName,
			ImageState:       imageState,
			ImageOwnerID:     b.AccountID,
			AvailabilityZone: az,
			ZoneID:           zoneID,
			InstanceType:     inst.InstanceType,
			OwnerID:          b.AccountID,
			StateName:        inst.State.Name,
			StateCode:        inst.State.Code,
			LaunchTime:       inst.LaunchTime,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].InstanceID < out[j].InstanceID })

	return out
}

// ---- Serial console ----

// RegisterImage registers a new AMI from a snapshot or manifest.
func (b *InMemoryBackend) RegisterImage(name, description, architecture string) (*AMIStub, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("RegisterImage")
	defer b.mu.Unlock()

	img := &AMIStub{
		ImageID:      newAMIID(),
		Name:         name,
		Description:  description,
		Architecture: architecture,
		OwnerID:      b.AccountID,
	}
	if img.Architecture == "" {
		img.Architecture = archX8664
	}
	b.images.Put(img)

	return img, nil
}

// SetImageMetadata applies RegisterImage's ImdsSupport/VirtualizationType --
// both declare+apply-only fields (empty is left unset): this backend has no
// IMDS or hypervisor simulation to enforce either against.
func (b *InMemoryBackend) SetImageMetadata(imageID, imdsSupport, virtualizationType string) {
	b.mu.Lock("SetImageMetadata")
	defer b.mu.Unlock()

	img, ok := b.images.Get(imageID)
	if !ok {
		return
	}

	if imdsSupport != "" {
		img.ImdsSupport = imdsSupport
	}

	if virtualizationType != "" {
		img.VirtualizationType = virtualizationType
	}
}

// SetImageEnhancedNetworking applies RegisterImage's EnaSupport/SriovNetSupport
// to an existing image, so DescribeImages can echo them back (both are
// ForceNew on aws_ami, so leaving them unset causes permanent apply drift).
func (b *InMemoryBackend) SetImageEnhancedNetworking(
	imageID string, enaSupportSet, enaSupport bool, sriovNetSupport string,
) {
	b.mu.Lock("SetImageEnhancedNetworking")
	defer b.mu.Unlock()

	img, ok := b.images.Get(imageID)
	if !ok {
		return
	}

	if enaSupportSet {
		img.EnaSupportSet = true
		img.EnaSupport = enaSupport
	}

	if sriovNetSupport != "" {
		img.SriovNetSupport = sriovNetSupport
	}
}

// SetImageRootDeviceName applies RegisterImage's RootDeviceName -- a
// declare+echo field this backend does not derive from BlockDeviceMapping
// when the caller omits it (see PARITY.md).
func (b *InMemoryBackend) SetImageRootDeviceName(imageID, rootDeviceName string) {
	if rootDeviceName == "" {
		return
	}

	b.mu.Lock("SetImageRootDeviceName")
	defer b.mu.Unlock()

	img, ok := b.images.Get(imageID)
	if !ok {
		return
	}

	img.RootDeviceName = rootDeviceName
}

// SetImageBlockDeviceMappings applies RegisterImage's BlockDeviceMapping.N.*
// entries to an existing image, so DescribeImages can echo them back.
func (b *InMemoryBackend) SetImageBlockDeviceMappings(imageID string, mappings []ImageBlockDeviceMapping) {
	if len(mappings) == 0 {
		return
	}

	b.mu.Lock("SetImageBlockDeviceMappings")
	defer b.mu.Unlock()

	img, ok := b.images.Get(imageID)
	if !ok {
		return
	}

	img.BlockDeviceMappings = mappings
}

// ImportImage creates an import task for importing a VM image.
func (b *InMemoryBackend) ImportImage(
	description, architecture, platform string, encrypted bool, kmsKeyID string,
) (*ImageImportTask, error) {
	b.mu.Lock("ImportImage")
	defer b.mu.Unlock()

	if encrypted && kmsKeyID == "" {
		kmsKeyID = defaultEBSKmsKeyAlias
	}

	task := &ImageImportTask{
		ImportTaskID: "import-ami-" + uuid.New().String()[:8],
		Description:  description,
		Architecture: architecture,
		Platform:     platform,
		Status:       stateTaskCompleted,
		Encrypted:    encrypted,
		KmsKeyID:     kmsKeyID,
	}
	b.imageImportTasks.Put(task)

	return task, nil
}

// DescribeImportImageTasks returns import image tasks.
func (b *InMemoryBackend) DescribeImportImageTasks(taskIDs []string) []*ImageImportTask {
	b.mu.RLock("DescribeImportImageTasks")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(taskIDs))
	for _, id := range taskIDs {
		filter[id] = true
	}

	var out []*ImageImportTask
	for _, t := range b.imageImportTasks.All() {
		if len(filter) > 0 && !filter[t.ImportTaskID] {
			continue
		}
		cp := *t
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ImportTaskID < out[j].ImportTaskID })

	return out
}

// ExportImage creates an export task for an AMI, storing task state so it can later be
// retrieved (and settled to "completed") via DescribeExportImageTasks. If imageID matches a
// registered AMI, its architecture is folded into the task description when the caller did
// not supply one.
func (b *InMemoryBackend) ExportImage(
	imageID, description, diskImageFormat, s3Bucket, s3Prefix, roleName string,
) (*ExportImageTaskRec, error) {
	if imageID == "" {
		return nil, fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ExportImage")
	defer b.mu.Unlock()

	if description == "" {
		if img, ok := b.images.Get(imageID); ok {
			description = img.Description
		}
	}

	if diskImageFormat == "" {
		diskImageFormat = defaultExportImageFormat
	}

	if s3Bucket == "" {
		s3Bucket = "export-images-" + b.AccountID
	}

	if roleName == "" {
		roleName = "vmimport"
	}

	id := "export-ami-" + uuid.New().String()[:8]
	task := &ExportImageTaskRec{
		ExportImageTaskID: id,
		Description:       description,
		ImageID:           imageID,
		DiskImageFormat:   diskImageFormat,
		Progress:          taskZeroProgress,
		Status:            vmTaskStateActive,
		StatusMessage:     vmTaskStateActive,
		S3Bucket:          s3Bucket,
		S3Prefix:          s3Prefix,
		RoleName:          roleName,
	}
	b.exportImageTasks.Put(task)

	cp := *task

	return &cp, nil
}

// ListImagesInRecycleBin returns soft-deleted AMIs.
func (b *InMemoryBackend) ListImagesInRecycleBin(imageIDs []string) []*RecycleBinImage {
	b.mu.RLock("ListImagesInRecycleBin")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(imageIDs))
	for _, id := range imageIDs {
		filter[id] = true
	}

	var out []*RecycleBinImage
	for _, img := range b.recycleBinImages.All() {
		if len(filter) > 0 && !filter[img.ImageID] {
			continue
		}
		cp := *img
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ImageID < out[j].ImageID })

	return out
}

// RestoreImageFromRecycleBin restores a soft-deleted AMI, moving it back out of
// the recycle bin and into the available image set.
//
// gopherstack models no Recycle Bin service, so DeregisterImage always deletes
// permanently and the bin is normally empty; this must report not-found rather
// than a false success for a nonexistent image.
func (b *InMemoryBackend) RestoreImageFromRecycleBin(imageID string) error {
	if imageID == "" {
		return fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("RestoreImageFromRecycleBin")
	defer b.mu.Unlock()

	binned, ok := b.recycleBinImages.Get(imageID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrImageNotFound, imageID)
	}

	// Restoring must return the AMI to the available set, not merely drop the
	// bin row. Only re-create it when the live image is genuinely absent, so a
	// restore never clobbers a real image that shares the ID.
	if !b.images.Has(imageID) {
		b.images.Put(&AMIStub{
			ImageID: binned.ImageID,
			Name:    binned.Name,
			State:   stateAvailableImg,
		})
	}

	b.recycleBinImages.Delete(imageID)

	return nil
}

// ---- Snapshot recycle bin ----

// FastLaunchConfig carries the EnableFastLaunch request parameters that
// DescribeFastLaunchImages must echo back (ec2@v1.319.1
// DescribeFastLaunchImagesSuccessItem: launchTemplate/maxParallelLaunches/
// resourceType/snapshotConfiguration). EnableFastLaunch previously discarded
// all of these, storing only a bool.
type FastLaunchConfig struct {
	ResourceType                string
	LaunchTemplateID            string
	LaunchTemplateName          string
	LaunchTemplateVersion       string
	MaxParallelLaunches         int
	SnapshotTargetResourceCount int
	HasLaunchTemplate           bool
	HasSnapshotConfiguration    bool
}

// EnableFastLaunch enables Windows fast launch for an AMI, storing the
// requested configuration for DescribeFastLaunchImages to report back.
func (b *InMemoryBackend) EnableFastLaunch(imageID string, cfg FastLaunchConfig) error {
	if imageID == "" {
		return fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("EnableFastLaunch")
	defer b.mu.Unlock()

	b.fastLaunchImages[imageID] = &FastLaunchImageItem{
		ImageID:                     imageID,
		State:                       stateEnabledFastLaunch,
		ResourceType:                cfg.ResourceType,
		LaunchTemplateID:            cfg.LaunchTemplateID,
		LaunchTemplateName:          cfg.LaunchTemplateName,
		LaunchTemplateVersion:       cfg.LaunchTemplateVersion,
		MaxParallelLaunches:         cfg.MaxParallelLaunches,
		SnapshotTargetResourceCount: cfg.SnapshotTargetResourceCount,
		HasLaunchTemplate:           cfg.HasLaunchTemplate,
		HasSnapshotConfiguration:    cfg.HasSnapshotConfiguration,
	}

	return nil
}

// DisableFastLaunch disables Windows fast launch for an AMI, returning the
// configuration that was in effect (or nil if the AMI was never enabled) so
// the handler can echo it back on DisableFastLaunchOutput.
func (b *InMemoryBackend) DisableFastLaunch(imageID string) (*FastLaunchImageItem, error) {
	if imageID == "" {
		return nil, fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisableFastLaunch")
	defer b.mu.Unlock()

	prev := b.fastLaunchImages[imageID]
	delete(b.fastLaunchImages, imageID)

	return prev, nil
}

// FastLaunchImageItem holds fast launch state and configuration for a single AMI.
type FastLaunchImageItem struct {
	ImageID                     string `json:"imageID,omitempty"`
	State                       string `json:"state,omitempty"`
	ResourceType                string `json:"resourceType,omitempty"`
	LaunchTemplateID            string `json:"launchTemplateID,omitempty"`
	LaunchTemplateName          string `json:"launchTemplateName,omitempty"`
	LaunchTemplateVersion       string `json:"launchTemplateVersion,omitempty"`
	MaxParallelLaunches         int    `json:"maxParallelLaunches,omitempty"`
	SnapshotTargetResourceCount int    `json:"snapshotTargetResourceCount,omitempty"`
	HasLaunchTemplate           bool   `json:"hasLaunchTemplate,omitempty"`
	HasSnapshotConfiguration    bool   `json:"hasSnapshotConfiguration,omitempty"`
}

// DescribeFastLaunchImages returns AMIs with fast launch enabled.
func (b *InMemoryBackend) DescribeFastLaunchImages(imageIDs []string) []FastLaunchImageItem {
	b.mu.RLock("DescribeFastLaunchImages")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(imageIDs))
	for _, id := range imageIDs {
		filter[id] = true
	}

	var out []FastLaunchImageItem
	for imageID, item := range b.fastLaunchImages {
		if len(filter) > 0 && !filter[imageID] {
			continue
		}
		out = append(out, *item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ImageID < out[j].ImageID })

	return out
}

// CopyImage copies an AMI stub, producing a new ID.
func (b *InMemoryBackend) CopyImage(sourceImageID, name, description string) (*AMIStub, error) {
	if sourceImageID == "" {
		return nil, fmt.Errorf("%w: SourceImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CopyImage")
	defer b.mu.Unlock()

	src := b.lookupImageLocked(sourceImageID)
	if src == nil {
		return nil, fmt.Errorf("%w: source AMI %s not found", ErrInvalidParameter, sourceImageID)
	}

	if name == "" {
		name = "copy-of-" + src.Name
	}

	if description == "" {
		description = src.Description
	}

	newImage := &AMIStub{
		ImageID:        newAMIID(),
		Name:           name,
		Description:    description,
		Architecture:   src.Architecture,
		Platform:       src.Platform,
		RootDeviceName: src.RootDeviceName,
		SourceImageID:  src.ImageID,
		OwnerID:        b.AccountID,
	}
	b.images.Put(newImage)

	cp := *newImage

	return &cp, nil
}

// DeregisterImage removes an AMI from the image store.
func (b *InMemoryBackend) DeregisterImage(imageID string) error {
	if imageID == "" {
		return fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeregisterImage")
	defer b.mu.Unlock()

	if _, ok := b.images.Get(imageID); !ok {
		return fmt.Errorf("%w: %s", ErrImageNotFound, imageID)
	}
	b.images.Delete(imageID)
	delete(b.tags, imageID)
	delete(b.imageAttributes, imageID)
	delete(b.imageDisabled, imageID)
	delete(b.imageDeprecated, imageID)
	delete(b.imageDeregistrationProtection, imageID)
	delete(b.fastLaunchImages, imageID)
	delete(b.imageWatermarks, imageID)
	delete(b.imageLaunchPermissions, imageID)
	delete(b.imageLaunchPermissionPublic, imageID)

	return nil
}

// ---- VPC / Subnet attribute mutations ----

// lookupImageLocked returns the AMI with the given ID from either the
// dynamic images map or the seeded stubAMIs list, or nil if not found. Must
// be called with b.mu held (for reading or writing). Shared by CopyImage,
// GetImageAncestry, and DescribeImageReferences so all three agree on what
// counts as a known image.
func (b *InMemoryBackend) lookupImageLocked(imageID string) *AMIStub {
	if existing, ok := b.images.Get(imageID); ok {
		return existing
	}

	for _, a := range stubAMIs {
		if a.ImageID == imageID {
			cp := a

			return &cp
		}
	}

	return nil
}

// CancelImageLaunchPermission removes the launch permission entry for an AMI,
// reusing the same imageAttributes store ModifyImageAttribute/ResetImageAttribute
// maintain for the "launchPermission" attribute.
func (b *InMemoryBackend) CancelImageLaunchPermission(imageID string) error {
	if imageID == "" {
		return fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CancelImageLaunchPermission")
	defer b.mu.Unlock()

	if m, ok := b.imageAttributes[imageID]; ok {
		delete(m, "launchPermission")
	}

	return nil
}

// DescribeImageReferences scans existing instance and launch template state
// for resources referencing the given AMIs.
func (b *InMemoryBackend) DescribeImageReferences(imageIDs []string) []*ImageReferenceEntry {
	b.mu.RLock("DescribeImageReferences")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(imageIDs))
	for _, id := range imageIDs {
		filter[id] = true
	}

	out := make([]*ImageReferenceEntry, 0)

	for _, inst := range b.instances.All() {
		if inst.ImageID == "" || (len(filter) > 0 && !filter[inst.ImageID]) {
			continue
		}

		out = append(out, &ImageReferenceEntry{
			ImageID:      inst.ImageID,
			ResourceType: imageRefTypeInstance,
			Arn:          arn.Build("ec2", b.Region, b.AccountID, "instance/"+inst.ID),
		})
	}

	for _, lt := range b.launchTemplates.All() {
		if lt.ImageID == "" || (len(filter) > 0 && !filter[lt.ImageID]) {
			continue
		}

		out = append(out, &ImageReferenceEntry{
			ImageID:      lt.ImageID,
			ResourceType: imageRefTypeLaunchTemplate,
			Arn:          arn.Build("ec2", b.Region, b.AccountID, "launch-template/"+lt.ID),
		})
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].ImageID != out[j].ImageID {
			return out[i].ImageID < out[j].ImageID
		}

		return out[i].Arn < out[j].Arn
	})

	return out
}

// GetImageAncestry walks the SourceImageID chain (populated by CopyImage)
// from the given AMI up to its root.
func (b *InMemoryBackend) GetImageAncestry(imageID string) ([]*ImageAncestryEntry, error) {
	if imageID == "" {
		return nil, fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetImageAncestry")
	defer b.mu.RUnlock()

	img := b.lookupImageLocked(imageID)
	if img == nil {
		return nil, fmt.Errorf("%w: %s", ErrImageNotFound, imageID)
	}

	out := make([]*ImageAncestryEntry, 0, 1)
	seen := make(map[string]bool, 1)
	cur := img

	for depth := 0; cur != nil && depth < maxImageAncestryDepth; depth++ {
		if seen[cur.ImageID] {
			break
		}

		seen[cur.ImageID] = true

		out = append(out, &ImageAncestryEntry{
			ImageID:       cur.ImageID,
			SourceImageID: cur.SourceImageID,
		})

		if cur.SourceImageID == "" {
			break
		}

		cur = b.lookupImageLocked(cur.SourceImageID)
	}

	return out, nil
}

const (
	imageRefTypeInstance       = "ec2:Instance"
	imageRefTypeLaunchTemplate = "ec2:LaunchTemplate"

	// maxImageAncestryDepth bounds the SourceImageID walk in GetImageAncestry
	// so a (never expected) cycle in copy history cannot loop forever.
	maxImageAncestryDepth = 32
)

// ImageReferenceEntry is a single resource-reference row returned by
// DescribeImageReferences, derived from existing instance and launch
// template state.
type ImageReferenceEntry struct {
	ImageID      string
	ResourceType string
	Arn          string
}

// ImageAncestryEntry is a single AMI in the ancestry chain returned by
// GetImageAncestry.
type ImageAncestryEntry struct {
	ImageID       string
	SourceImageID string
}
