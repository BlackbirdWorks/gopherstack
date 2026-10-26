package ocicore

import "time"

// Compute and block storage resource kinds, as they appear in OCIDs.
const (
	kindInstance     = "instance"
	kindImage        = "image"
	kindVnic         = "vnic"
	kindPrivateIP    = "privateip"
	kindVnicAttach   = "vnicattachment"
	kindVolume       = "volume"
	kindBootVolume   = "bootvolume"
	kindVolumeAttach = "volumeattachment"
	kindBootAttach   = "instancebootvolumeattachment"
	kindBackup       = "volumebackup"
	kindPolicy       = "volumebackuppolicy"
	kindAssign       = "volumebackuppolicyassignment"
)

// Compute and block storage lifecycle states.
const (
	statRunning   = "RUNNING"
	statStarting  = "STARTING"
	statStopping  = "STOPPING"
	statStopped   = "STOPPED"
	statAttaching = "ATTACHING"
	statAttached  = "ATTACHED"
	statDetaching = "DETACHING"
	statDetached  = "DETACHED"
	statCreating  = "CREATING"
	statRestoring = "RESTORING"
	statDeleted   = "DELETED"
)

// InstanceShapeConfig is the resolved shape configuration of an instance.
type InstanceShapeConfig struct {
	ProcessorDescription      string  `json:"processorDescription,omitempty"`
	Ocpus                     float64 `json:"ocpus"`
	MemoryInGBs               float64 `json:"memoryInGBs"`
	Vcpus                     int     `json:"vcpus"`
	NetworkingBandwidthInGbps float64 `json:"networkingBandwidthInGbps"`
	MaxVnicAttachments        int     `json:"maxVnicAttachments"`
}

// SourceDetails is the polymorphic instance source (sourceType image or bootVolume).
type SourceDetails struct {
	SourceType          string `json:"sourceType"`
	ImageID             string `json:"imageId,omitempty"`
	BootVolumeID        string `json:"bootVolumeId,omitempty"`
	BootVolumeSizeInGBs *int64 `json:"bootVolumeSizeInGBs,omitempty"`
	BootVolumeVpusPerGB *int64 `json:"bootVolumeVpusPerGB,omitempty"`
	KmsKeyID            string `json:"kmsKeyId,omitempty"`
}

// Instance is a compute instance; only its control plane state exists.
type Instance struct {
	Metadata           map[string]string    `json:"metadata"`
	InstanceOptions    map[string]any       `json:"instanceOptions,omitempty"`
	AvailabilityConfig map[string]any       `json:"availabilityConfig,omitempty"`
	LaunchOptions      map[string]any       `json:"launchOptions,omitempty"`
	AgentConfig        map[string]any       `json:"agentConfig,omitempty"`
	ExtendedMetadata   map[string]any       `json:"extendedMetadata"`
	ShapeConfig        *InstanceShapeConfig `json:"shapeConfig,omitempty"`
	SourceDetails      *SourceDetails       `json:"sourceDetails,omitempty"`
	Base
	Region             string `json:"region"`
	LaunchMode         string `json:"launchMode"`
	ImageID            string `json:"imageId,omitempty"`
	Shape              string `json:"shape"`
	FaultDomain        string `json:"faultDomain"`
	AvailabilityDomain string `json:"availabilityDomain"`
}

// Image is a platform or custom image.
type Image struct {
	Base
	OperatingSystem        string `json:"operatingSystem"`
	OperatingSystemVersion string `json:"operatingSystemVersion"`
	BaseImageID            string `json:"baseImageId,omitempty"`
	LaunchMode             string `json:"launchMode"`
	ListingType            string `json:"listingType"`
	SizeInMBs              int64  `json:"sizeInMBs"`
	BillableSizeInGBs      int64  `json:"billableSizeInGBs"`
	CreateImageAllowed     bool   `json:"createImageAllowed"`
}

// Vnic is a virtual network interface card.
type Vnic struct {
	Base
	AvailabilityDomain  string   `json:"availabilityDomain"`
	HostnameLabel       string   `json:"hostnameLabel,omitempty"`
	MacAddress          string   `json:"macAddress"`
	PrivateIP           string   `json:"privateIp"`
	PublicIP            string   `json:"publicIp,omitempty"`
	SubnetID            string   `json:"subnetId"`
	NsgIDs              []string `json:"nsgIds"`
	IsPrimary           bool     `json:"isPrimary"`
	SkipSourceDestCheck bool     `json:"skipSourceDestCheck"`
}

// PrivateIP is a private IP address on a VNIC (or an unassigned reserved one).
type PrivateIP struct {
	Base
	AvailabilityDomain string `json:"availabilityDomain,omitempty"`
	HostnameLabel      string `json:"hostnameLabel,omitempty"`
	IPAddress          string `json:"ipAddress"`
	IPState            string `json:"ipState"`
	Lifetime           string `json:"lifetime"`
	SubnetID           string `json:"subnetId"`
	VnicID             string `json:"vnicId,omitempty"`
	IsPrimary          bool   `json:"isPrimary"`
}

// VnicAttachment links a VNIC to an instance.
type VnicAttachment struct {
	Base
	AvailabilityDomain string `json:"availabilityDomain"`
	InstanceID         string `json:"instanceId"`
	SubnetID           string `json:"subnetId"`
	VnicID             string `json:"vnicId"`
	NicIndex           int    `json:"nicIndex"`
	VlanTag            int    `json:"vlanTag"`
}

// Volume is a block volume.
type Volume struct {
	SourceDetails *VolumeSource `json:"sourceDetails,omitempty"`
	Base
	AvailabilityDomain string `json:"availabilityDomain"`
	KmsKeyID           string `json:"kmsKeyId,omitempty"`
	SizeInMBs          int64  `json:"sizeInMBs"`
	SizeInGBs          int64  `json:"sizeInGBs"`
	VpusPerGB          int64  `json:"vpusPerGB"`
	IsHydrated         bool   `json:"isHydrated"`
	IsAutoTuneEnabled  bool   `json:"isAutoTuneEnabled"`
}

// VolumeSource is the polymorphic volume source (type volume, volumeBackup or bootVolume).
type VolumeSource struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

// BootVolume is an instance boot volume.
type BootVolume struct {
	SourceDetails *VolumeSource `json:"sourceDetails,omitempty"`
	Base
	AvailabilityDomain string `json:"availabilityDomain"`
	ImageID            string `json:"imageId,omitempty"`
	KmsKeyID           string `json:"kmsKeyId,omitempty"`
	SizeInMBs          int64  `json:"sizeInMBs"`
	SizeInGBs          int64  `json:"sizeInGBs"`
	VpusPerGB          int64  `json:"vpusPerGB"`
	IsHydrated         bool   `json:"isHydrated"`
	IsAutoTuneEnabled  bool   `json:"isAutoTuneEnabled"`
}

// VolumeAttachment is an iSCSI or paravirtualized volume attachment (attachmentType).
type VolumeAttachment struct {
	Base
	AttachmentType               string `json:"attachmentType"`
	AvailabilityDomain           string `json:"availabilityDomain"`
	InstanceID                   string `json:"instanceId"`
	VolumeID                     string `json:"volumeId"`
	Device                       string `json:"device,omitempty"`
	IscsiLoginState              string `json:"iscsiLoginState,omitempty"`
	Ipv4                         string `json:"ipv4,omitempty"`
	Iqn                          string `json:"iqn,omitempty"`
	ChapUsername                 string `json:"chapUsername,omitempty"`
	ChapSecret                   string `json:"chapSecret,omitempty"`
	EncryptionInTransitType      string `json:"encryptionInTransitType"`
	Port                         int    `json:"port,omitempty"`
	IsReadOnly                   bool   `json:"isReadOnly"`
	IsShareable                  bool   `json:"isShareable"`
	IsMultipath                  bool   `json:"isMultipath"`
	IsPvEncryptionInTransit      bool   `json:"isPvEncryptionInTransitEnabled"`
	IsAgentAutoIscsiLoginEnabled bool   `json:"isAgentAutoIscsiLoginEnabled,omitempty"`
}

// BootVolumeAttachment links a boot volume to an instance.
type BootVolumeAttachment struct {
	Base
	AvailabilityDomain string `json:"availabilityDomain"`
	BootVolumeID       string `json:"bootVolumeId"`
	InstanceID         string `json:"instanceId"`
}

// VolumeBackup is a block volume backup.
type VolumeBackup struct {
	TimeRequestReceived time.Time `json:"timeRequestReceived"`
	Base
	Type            string `json:"type"`
	SourceType      string `json:"sourceType"`
	VolumeID        string `json:"volumeId"`
	KmsKeyID        string `json:"kmsKeyId,omitempty"`
	SizeInMBs       int64  `json:"sizeInMBs"`
	SizeInGBs       int64  `json:"sizeInGBs"`
	UniqueSizeInGBs int64  `json:"uniqueSizeInGBs"`
	UniqueSizeInMbs int64  `json:"uniqueSizeInMbs"`
}

// BackupSchedule is one schedule of a volume backup policy.
type BackupSchedule struct {
	BackupType       string `json:"backupType"`
	Period           string `json:"period"`
	OffsetType       string `json:"offsetType,omitempty"`
	DayOfWeek        string `json:"dayOfWeek,omitempty"`
	Month            string `json:"month,omitempty"`
	TimeZone         string `json:"timeZone,omitempty"`
	RetentionSeconds int    `json:"retentionSeconds"`
	OffsetSeconds    int    `json:"offsetSeconds,omitempty"`
	HourOfDay        int    `json:"hourOfDay,omitempty"`
	DayOfMonth       int    `json:"dayOfMonth,omitempty"`
}

// VolumeBackupPolicy is a predefined or user defined backup policy; schedules are stored, never executed.
type VolumeBackupPolicy struct {
	Base
	DestinationRegion string           `json:"destinationRegion,omitempty"`
	Schedules         []BackupSchedule `json:"schedules"`
}

// BackupPolicyAssignment assigns a policy to a volume or boot volume.
type BackupPolicyAssignment struct {
	Base
	AssetID  string `json:"assetId"`
	PolicyID string `json:"policyId"`
}

// NsgVnic is one entry of ListNetworkSecurityGroupVnics.
type NsgVnic struct {
	TimeAssociated time.Time `json:"timeAssociated"`
	VnicID         string    `json:"vnicId"`
	ResourceID     string    `json:"resourceId"`
}

func (v *Instance) vcnRef() string               { return "" }
func (v *Image) vcnRef() string                  { return "" }
func (v *Vnic) vcnRef() string                   { return "" }
func (v *PrivateIP) vcnRef() string              { return "" }
func (v *VnicAttachment) vcnRef() string         { return "" }
func (v *Volume) vcnRef() string                 { return "" }
func (v *BootVolume) vcnRef() string             { return "" }
func (v *VolumeAttachment) vcnRef() string       { return "" }
func (v *BootVolumeAttachment) vcnRef() string   { return "" }
func (v *VolumeBackup) vcnRef() string           { return "" }
func (v *VolumeBackupPolicy) vcnRef() string     { return "" }
func (v *BackupPolicyAssignment) vcnRef() string { return "" }

func (v *Instance) view() resource {
	c := *v
	c.Internal = nil

	return &c
}

func (v *Image) view() resource {
	c := *v
	c.Internal = nil

	return &c
}

func (v *Vnic) view() resource {
	c := *v
	c.Internal = nil

	return &c
}

func (v *PrivateIP) view() resource {
	c := *v
	c.Internal = nil

	return &c
}

func (v *VnicAttachment) view() resource {
	c := *v
	c.Internal = nil

	return &c
}

func (v *Volume) view() resource {
	c := *v
	c.Internal = nil

	return &c
}

func (v *BootVolume) view() resource {
	c := *v
	c.Internal = nil

	return &c
}

func (v *VolumeAttachment) view() resource {
	c := *v
	c.Internal = nil

	return &c
}

func (v *BootVolumeAttachment) view() resource {
	c := *v
	c.Internal = nil

	return &c
}

func (v *VolumeBackup) view() resource {
	c := *v
	c.Internal = nil

	return &c
}

func (v *VolumeBackupPolicy) view() resource {
	c := *v
	c.Internal = nil

	return &c
}

func (v *BackupPolicyAssignment) view() resource {
	c := *v
	c.Internal = nil

	return &c
}
