package ocicore

import "time"

// Block storage resource kinds added beyond volumes and backups.
const (
	kindBootBackup  = "bootvolumebackup"
	kindVolumeGroup = "volumegroup"
	kindGroupBackup = "volumegroupbackup"
	kindReplica     = "blockvolumereplica"
)

// RetentionDuration is the retentionPeriod of a backup.
type RetentionDuration struct {
	RetentionTimeUnit   string `json:"retentionTimeUnit"`
	RetentionTimeAmount int    `json:"retentionTimeAmount"`
}

// Retention holds the backup retention settings shared by volume, boot volume and volume group backups.
type Retention struct {
	RetentionPeriod              *RetentionDuration `json:"retentionPeriod,omitempty"`
	TimeRetentionExpiresAt       *time.Time         `json:"timeRetentionExpiresAt,omitempty"`
	IsPreventDeletionEnabled     bool               `json:"isPreventDeletionEnabled"`
	IsRetentionLockEnabled       bool               `json:"isRetentionLockEnabled"`
	IsIndefiniteRetentionEnabled bool               `json:"isIndefiniteRetentionEnabled"`
}

// BootVolumeBackup is a boot volume backup.
type BootVolumeBackup struct {
	TimeRequestReceived time.Time `json:"timeRequestReceived"`
	Base
	Retention
	Type                     string `json:"type"`
	SourceType               string `json:"sourceType"`
	BootVolumeID             string `json:"bootVolumeId"`
	ImageID                  string `json:"imageId,omitempty"`
	KmsKeyID                 string `json:"kmsKeyId,omitempty"`
	SourceBootVolumeBackupID string `json:"sourceBootVolumeBackupId,omitempty"`
	VolumeGroupBackupID      string `json:"volumeGroupBackupId,omitempty"`
	Region                   string `json:"region,omitempty"`
	SizeInGBs                int64  `json:"sizeInGBs"`
	UniqueSizeInGBs          int64  `json:"uniqueSizeInGBs"`
}

// VolumeGroupSource is the polymorphic source of a volume group: one of the id fields is set.
type VolumeGroupSource struct {
	Type                 string   `json:"type"`
	VolumeGroupBackupID  string   `json:"volumeGroupBackupId,omitempty"`
	VolumeGroupID        string   `json:"volumeGroupId,omitempty"`
	VolumeGroupReplicaID string   `json:"volumeGroupReplicaId,omitempty"`
	VolumeIDs            []string `json:"volumeIds,omitempty"`
}

// VolumeGroup is a set of volumes and boot volumes backed up together.
type VolumeGroup struct {
	SourceDetails *VolumeGroupSource `json:"sourceDetails,omitempty"`
	Base
	AvailabilityDomain string   `json:"availabilityDomain"`
	VolumeIDs          []string `json:"volumeIds"`
	SizeInMBs          int64    `json:"sizeInMBs"`
	SizeInGBs          int64    `json:"sizeInGBs"`
	IsHydrated         bool     `json:"isHydrated"`
}

// VolumeGroupBackup is a crash-consistent backup of every member of a volume group.
type VolumeGroupBackup struct {
	TimeRequestReceived time.Time `json:"timeRequestReceived"`
	Base
	Retention
	Type                      string   `json:"type"`
	SourceType                string   `json:"sourceType"`
	VolumeGroupID             string   `json:"volumeGroupId,omitempty"`
	SourceVolumeGroupBackupID string   `json:"sourceVolumeGroupBackupId,omitempty"`
	Region                    string   `json:"region,omitempty"`
	VolumeBackupIDs           []string `json:"volumeBackupIds"`
	SizeInMBs                 int64    `json:"sizeInMBs"`
	SizeInGBs                 int64    `json:"sizeInGBs"`
	UniqueSizeInMbs           int64    `json:"uniqueSizeInMbs"`
	UniqueSizeInGbs           int64    `json:"uniqueSizeInGbs"`
}

// BlockVolumeReplica is the asynchronous copy of a block volume in another availability domain or region.
type BlockVolumeReplica struct {
	TimeLastSynced time.Time `json:"timeLastSynced"`
	Base
	AvailabilityDomain        string `json:"availabilityDomain"`
	BlockVolumeID             string `json:"blockVolumeId"`
	KmsKeyID                  string `json:"kmsKeyId,omitempty"`
	SizeInGBs                 int64  `json:"sizeInGBs"`
	TotalDataTransferredInGBs int64  `json:"totalDataTransferredInGBs"`
}

// BlockVolumeReplicaInfo is the replica summary embedded in a volume.
type BlockVolumeReplicaInfo struct {
	DisplayName          string `json:"displayName"`
	BlockVolumeReplicaID string `json:"blockVolumeReplicaId"`
	AvailabilityDomain   string `json:"availabilityDomain"`
	KmsKeyID             string `json:"kmsKeyId,omitempty"`
}

func (v *BootVolumeBackup) vcnRef() string { return "" }
func (v *VolumeGroup) vcnRef() string      { return "" }
func (v *VolumeGroupBackup) vcnRef() string {
	return ""
}
func (v *BlockVolumeReplica) vcnRef() string { return "" }

func (v *BootVolumeBackup) view() Resource {
	c := *v
	c.Internal, c.Region = nil, ""

	return &c
}

func (v *VolumeGroup) view() Resource {
	c := *v
	c.Internal = nil

	return &c
}

func (v *VolumeGroupBackup) view() Resource {
	c := *v
	c.Internal, c.Region = nil, ""

	return &c
}

func (v *BlockVolumeReplica) view() Resource {
	c := *v
	c.Internal = nil

	return &c
}
