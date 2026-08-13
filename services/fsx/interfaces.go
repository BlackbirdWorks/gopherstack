package fsx

import (
	"context"
	"strconv"
	"time"
)

// epochTime marshals to a JSON number of epoch seconds (with fractional
// milliseconds), matching the AWS JSON-RPC timestamp wire format that the
// FSx SDK deserializer expects.
type epochTime time.Time

// MarshalJSON renders the time as epoch seconds.
func (t epochTime) MarshalJSON() ([]byte, error) {
	ms := time.Time(t).UnixMilli()

	return []byte(strconv.FormatFloat(float64(ms)/1000.0, 'f', -1, 64)), nil
}

// StorageBackend is the interface for FSx storage operations.
type StorageBackend interface {
	CreateFileSystem(input *createFileSystemInput) (*FileSystem, error)
	DescribeFileSystems(ids []string, maxResults int32, nextToken string) ([]*FileSystem, string, error)
	DeleteFileSystem(fileSystemID string) error
	UpdateFileSystem(input *updateFileSystemInput) (*FileSystem, error)

	CreateBackup(input *createBackupInput) (*Backup, error)
	DescribeBackups(backupIDs []string, maxResults int32, nextToken string) ([]*Backup, string, error)
	DeleteBackup(backupID string) error
	CopyBackup(input *copyBackupInput) (*Backup, error)

	CreateFileSystemFromBackup(input *createFileSystemFromBackupInput) (*FileSystem, error)

	AssociateFileSystemAliases(fileSystemID string, aliases []string) ([]FileSystemAlias, error)
	DisassociateFileSystemAliases(fileSystemID string, aliases []string) ([]FileSystemAlias, error)
	DescribeFileSystemAliases(
		fileSystemID string,
		maxResults int32,
		nextToken string,
	) ([]FileSystemAlias, string, error)

	CreateDataRepositoryAssociation(input *createDataRepositoryAssociationInput) (*DataRepositoryAssociation, error)
	DeleteDataRepositoryAssociation(associationID string) error
	DescribeDataRepositoryAssociations(
		ids []string,
		maxResults int32,
		nextToken string,
	) ([]*DataRepositoryAssociation, string, error)
	UpdateDataRepositoryAssociation(input *updateDataRepositoryAssociationInput) (*DataRepositoryAssociation, error)

	CancelDataRepositoryTask(taskID string) error
	CreateDataRepositoryTask(input *createDataRepositoryTaskInput) (*DataRepositoryTask, error)
	DescribeDataRepositoryTasks(ids []string, maxResults int32, nextToken string) ([]*DataRepositoryTask, string, error)

	CreateFileCache(input *createFileCacheInput) (*FileCache, error)
	DeleteFileCache(fileCacheID string) error
	DescribeFileCaches(ids []string, maxResults int32, nextToken string) ([]*FileCache, string, error)
	UpdateFileCache(input *updateFileCacheInput) (*FileCache, error)

	CreateSnapshot(input *createSnapshotInput) (*Snapshot, error)
	DeleteSnapshot(snapshotID string) error
	DescribeSnapshots(ids []string, maxResults int32, nextToken string) ([]*Snapshot, string, error)
	UpdateSnapshot(input *updateSnapshotInput) (*Snapshot, error)
	CopySnapshotAndUpdateVolume(input *copySnapshotAndUpdateVolumeInput) (*Volume, error)

	CreateStorageVirtualMachine(input *createStorageVirtualMachineInput) (*StorageVirtualMachine, error)
	DeleteStorageVirtualMachine(svmID string) error
	DescribeStorageVirtualMachines(
		ids []string,
		maxResults int32,
		nextToken string,
	) ([]*StorageVirtualMachine, string, error)
	UpdateStorageVirtualMachine(input *updateStorageVirtualMachineInput) (*StorageVirtualMachine, error)

	CreateVolume(input *createVolumeInput) (*Volume, error)
	CreateVolumeFromBackup(input *createVolumeFromBackupInput) (*Volume, error)
	DeleteVolume(volumeID string) error
	DescribeVolumes(ids []string, maxResults int32, nextToken string) ([]*Volume, string, error)
	RestoreVolumeFromSnapshot(input *restoreVolumeFromSnapshotInput) (*Volume, error)
	UpdateVolume(input *updateVolumeInput) (*Volume, error)

	CreateAndAttachS3AccessPoint(input *createAndAttachS3AccessPointInput) (*S3AccessPoint, error)
	DetachAndDeleteS3AccessPoint(name, fileSystemID string) error
	DescribeS3AccessPointAttachments(
		names []string,
		maxResults int32,
		nextToken string,
	) ([]*S3AccessPoint, string, error)

	DescribeSharedVpcConfiguration() (*SharedVpcConfiguration, error)
	UpdateSharedVpcConfiguration(input *updateSharedVpcConfigurationInput) (*SharedVpcConfiguration, error)

	ReleaseFileSystemNfsV3Locks(fileSystemID string) error
	StartMisconfiguredStateRecovery(fileSystemID string) error

	TagResource(resourceARN string, tags []Tag) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) ([]Tag, error)

	AccountID() string
	Region() string
	Reset()
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// FileSystem represents an Amazon FSx file system.
// CreationTime is first so its non-pointer prefix reduces GC pointer bytes.
type FileSystem struct {
	CreationTime          epochTime             `json:"CreationTime"`
	LustreConfiguration   *LustreConfiguration  `json:"LustreConfiguration,omitempty"`
	WindowsConfiguration  *WindowsConfiguration `json:"WindowsConfiguration,omitempty"`
	OntapConfiguration    *OntapConfiguration   `json:"OntapConfiguration,omitempty"`
	OpenZFSConfiguration  *OpenZFSConfiguration `json:"OpenZFSConfiguration,omitempty"`
	FileSystemID          string                `json:"FileSystemId"`
	FileSystemType        string                `json:"FileSystemType"`
	FileSystemTypeVersion string                `json:"FileSystemTypeVersion,omitempty"`
	Lifecycle             string                `json:"Lifecycle"`
	ResourceARN           string                `json:"ResourceARN"`
	DNSName               string                `json:"DNSName,omitempty"`
	StorageType           string                `json:"StorageType,omitempty"`
	VpcID                 string                `json:"VpcId,omitempty"`
	OwnersID              string                `json:"OwnerId,omitempty"`
	SubnetIDs             []string              `json:"SubnetIds,omitempty"`
	NetworkInterfaceIDs   []string              `json:"NetworkInterfaceIds,omitempty"`
	Tags                  []Tag                 `json:"Tags,omitempty"`
	StorageCapacityGiB    int32                 `json:"StorageCapacity,omitempty"`
}

// WindowsConfiguration describes the Windows-specific configuration of an
// FSx file system. Real AWS always returns this block for WINDOWS file
// systems, with at least ThroughputCapacity and DeploymentType populated.
// Aliases is not populated here: the source of truth for associated DNS
// aliases is DescribeFileSystemAliases (see AssociateFileSystemAliases /
// DisassociateFileSystemAliases in file_systems.go), which every client
// this emulator targets already calls directly rather than reading this
// convenience mirror.
type WindowsConfiguration struct {
	ActiveDirectoryID             string            `json:"ActiveDirectoryId,omitempty"`
	DailyAutomaticBackupStartTime string            `json:"DailyAutomaticBackupStartTime,omitempty"`
	DeploymentType                string            `json:"DeploymentType,omitempty"`
	PreferredSubnetID             string            `json:"PreferredSubnetId,omitempty"`
	RemoteAdministrationEndpoint  string            `json:"RemoteAdministrationEndpoint,omitempty"`
	WeeklyMaintenanceStartTime    string            `json:"WeeklyMaintenanceStartTime,omitempty"`
	Aliases                       []FileSystemAlias `json:"Aliases,omitempty"`
	AutomaticBackupRetentionDays  int32             `json:"AutomaticBackupRetentionDays,omitempty"`
	ThroughputCapacity            int32             `json:"ThroughputCapacity,omitempty"`
	CopyTagsToBackups             bool              `json:"CopyTagsToBackups,omitempty"`
}

// OntapConfiguration describes the FSx for NetApp ONTAP-specific
// configuration of an FSx file system. Real AWS always returns this block
// for ONTAP file systems, with at least DeploymentType populated.
type OntapConfiguration struct {
	Endpoints                     *FileSystemEndpoints `json:"Endpoints,omitempty"`
	DailyAutomaticBackupStartTime string               `json:"DailyAutomaticBackupStartTime,omitempty"`
	DeploymentType                string               `json:"DeploymentType,omitempty"`
	EndpointIPAddressRange        string               `json:"EndpointIpAddressRange,omitempty"`
	PreferredSubnetID             string               `json:"PreferredSubnetId,omitempty"`
	AutomaticBackupRetentionDays  int32                `json:"AutomaticBackupRetentionDays,omitempty"`
	HAPairs                       int32                `json:"HAPairs,omitempty"`
	ThroughputCapacity            int32                `json:"ThroughputCapacity,omitempty"`
	ThroughputCapacityPerHAPair   int32                `json:"ThroughputCapacityPerHAPair,omitempty"`
}

// OpenZFSConfiguration describes the FSx for OpenZFS-specific configuration
// of an FSx file system. Real AWS always returns this block for OPENZFS
// file systems, with at least DeploymentType and RootVolumeId populated.
type OpenZFSConfiguration struct {
	DailyAutomaticBackupStartTime string `json:"DailyAutomaticBackupStartTime,omitempty"`
	DeploymentType                string `json:"DeploymentType,omitempty"`
	PreferredSubnetID             string `json:"PreferredSubnetId,omitempty"`
	RootVolumeID                  string `json:"RootVolumeId,omitempty"`
	WeeklyMaintenanceStartTime    string `json:"WeeklyMaintenanceStartTime,omitempty"`
	AutomaticBackupRetentionDays  int32  `json:"AutomaticBackupRetentionDays,omitempty"`
	ThroughputCapacity            int32  `json:"ThroughputCapacity,omitempty"`
	CopyTagsToBackups             bool   `json:"CopyTagsToBackups,omitempty"`
	CopyTagsToVolumes             bool   `json:"CopyTagsToVolumes,omitempty"`
}

// FileSystemEndpoints holds the ONTAP management/intercluster endpoints
// used to access data or manage the file system.
type FileSystemEndpoints struct {
	Intercluster *FileSystemEndpoint `json:"Intercluster,omitempty"`
	Management   *FileSystemEndpoint `json:"Management,omitempty"`
}

// FileSystemEndpoint is a single DNS endpoint on an ONTAP file system.
type FileSystemEndpoint struct {
	DNSName string `json:"DNSName,omitempty"`
}

// LustreConfiguration describes the Lustre-specific configuration of an FSx
// file system. AWS always returns this block (with at least DeploymentType,
// MountName, and DataRepositoryConfiguration) for Lustre file systems.
type LustreConfiguration struct {
	DataRepositoryConfiguration *DataRepositoryConfiguration `json:"DataRepositoryConfiguration,omitempty"`
	DeploymentType              string                       `json:"DeploymentType,omitempty"`
	DataCompressionType         string                       `json:"DataCompressionType,omitempty"`
	DriveCacheType              string                       `json:"DriveCacheType,omitempty"`
	MountName                   string                       `json:"MountName,omitempty"`
	WeeklyMaintenanceStartTime  string                       `json:"WeeklyMaintenanceStartTime,omitempty"`
	PerUnitStorageThroughput    int32                        `json:"PerUnitStorageThroughput,omitempty"`
}

// DataRepositoryConfiguration describes the data repository linkage for a
// Lustre file system. AWS returns this block (with a Lifecycle) on every
// Lustre file system, even when no S3 repository is linked.
type DataRepositoryConfiguration struct {
	Lifecycle        string `json:"Lifecycle,omitempty"`
	AutoImportPolicy string `json:"AutoImportPolicy,omitempty"`
	ImportPath       string `json:"ImportPath,omitempty"`
	ExportPath       string `json:"ExportPath,omitempty"`
}

// Backup represents an Amazon FSx backup.
// CreationTime is first so its non-pointer prefix reduces GC pointer bytes.
type Backup struct {
	CreationTime epochTime   `json:"CreationTime"`
	FileSystem   *FileSystem `json:"FileSystem,omitempty"`
	BackupID     string      `json:"BackupId"`
	BackupType   string      `json:"Type"`
	Lifecycle    string      `json:"Lifecycle"`
	ResourceARN  string      `json:"ResourceARN"`
	Tags         []Tag       `json:"Tags,omitempty"`
}

// FileSystemAlias represents an FSx file-system DNS alias.
type FileSystemAlias struct {
	Name      string `json:"Name"`
	Lifecycle string `json:"Lifecycle"`
}

// DataRepositoryAssociation links a file system path to an S3 data repository.
// CreationTime is first so its non-pointer prefix reduces GC pointer bytes.
// CreationTime uses epochTime: the real FSx deserializer requires a JSON
// number of epoch seconds here, not an RFC3339 string.
type DataRepositoryAssociation struct {
	CreationTime       epochTime `json:"CreationTime"`
	AssociationID      string    `json:"AssociationId"`
	FileSystemID       string    `json:"FileSystemId"`
	FileSystemPath     string    `json:"FileSystemPath"`
	DataRepositoryPath string    `json:"DataRepositoryPath"`
	Lifecycle          string    `json:"Lifecycle"`
	ResourceARN        string    `json:"ResourceARN"`
	Tags               []Tag     `json:"Tags,omitempty"`
}

// DataRepositoryTask represents a task that moves data between FSx and a data repository.
// CreationTime is first so its non-pointer prefix reduces GC pointer bytes.
// CreationTime uses epochTime: the real FSx deserializer requires a JSON
// number of epoch seconds here, not an RFC3339 string.
type DataRepositoryTask struct {
	CreationTime epochTime `json:"CreationTime"`
	TaskID       string    `json:"TaskId"`
	FileSystemID string    `json:"FileSystemId"`
	Type         string    `json:"Type"`
	Lifecycle    string    `json:"Lifecycle"`
	ResourceARN  string    `json:"ResourceARN"`
	Paths        []string  `json:"Paths,omitempty"`
	Tags         []Tag     `json:"Tags,omitempty"`
}

// FileCache represents an Amazon FSx file cache.
// CreationTime is first so its non-pointer prefix reduces GC pointer bytes.
// CreationTime uses epochTime: the real FSx deserializer requires a JSON
// number of epoch seconds here, not an RFC3339 string.
type FileCache struct {
	CreationTime       epochTime `json:"CreationTime"`
	FileCacheID        string    `json:"FileCacheId"`
	FileCacheType      string    `json:"FileCacheType"`
	Lifecycle          string    `json:"Lifecycle"`
	ResourceARN        string    `json:"ResourceARN"`
	Tags               []Tag     `json:"Tags,omitempty"`
	StorageCapacityGiB int32     `json:"StorageCapacity,omitempty"`
}

// Snapshot represents an FSx ONTAP or OpenZFS snapshot.
// CreationTime is first so its non-pointer prefix reduces GC pointer bytes.
// CreationTime uses epochTime: the real FSx deserializer requires a JSON
// number of epoch seconds here, not an RFC3339 string.
type Snapshot struct {
	CreationTime epochTime `json:"CreationTime"`
	SnapshotID   string    `json:"SnapshotId"`
	VolumeID     string    `json:"VolumeId"`
	Name         string    `json:"Name"`
	Lifecycle    string    `json:"Lifecycle"`
	ResourceARN  string    `json:"ResourceARN"`
	Tags         []Tag     `json:"Tags,omitempty"`
}

// StorageVirtualMachine represents an FSx ONTAP Storage Virtual Machine.
// CreationTime is first so its non-pointer prefix reduces GC pointer bytes.
// CreationTime uses epochTime: the real FSx deserializer requires a JSON
// number of epoch seconds here, not an RFC3339 string.
type StorageVirtualMachine struct {
	CreationTime            epochTime `json:"CreationTime"`
	StorageVirtualMachineID string    `json:"StorageVirtualMachineId"`
	FileSystemID            string    `json:"FileSystemId"`
	Name                    string    `json:"Name"`
	Lifecycle               string    `json:"Lifecycle"`
	ResourceARN             string    `json:"ResourceARN"`
	Subtype                 string    `json:"Subtype,omitempty"`
	RootVolumeSecurityStyle string    `json:"RootVolumeSecurityStyle,omitempty"`
	Tags                    []Tag     `json:"Tags,omitempty"`
}

// Volume represents an FSx ONTAP or OpenZFS volume.
// CreationTime is first so its non-pointer prefix reduces GC pointer bytes.
// CreationTime uses epochTime: the real FSx deserializer requires a JSON
// number of epoch seconds here, not an RFC3339 string.
type Volume struct {
	CreationTime            epochTime `json:"CreationTime"`
	VolumeID                string    `json:"VolumeId"`
	VolumeType              string    `json:"VolumeType"`
	FileSystemID            string    `json:"FileSystemId"`
	StorageVirtualMachineID string    `json:"StorageVirtualMachineId,omitempty"`
	Name                    string    `json:"Name"`
	Lifecycle               string    `json:"Lifecycle"`
	ResourceARN             string    `json:"ResourceARN"`
	Tags                    []Tag     `json:"Tags,omitempty"`
}

// S3AccessPoint represents an S3 access point attached to an FSx resource.
// CreationTime is first so its non-pointer prefix reduces GC pointer bytes.
// CreationTime uses epochTime: the real FSx deserializer requires a JSON
// number of epoch seconds here, not an RFC3339 string.
type S3AccessPoint struct {
	CreationTime epochTime `json:"CreationTime"`
	Name         string    `json:"Name"`
	FileSystemID string    `json:"FileSystemId"`
	VolumeID     string    `json:"VolumeId,omitempty"`
	Lifecycle    string    `json:"Lifecycle"`
	ResourceARN  string    `json:"ResourceARN"`
	Tags         []Tag     `json:"Tags,omitempty"`
}

// SharedVpcConfiguration holds the shared VPC on-file-system-creation setting.
type SharedVpcConfiguration struct {
	EnableSharedVpcOnFileSystemCreation string `json:"EnableSharedVpcOnFileSystemCreation"`
}

// Tag is a key-value pair attached to an FSx resource.
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

var _ StorageBackend = (*InMemoryBackend)(nil)
