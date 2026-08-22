package efs

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// PosixUser represents the POSIX identity used for all file system operations
// by NFS clients using the access point.
type PosixUser struct {
	SecondaryGids []int64 `json:"SecondaryGids,omitempty"`
	UID           int64   `json:"Uid"`
	GID           int64   `json:"Gid"`
}

// CreationInfo specifies the POSIX IDs and permissions to apply to the access
// point's RootDirectory when it does not exist.
type CreationInfo struct {
	Permissions string `json:"Permissions"`
	OwnerUID    int64  `json:"OwnerUid"`
	OwnerGID    int64  `json:"OwnerGid"`
}

// RootDirectory specifies the directory on the Amazon EFS file system that the
// access point provides access to.
type RootDirectory struct {
	CreationInfo *CreationInfo `json:"CreationInfo,omitempty"`
	Path         string        `json:"Path,omitempty"`
}

// FileSystem represents an EFS file system.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via TagResource / CreateFileSystem.
type FileSystem struct {
	CreationTime                   time.Time  `json:"creationTime"`
	Tags                           *tags.Tags `json:"tags,omitempty"`
	LastThroughputChange           time.Time  `json:"lastThroughputChange,omitzero"`
	PerformanceMode                string     `json:"performanceMode"`
	FileSystemArn                  string     `json:"fileSystemArn"`
	CreationToken                  string     `json:"creationToken"`
	Name                           string     `json:"name,omitempty"`
	FileSystemID                   string     `json:"fileSystemId"`
	ThroughputMode                 string     `json:"throughputMode"`
	LifeCycleState                 string     `json:"lifeCycleState"`
	AccountID                      string     `json:"accountId"`
	Region                         string     `json:"region"`
	KmsKeyID                       string     `json:"kmsKeyId,omitempty"`
	AvailabilityZoneName           string     `json:"availabilityZoneName,omitempty"`
	AvailabilityZoneID             string     `json:"availabilityZoneId,omitempty"`
	ReplicationOverwriteProtection string     `json:"replicationOverwriteProtection,omitempty"`
	ProvisionedThroughputMib       float64    `json:"provisionedThroughputMib,omitempty"`
	NumberOfMountTargets           int32      `json:"numberOfMountTargets"`
	Encrypted                      bool       `json:"encrypted"`
}

// MountTarget represents an EFS mount target.
type MountTarget struct {
	// region is the AWS region this mount target belongs to. It is the outer
	// half of the composite key ("region|MountTargetID") used by the
	// backend's flat store.Table[MountTarget] (see store_setup.go), which
	// replaces the old map[string]map[string]*MountTarget nesting (outer key
	// = region). Unexported so it never appears in EFS wire responses;
	// persistence.go carries it through a DTO explicitly since json.Marshal
	// never sees unexported fields.
	region               string
	MountTargetID        string   `json:"mountTargetId"`
	MountTargetArn       string   `json:"mountTargetArn"`
	FileSystemID         string   `json:"fileSystemId"`
	SubnetID             string   `json:"subnetId"`
	VpcID                string   `json:"vpcId"`
	AvailabilityZoneName string   `json:"availabilityZoneName"`
	AvailabilityZoneID   string   `json:"availabilityZoneId"`
	NetworkInterfaceID   string   `json:"networkInterfaceId"`
	IPAddress            string   `json:"ipAddress"`
	IPAddressType        string   `json:"ipAddressType,omitempty"`
	IPv6Address          string   `json:"ipv6Address,omitempty"`
	LifeCycleState       string   `json:"lifeCycleState"`
	OwnerID              string   `json:"ownerId"`
	SecurityGroups       []string `json:"securityGroups,omitempty"`
}

// AccessPoint represents an EFS access point.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via TagResource.
type AccessPoint struct {
	// region is the AWS region this access point belongs to. It is the outer
	// half of the composite key ("region|AccessPointID") used by the
	// backend's flat store.Table[AccessPoint] (see store_setup.go), which
	// replaces the old map[string]map[string]*AccessPoint nesting (outer key
	// = region). Unexported so it never appears in EFS wire responses;
	// persistence.go carries it through a DTO explicitly since json.Marshal
	// never sees unexported fields.
	region         string
	AccessPointID  string         `json:"accessPointId"`
	AccessPointArn string         `json:"accessPointArn"`
	FileSystemID   string         `json:"fileSystemId"`
	ClientToken    string         `json:"clientToken,omitempty"`
	Name           string         `json:"name,omitempty"`
	LifeCycleState string         `json:"lifeCycleState"`
	Tags           *tags.Tags     `json:"tags,omitempty"`
	PosixUser      *PosixUser     `json:"posixUser,omitempty"`
	RootDirectory  *RootDirectory `json:"rootDirectory,omitempty"`
	OwnerID        string         `json:"ownerId"`
}

// ReplicationDestination represents a destination in an EFS replication configuration.
//
// LastReplicatedTimestamp is wire-encoded as epoch-seconds (matching the real
// SDK's types.Destination.LastReplicatedTimestamp *time.Time, which serializes
// as a JSON number under the restjson1 protocol), stored here as an int64 unix
// timestamp so a zero value naturally omits via omitempty -- mirroring the
// ReplicationConfiguration.CreationTime convention below.
//
// This struct is reused for both directions: unmarshaling the request body
// (shaped like the real SDK's request-side types.DestinationToCreate, which has
// FileSystemId/Region/AvailabilityZoneName/KmsKeyId/RoleArn) and internal
// storage. FileSystemArn/AvailabilityZoneName/KmsKeyID are request-only /
// internal-bookkeeping fields -- the real response-side types.Destination has
// none of the three (deserializers.go's
// awsRestjson1_deserializeDocumentDestination declares no case for any of
// them). The response wire is built explicitly by destinationToResponse in
// handler_replication.go rather than by marshaling this struct directly, so
// json tags here only need to be correct for request unmarshaling.
type ReplicationDestination struct {
	FileSystemID            string `json:"FileSystemId,omitempty"`
	FileSystemArn           string `json:"FileSystemArn,omitempty"`
	Region                  string `json:"Region,omitempty"`
	AvailabilityZoneName    string `json:"AvailabilityZoneName,omitempty"`
	KmsKeyID                string `json:"KmsKeyId,omitempty"`
	OwnerID                 string `json:"OwnerId,omitempty"`
	Status                  string `json:"Status,omitempty"`
	RoleArn                 string `json:"RoleArn,omitempty"`
	LastReplicatedTimestamp int64  `json:"LastReplicatedTimestamp,omitempty"`
}

// ReplicationConfiguration represents an EFS replication configuration.
type ReplicationConfiguration struct {
	OriginalSourceFileSystemARN string                   `json:"OriginalSourceFileSystemArn"`
	SourceFileSystemARN         string                   `json:"SourceFileSystemArn"`
	SourceFileSystemID          string                   `json:"SourceFileSystemId"`
	SourceFileSystemRegion      string                   `json:"SourceFileSystemRegion"`
	SourceFileSystemOwnerID     string                   `json:"SourceFileSystemOwnerId"`
	Destinations                []ReplicationDestination `json:"Destinations"`
	CreationTime                int64                    `json:"CreationTime"`
}

// AccountPreferences represents EFS account preferences.
type AccountPreferences struct {
	ResourceIDType string `json:"ResourceIdType"`
}

// UpdateFileSystemRequest holds parameters for updating an EFS file system.
type UpdateFileSystemRequest struct {
	ThroughputMode           string  `json:"ThroughputMode,omitempty"`
	ProvisionedThroughputMib float64 `json:"ProvisionedThroughputInMibps,omitempty"`
}

// CreateFileSystemRequest holds parameters for creating an EFS file system.
type CreateFileSystemRequest struct {
	Tags                     map[string]string
	CreationToken            string
	PerformanceMode          string
	ThroughputMode           string
	KmsKeyID                 string
	AvailabilityZoneName     string
	ProvisionedThroughputMib float64
	Encrypted                bool
}

// CreateMountTargetRequest holds parameters for creating an EFS mount target.
type CreateMountTargetRequest struct {
	FileSystemID   string
	SubnetID       string
	IPAddress      string
	IPAddressType  string
	IPv6Address    string
	SecurityGroups []string
}

// CreateAccessPointRequest holds parameters for creating an EFS access point.
type CreateAccessPointRequest struct {
	Tags          map[string]string
	PosixUser     *PosixUser
	RootDirectory *RootDirectory
	FileSystemID  string
	ClientToken   string
}

// LifecyclePolicy represents an EFS lifecycle management policy.
type LifecyclePolicy struct {
	TransitionToIA                  string `json:"TransitionToIA,omitempty"`
	TransitionToPrimaryStorageClass string `json:"TransitionToPrimaryStorageClass,omitempty"`
	TransitionToArchive             string `json:"TransitionToArchive,omitempty"`
}
