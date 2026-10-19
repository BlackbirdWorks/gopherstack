package s3files

import "time"

// Resource lifecycle states (types.LifeCycleState).
const (
	statusCreating  = "creating"
	statusAvailable = "available"
	statusDeleting  = "deleting"
	statusUpdating  = "updating"
)

// Import triggers (types.ImportTrigger).
const (
	triggerDirectoryFirstAccess = "ON_DIRECTORY_FIRST_ACCESS"
	triggerFileAccess           = "ON_FILE_ACCESS"
)

// IP address types (types.IpAddressType).
const (
	ipv4Only  = "IPV4_ONLY"
	ipv6Only  = "IPV6_ONLY"
	dualStack = "DUAL_STACK"
)

// Lifecycle is the state shared by every S3 Files resource.
type Lifecycle struct {
	CreatedAt         time.Time  `json:"createdAt"`
	DeleteRequestedAt *time.Time `json:"deleteRequestedAt,omitempty"`
	Status            string     `json:"status"`
	StatusMessage     string     `json:"statusMessage,omitempty"`
}

// ImportDataRule controls how S3 data is imported into a file system.
type ImportDataRule struct {
	Prefix       *string `json:"prefix"`
	SizeLessThan *int64  `json:"sizeLessThan"`
	Trigger      string  `json:"trigger"`
}

// ExpirationDataRule controls when cached data expires from a file system.
type ExpirationDataRule struct {
	DaysAfterLastAccess *int32 `json:"daysAfterLastAccess"`
}

// SyncConfiguration is a file system's synchronization configuration.
type SyncConfiguration struct {
	ImportDataRules     []ImportDataRule     `json:"importDataRules"`
	ExpirationDataRules []ExpirationDataRule `json:"expirationDataRules"`
	Version             int32                `json:"version"`
}

// FileSystem is an S3 file system scoped to a bucket or prefix.
type FileSystem struct {
	Tags        map[string]string  `json:"tags,omitempty"`
	Sync        *SyncConfiguration `json:"sync,omitempty"`
	ID          string             `json:"id"`
	ARN         string             `json:"arn"`
	Bucket      string             `json:"bucket"`
	Prefix      string             `json:"prefix,omitempty"`
	RoleARN     string             `json:"roleArn"`
	KMSKeyID    string             `json:"kmsKeyId,omitempty"`
	ClientToken string             `json:"clientToken,omitempty"`
	OwnerID     string             `json:"ownerId"`
	Region      string             `json:"region"`
	Policy      string             `json:"policy,omitempty"`
	Lifecycle
}

// MountTarget is a network endpoint for mounting a file system.
type MountTarget struct {
	Lifecycle
	IPv6Address        string   `json:"ipv6Address,omitempty"`
	SubnetID           string   `json:"subnetId"`
	VPCID              string   `json:"vpcId,omitempty"`
	AvailabilityZoneID string   `json:"availabilityZoneId,omitempty"`
	IPv4Address        string   `json:"ipv4Address,omitempty"`
	ID                 string   `json:"id"`
	IPAddressType      string   `json:"ipAddressType"`
	NetworkInterfaceID string   `json:"networkInterfaceId"`
	OwnerID            string   `json:"ownerId"`
	Region             string   `json:"region"`
	FileSystemID       string   `json:"fileSystemId"`
	SecurityGroups     []string `json:"securityGroups,omitempty"`
}

// PosixUser is the POSIX identity enforced through an access point.
type PosixUser struct {
	UID           *int64  `json:"uid"`
	GID           *int64  `json:"gid"`
	SecondaryGIDs []int64 `json:"secondaryGids,omitempty"`
}

// CreationPermissions are applied to a newly created root directory.
type CreationPermissions struct {
	OwnerUID    *int64  `json:"ownerUid"`
	OwnerGID    *int64  `json:"ownerGid"`
	Permissions *string `json:"permissions"`
}

// RootDirectory is an access point's root directory.
type RootDirectory struct {
	CreationPermissions *CreationPermissions `json:"creationPermissions,omitempty"`
	Path                *string              `json:"path,omitempty"`
}

// AccessPoint is an application-specific entry point into a file system.
type AccessPoint struct {
	Tags          map[string]string `json:"tags,omitempty"`
	PosixUser     *PosixUser        `json:"posixUser,omitempty"`
	RootDirectory *RootDirectory    `json:"rootDirectory,omitempty"`
	ID            string            `json:"id"`
	ARN           string            `json:"arn"`
	FileSystemID  string            `json:"fileSystemId"`
	ClientToken   string            `json:"clientToken,omitempty"`
	OwnerID       string            `json:"ownerId"`
	Region        string            `json:"region"`
	Lifecycle
}
