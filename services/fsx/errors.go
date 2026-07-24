package fsx

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

const (
	errFileSystemNotFound = "FileSystemNotFound"
	errBackupNotFound     = "BackupNotFound"
	// errValidation is BadRequest, not "ValidationError": real FSx has no
	// ValidationError exception type (see types/errors.go in the SDK). Its
	// generic client-error shape is BadRequest; CreateFileSystem-specific
	// gaps use the more specific MissingFileSystemConfiguration below.
	errValidation = "BadRequest"
)

var (
	// ErrFileSystemNotFound is returned when a file system does not exist.
	ErrFileSystemNotFound = awserr.New(errFileSystemNotFound, awserr.ErrNotFound)
	// ErrBackupNotFound is returned when a backup does not exist.
	ErrBackupNotFound = awserr.New(errBackupNotFound, awserr.ErrConflict)
	// ErrValidation is returned on invalid input (wire code: BadRequest).
	ErrValidation = awserr.New(errValidation, awserr.ErrInvalidParameter)
	// ErrMissingFileSystemConfiguration is returned when CreateFileSystem is
	// called for WINDOWS/ONTAP/OPENZFS without the required per-type
	// configuration block (WindowsConfiguration/OntapConfiguration/
	// OpenZFSConfiguration).
	ErrMissingFileSystemConfiguration = awserr.New("MissingFileSystemConfiguration", awserr.ErrInvalidParameter)
	// ErrTagInvalid is returned when a tag key or value fails validation.
	ErrTagInvalid = awserr.New("BadRequest", awserr.ErrInvalidParameter)
	// ErrTagLimitExceeded is returned when the 50-tag-per-resource limit is exceeded.
	ErrTagLimitExceeded = awserr.New("ServiceLimitExceeded", awserr.ErrInvalidParameter)

	// ErrSnapshotNotFound is returned when a snapshot does not exist.
	ErrSnapshotNotFound = awserr.New("SnapshotNotFound", awserr.ErrNotFound)
	// ErrStorageVirtualMachineNotFound is returned when an SVM does not exist.
	ErrStorageVirtualMachineNotFound = awserr.New("StorageVirtualMachineNotFound", awserr.ErrNotFound)
	// ErrVolumeNotFound is returned when a volume does not exist.
	ErrVolumeNotFound = awserr.New("VolumeNotFound", awserr.ErrNotFound)
	// ErrFileCacheNotFound is returned when a file cache does not exist.
	ErrFileCacheNotFound = awserr.New("FileCacheNotFound", awserr.ErrNotFound)
	// ErrDataRepositoryAssociationNotFound is returned when a DRA does not exist.
	ErrDataRepositoryAssociationNotFound = awserr.New("DataRepositoryAssociationNotFound", awserr.ErrNotFound)
	// ErrDataRepositoryTaskNotFound is returned when a DRT does not exist.
	ErrDataRepositoryTaskNotFound = awserr.New("DataRepositoryTaskNotFound", awserr.ErrNotFound)
	// ErrS3AccessPointNotFound is returned when an S3 access point does not exist.
	ErrS3AccessPointNotFound = awserr.New("InvalidRequest", awserr.ErrNotFound)
	// ErrResourceNotFound is returned by the generic Tag/Untag/ListTagsForResource
	// operations when the given ResourceARN does not match any known FSx resource.
	// Real FSx uses the generic ResourceNotFound exception here, distinct from the
	// resource-type-specific *NotFound exceptions used by Describe/Delete ops.
	ErrResourceNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
)
