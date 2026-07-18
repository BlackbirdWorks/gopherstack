package datasync

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const (
	invalidRequestType    = "InvalidRequestException"
	resourceNotFoundType  = "ResourceNotFoundException"
	conflictExceptionType = "ResourceExistsException"

	agentStatusOnline   = "ONLINE"
	taskStatusAvailable = "AVAILABLE"
	taskStatusRunning   = "RUNNING"

	executionStatusLaunching = "LAUNCHING"
	executionStatusSuccess   = "SUCCESS"
	executionStatusError     = "ERROR"

	defaultMaxResults = 100

	arnSplitParts = 2

	locationTypeAzureBlob     = "AZURE_BLOB"
	locationTypeEFS           = "EFS"
	locationTypeFsxLustre     = "FSX_LUSTRE"
	locationTypeFsxOntap      = "FSX_ONTAP"
	locationTypeFsxOpenZfs    = "FSX_OPENZFS"
	locationTypeFsxWindows    = "FSX_WINDOWS"
	locationTypeHDFS          = "HDFS"
	locationTypeNFS           = "NFS"
	locationTypeObjectStorage = "OBJECT_STORAGE"
	locationTypeSMB           = "SMB"
)

var (
	// ErrNotFound is returned when a resource does not exist.
	ErrNotFound = awserr.New(resourceNotFoundType, awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New(conflictExceptionType, awserr.ErrAlreadyExists)
	// ErrInvalidParameter is returned for invalid input.
	ErrInvalidParameter = awserr.New(invalidRequestType, awserr.ErrInvalidParameter)
)
