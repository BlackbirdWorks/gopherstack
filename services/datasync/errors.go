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

	taskModeBasic = "BASIC"

	smbAuthTypeNTLM = "NTLM"

	internalExceptionType = "InternalException"

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

	// fsxLustreURIScheme is the LocationUri scheme prefix for FSx for Lustre
	// locations. AWS's own published LocationUri regex
	// (`^(efs|nfs|s3|smb|hdfs|fsx[a-z0-9-]+)://...$`, identical across every
	// DescribeLocation*Output doc page) definitively rules out the bare
	// "lustre://" this backend previously emitted -- neither "lustre" nor any
	// "fsx"-prefixed match. "fsxl://" is the pattern-compliant single-letter
	// scheme chosen by analogy with FSx OpenZFS's confirmed "fsxz://" (real
	// AWS CLI example: fsxz://us-west-2.fs-.../fsx/folderA/folder) -- Lustre
	// has no NFS/SMB protocol choice to key off of (unlike FSx Windows/ONTAP,
	// which reuse smb://), so a distinct fsx-prefixed scheme is the only
	// pattern-compliant option. Not independently confirmed against real AWS
	// output; see PARITY.md gaps.
	fsxLustreURIScheme = "fsxl://"
)

var (
	// ErrNotFound is returned when a resource does not exist.
	ErrNotFound = awserr.New(resourceNotFoundType, awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New(conflictExceptionType, awserr.ErrAlreadyExists)
	// ErrInvalidParameter is returned for invalid input.
	ErrInvalidParameter = awserr.New(invalidRequestType, awserr.ErrInvalidParameter)
)
