package directoryservice

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const (
	errEntityNotExistsException     = "EntityDoesNotExistException"
	errEntityAlreadyExistsException = "EntityAlreadyExistsException"
	errClientException              = "ClientException"

	defaultSimpleADLimit    int32 = 10
	defaultMicrosoftADLimit int32 = 20
	defaultSnapshotLimit    int32 = 5
)

var (
	// ErrDirectoryNotFound is returned when a directory does not exist.
	ErrDirectoryNotFound = awserr.New(errEntityNotExistsException, awserr.ErrNotFound)
	// ErrSnapshotNotFound is returned when a snapshot does not exist.
	ErrSnapshotNotFound = awserr.New(errEntityNotExistsException, awserr.ErrNotFound)
	// ErrAliasAlreadyExists is returned when the alias is already taken.
	ErrAliasAlreadyExists = awserr.New(errEntityAlreadyExistsException, awserr.ErrAlreadyExists)
	// ErrInvalidParameter is returned on invalid input.
	ErrInvalidParameter = awserr.New(errClientException, awserr.ErrInvalidParameter)
	// ErrDirectoryLimitExceeded is returned when the directory limit for the region is reached.
	ErrDirectoryLimitExceeded = awserr.New("DirectoryLimitExceededException", awserr.ErrConflict)
	// ErrSnapshotLimitExceeded is returned when the manual snapshot limit for a directory is reached.
	ErrSnapshotLimitExceeded = awserr.New("SnapshotLimitExceededException", awserr.ErrConflict)
	// ErrUnsupportedOperation is returned when an operation is not supported by the directory type.
	ErrUnsupportedOperation = awserr.New("UnsupportedOperationException", awserr.ErrConflict)

	// ErrTrustNotFound is returned when a trust does not exist.
	ErrTrustNotFound = awserr.New(errEntityNotExistsException, awserr.ErrNotFound)
	// ErrCertNotFound is returned when a certificate does not exist.
	ErrCertNotFound = awserr.New(errEntityNotExistsException, awserr.ErrNotFound)
	// ErrConditionalForwarderNotFound is returned when a conditional forwarder does not exist.
	ErrConditionalForwarderNotFound = awserr.New(errEntityNotExistsException, awserr.ErrNotFound)
	// ErrSchemaExtensionNotFound is returned when a schema extension does not exist.
	ErrSchemaExtensionNotFound = awserr.New(errEntityNotExistsException, awserr.ErrNotFound)
	// ErrSharedDirectoryNotFound is returned when a shared directory does not exist.
	ErrSharedDirectoryNotFound = awserr.New(errEntityNotExistsException, awserr.ErrNotFound)
	// ErrAssessmentNotFound is returned when an AD assessment does not exist.
	ErrAssessmentNotFound = awserr.New(errEntityNotExistsException, awserr.ErrNotFound)
)
