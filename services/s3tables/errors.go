package s3tables

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrTableBucketNotFound is returned when a TableBucket does not exist.
	ErrTableBucketNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrTableBucketAlreadyExists is returned when a TableBucket already exists.
	ErrTableBucketAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrNamespaceNotFound is returned when a Namespace does not exist.
	ErrNamespaceNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrNamespaceAlreadyExists is returned when a Namespace already exists.
	ErrNamespaceAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrTableNotFound is returned when a Table does not exist.
	ErrTableNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrTableAlreadyExists is returned when a Table already exists.
	ErrTableAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrTableVersionConflict is returned when an optimistic-lock token is stale.
	ErrTableVersionConflict = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrInvalidTableMetadataLocation is returned when an Iceberg metadata URI is invalid.
	ErrInvalidTableMetadataLocation = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
	// ErrNilAppContext is returned when a nil AppContext is passed to Init.
	ErrNilAppContext = awserr.New("InvalidParameter", awserr.ErrInvalidParameter)
	// ErrInvalidContinuationToken is returned when a list operation's
	// continuation token is malformed.
	ErrInvalidContinuationToken = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
)
