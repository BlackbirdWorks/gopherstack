package rekognition

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const (
	errResourceNotFound  = "ResourceNotFoundException"
	errConflictException = "ConflictException"
	errResourceInUse     = "ResourceInUseException"
	errValidation        = "ValidationException"
)

var (
	// ErrNameInUse is a sentinel for Create* operations whose Name/Arn is
	// already taken by a resource type that the real AWS API reports as
	// ResourceInUseException -- stream processors, projects, and project
	// versions -- as opposed to ResourceAlreadyExistsException (collections,
	// datasets) or ConflictException (users). Verified against
	// aws-sdk-go-v2/service/rekognition's per-operation deserializers.go
	// error switches (each generated Create* op only recognizes a specific
	// exception type; anything else deserializes as an untyped
	// smithy.GenericAPIError, breaking SDK-side `errors.As` typed matching).
	ErrNameInUse = errors.New("resource name already in use")
	// ErrUserConflict is returned when CreateUser is called with a UserId
	// that already exists; AWS reports this as ConflictException (not
	// ResourceAlreadyExistsException).
	ErrUserConflict = errors.New("user already exists")

	// ErrCollectionNotFound is returned when a collection does not exist.
	ErrCollectionNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrCollectionAlreadyExists is returned when a collection already exists.
	ErrCollectionAlreadyExists = awserr.New(errConflictException, awserr.ErrAlreadyExists)
	// ErrStreamProcessorNotFound is returned when a stream processor does not exist.
	ErrStreamProcessorNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrStreamProcessorAlreadyExists is returned when a stream processor already
	// exists. Maps to ResourceInUseException, not ResourceAlreadyExistsException.
	ErrStreamProcessorAlreadyExists = awserr.New(errResourceInUse, ErrNameInUse)
	// ErrFaceNotFound is returned when a face does not exist in a collection.
	ErrFaceNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrValidation is returned on invalid input.
	ErrValidation = awserr.New(errValidation, awserr.ErrInvalidParameter)
	// ErrUnknownOperation is returned when the requested operation is not implemented.
	ErrUnknownOperation = errors.New("unknown operation")

	// ErrProjectNotFound is returned when a project does not exist.
	ErrProjectNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrProjectAlreadyExists is returned when a project name is already in
	// use. Maps to ResourceInUseException (not ResourceAlreadyExistsException
	// -- see ErrNameInUse).
	ErrProjectAlreadyExists = awserr.New(errResourceInUse, ErrNameInUse)
	// ErrProjectVersionNotFound is returned when a project version does not exist.
	ErrProjectVersionNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrProjectVersionAlreadyExists is returned when a project version name
	// is already in use. Maps to ResourceInUseException (see ErrNameInUse).
	ErrProjectVersionAlreadyExists = awserr.New(errResourceInUse, ErrNameInUse)
	// ErrDatasetNotFound is returned when a dataset does not exist.
	ErrDatasetNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrUserNotFound is returned when a user does not exist.
	ErrUserNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrUserAlreadyExists is returned when a UserId is already registered
	// in the collection. Maps to ConflictException (not
	// ResourceAlreadyExistsException -- see ErrUserConflict).
	ErrUserAlreadyExists = awserr.New(errConflictException, ErrUserConflict)
	// ErrLivenessSessionNotFound is returned when a liveness session does not exist.
	ErrLivenessSessionNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrAsyncJobNotFound is returned when an async job does not exist.
	ErrAsyncJobNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrMediaAnalysisJobNotFound is returned when a media analysis job does not exist.
	ErrMediaAnalysisJobNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
)
