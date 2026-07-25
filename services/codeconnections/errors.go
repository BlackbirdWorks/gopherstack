package codeconnections

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ResourceAlreadyExistsException", awserr.ErrConflict)
	// ErrValidation is returned when input validation fails. The real
	// aws-sdk-go-v2/service/codeconnections@v1.10.22 types/errors.go has no
	// ValidationException type at all (its modeled exception set is
	// AccessDeniedException/ConcurrentModificationException/
	// ConditionalCheckFailedException/ConflictException/
	// InternalServerException/InvalidInputException/LimitExceededException/
	// ResourceAlreadyExistsException/ResourceNotFoundException/
	// ResourceUnavailableException/RetryLatestCommitFailedException/
	// SyncBlockerDoesNotExistException/SyncConfigurationStillExistsException/
	// ThrottlingException/UnsupportedOperationException/
	// UnsupportedProviderTypeException/UpdateOutOfSyncException --
	// InvalidInputException is the real type for malformed/missing-required-
	// field input, confirmed against every mutating op's error list in
	// botocore's codeconnections/2023-12-01/service-2.json (e.g.
	// CreateSyncConfiguration, ListRepositorySyncDefinitions,
	// GetResourceSyncStatus all list InvalidInputException, none list
	// ValidationException).
	ErrValidation = awserr.New("InvalidInputException", awserr.ErrInvalidParameter)
	// ErrResourceInUse is returned when a host cannot be deleted because a
	// connection still references it ("Before you delete a host, all
	// connections associated to the host must be deleted."). A previous
	// audit pass used ConflictException here on the theory that "no
	// dedicated typed error is documented for this case" -- but DeleteHost's
	// real, complete error list (botocore codeconnections/2023-12-01/
	// service-2.json) is exactly [ResourceNotFoundException,
	// ResourceUnavailableException]; ConflictException is not a possible
	// error for this operation at all. ResourceUnavailableException is the
	// correct real type (its doc note also covers the sibling "host cannot
	// be deleted while VPC_CONFIG_INITIALIZING/VPC_CONFIG_DELETING" case,
	// the same "host not currently deletable" family as this one).
	ErrResourceInUse = awserr.New("ResourceUnavailableException", awserr.ErrConflict)
	// ErrSyncConfigStillExists is returned when a repository link cannot be
	// deleted because a sync configuration still references it. The real
	// DeleteRepositoryLink operation documents SyncConfigurationStillExistsException
	// for exactly this case.
	ErrSyncConfigStillExists = awserr.New("SyncConfigurationStillExistsException", awserr.ErrConflict)
	// ErrSyncBlockerNotFound is returned by UpdateSyncBlocker when the blocker ID
	// does not exist (or was created in a different region). The real operation
	// documents SyncBlockerDoesNotExistException for this case; it does NOT
	// resolve unknown IDs gracefully.
	ErrSyncBlockerNotFound = awserr.New("SyncBlockerDoesNotExistException", awserr.ErrNotFound)
)
