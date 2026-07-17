package codeconnections

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ResourceAlreadyExistsException", awserr.ErrConflict)
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	// ErrResourceInUse is returned when a host cannot be deleted because a
	// connection still references it. The real DeleteHost operation documents
	// no dedicated typed error for this case ("Before you delete a host, all
	// connections associated to the host must be deleted."), so the closest
	// real, generic type is used instead of a fabricated name.
	ErrResourceInUse = awserr.New("ConflictException", awserr.ErrConflict)
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
