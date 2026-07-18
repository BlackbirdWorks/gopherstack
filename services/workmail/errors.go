package workmail

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("EntityNotFoundException", awserr.ErrNotFound)
	// ErrConflict is returned when a resource already exists.
	ErrConflict = awserr.New("EntityAlreadyExistsException", awserr.ErrAlreadyExists)
	// ErrValidation is returned for invalid request parameters.
	ErrValidation = awserr.New("InvalidParameterException", awserr.ErrInvalidParameter)
	// ErrLimitExceeded is returned when resource limits are hit.
	ErrLimitExceeded = awserr.New("LimitExceededException", awserr.ErrConflict)
	// ErrMailDomainState is returned for domain state issues.
	ErrMailDomainState = awserr.New("MailDomainStateException", awserr.ErrConflict)
	// ErrEntityState is returned when an operation violates entity state constraints.
	ErrEntityState = awserr.New("EntityStateException", awserr.ErrConflict)
)
