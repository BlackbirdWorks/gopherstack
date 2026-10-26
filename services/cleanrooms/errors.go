package cleanrooms

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrAccessDenied maps to AccessDeniedException (403).
	ErrAccessDenied  = errors.New("AccessDeniedException")
	ErrNotFound      = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrAlreadyExists)
	ErrValidation    = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	// ErrConflict indicates a resource state conflict, e.g. attempting to cancel a
	// protected query/job that has already reached a terminal status. Maps to the
	// same wire error (ConflictException/409) as ErrAlreadyExists but is kept
	// distinct so callers/tests can express "wrong state" independently of
	// "duplicate resource".
	ErrConflict = awserr.New("ConflictException", awserr.ErrConflict)
)
