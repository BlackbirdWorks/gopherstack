package redshiftdata

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrNotFound is returned when a statement does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrTerminalState is returned when cancelling a statement that is already in a terminal state.
	ErrTerminalState = awserr.New("ValidationException", awserr.ErrConflict)
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	// ErrNoResultSet is returned when fetching results for a statement with no result set.
	ErrNoResultSet = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)
