package mwaa

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

// Errors used by the backend.
var (
	// ErrEnvironmentNotFound is returned when an environment does not exist.
	ErrEnvironmentNotFound = awserr.New("ResourceNotFoundException: environment not found", awserr.ErrNotFound)
	// ErrEnvironmentAlreadyExists is returned when an environment already exists.
	ErrEnvironmentAlreadyExists = awserr.New(
		"AlreadyExistsException: environment already exists",
		awserr.ErrAlreadyExists,
	)
	// ErrInvalidParameter is returned when an invalid or missing parameter is provided.
	ErrInvalidParameter = awserr.New("ValidationException: invalid parameter", awserr.ErrInvalidParameter)
)
