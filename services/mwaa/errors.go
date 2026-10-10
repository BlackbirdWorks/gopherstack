package mwaa

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

// Errors used by the backend.
var (
	// ErrEnvironmentNotFound is returned when an environment does not exist.
	ErrEnvironmentNotFound = awserr.New("ResourceNotFoundException: environment not found", awserr.ErrNotFound)
	// ErrEnvironmentAlreadyExists is returned when an environment already exists.
	// The message deliberately does not say "AlreadyExistsException" -- MWAA's
	// API model has no such exception (see handler.go's writeEnvironmentResult),
	// so this sentinel is mapped to a real ValidationException/400 on the wire;
	// embedding the fabricated exception name in the message text would leak it
	// right back into the response body's "message" field.
	ErrEnvironmentAlreadyExists = awserr.New(
		"ValidationException: environment already exists",
		awserr.ErrAlreadyExists,
	)
	// ErrInvalidParameter is returned when an invalid or missing parameter is provided.
	ErrInvalidParameter = awserr.New("ValidationException: invalid parameter", awserr.ErrInvalidParameter)
)

func envNotFound(name string) error {
	return awserr.New("ResourceNotFoundException: Environment "+name+" not found", ErrEnvironmentNotFound)
}

func envAlreadyExists(name string) error {
	return awserr.New("ValidationException: Environment "+name+" already exists", ErrEnvironmentAlreadyExists)
}
