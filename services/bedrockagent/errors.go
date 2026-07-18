package bedrockagent

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

// ---------------------------------------------------------------------------
// Sentinel errors
// ---------------------------------------------------------------------------

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource with the given name already exists.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrAlreadyExists)
	// ErrValidation is returned for invalid request parameters.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)
