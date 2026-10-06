package bedrockruntime

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Sentinel errors for the bedrockruntime backend.
var (
	// ErrValidation is returned when a request parameter fails validation.
	ErrValidation = errors.New("ValidationException")
	// ErrConflict is returned when a client token is replayed with different parameters.
	ErrConflict = errors.New("ConflictException")
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
)
