package bedrockruntime

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Sentinel errors for the bedrockruntime backend.
var (
	// ErrValidation is returned when a request parameter fails validation.
	ErrValidation = errors.New("ValidationException")
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
)
