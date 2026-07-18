package comprehend

import "errors"

var (
	// ErrNotFound is returned when a requested Comprehend resource is absent.
	ErrNotFound = errors.New("ResourceNotFoundException")
	// ErrConflict is returned when a named Comprehend resource already exists.
	ErrConflict = errors.New("ResourceInUseException")
	// ErrValidation is returned for invalid request values.
	ErrValidation = errors.New("InvalidRequestException")
)
