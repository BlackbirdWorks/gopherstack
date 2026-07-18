package translate

import "errors"

var (
	// ErrNotFound is returned when a requested resource is absent.
	ErrNotFound = errors.New("ResourceNotFoundException")
	// ErrConflict is returned when a resource already exists.
	ErrConflict = errors.New("ResourceInUseException")
	// ErrValidation is returned for invalid request parameters.
	ErrValidation = errors.New("InvalidRequestException")
)
