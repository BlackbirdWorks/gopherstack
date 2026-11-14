package timestreamquery

import "errors"

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = errors.New("ResourceNotFoundException")
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = errors.New("ConflictException")
	// ErrValidation is returned when request input fails validation.
	ErrValidation = errors.New("ValidationException")
	// ErrQueryExecution is returned when Timestream cannot run a query (for example on timeout).
	ErrQueryExecution = errors.New("QueryExecutionException")
)
