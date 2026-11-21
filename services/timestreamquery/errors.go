package timestreamquery

import "errors"

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = errors.New("ResourceNotFoundException")
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = errors.New("ConflictException")
	// ErrValidation is returned when request input fails validation.
	ErrValidation = errors.New("ValidationException")
	// ErrThrottling is returned when a request exceeds a documented rate limit.
	ErrThrottling = errors.New("ThrottlingException")
	// ErrQueryExecution is returned when Timestream cannot run a query (for example on timeout).
	ErrQueryExecution = errors.New("QueryExecutionException")
)
