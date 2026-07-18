package iotanalytics

import "errors"

// Sentinel errors for IoT Analytics backend operations.
var (
	// ErrChannelNotFound is returned when a channel does not exist.
	ErrChannelNotFound = newNotFoundError("channel not found")
	// ErrDatastoreNotFound is returned when a datastore does not exist.
	ErrDatastoreNotFound = newNotFoundError("datastore not found")
	// ErrDatasetNotFound is returned when a dataset does not exist.
	ErrDatasetNotFound = newNotFoundError("dataset not found")
	// ErrPipelineNotFound is returned when a pipeline does not exist.
	ErrPipelineNotFound = newNotFoundError("pipeline not found")
	// ErrDatasetContentNotFound is returned when a dataset content version does not exist.
	ErrDatasetContentNotFound = newNotFoundError("dataset content not found")
	// ErrLoggingOptionsNotFound is returned when logging options have not been configured.
	ErrLoggingOptionsNotFound = newNotFoundError("logging options not found")
	// ErrReprocessingNotFound is returned when a pipeline reprocessing job does not exist.
	ErrReprocessingNotFound = newNotFoundError("reprocessing not found")
	// ErrResourceNotFound is returned when a tagged resource ARN does not match any known resource.
	ErrResourceNotFound = newNotFoundError("resource not found")
	// ErrAlreadyExists is returned when a resource with the given name already exists.
	ErrAlreadyExists = errors.New("resource already exists")
	// ErrValidation is returned when request input fails validation.
	ErrValidation = errors.New("validation error")
)

// notFoundError represents a resource-not-found error.
type notFoundError struct {
	msg string
}

func (e *notFoundError) Error() string { return e.msg }

// newNotFoundError creates a new notFoundError.
func newNotFoundError(msg string) *notFoundError {
	return &notFoundError{msg: msg}
}

// isNotFound returns true if the error is a notFoundError.
func isNotFound(err error) bool {
	_, ok := err.(*notFoundError) //nolint:errorlint // direct type check for sentinel

	return ok
}
