package firehose

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrNotFound is returned when a delivery stream is not found.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a delivery stream already exists.
	ErrAlreadyExists = awserr.New("ResourceInUseException", awserr.ErrAlreadyExists)
	// ErrTransformPayload is a sentinel error indicating the Lambda transform
	// payload could not be built. Use [errors.Is] to check for this condition.
	ErrTransformPayload = errors.New("failed to build Lambda transform payload")
	// ErrRecordTooLarge is returned when a record exceeds the 1,000 KB per-record limit.
	ErrRecordTooLarge = awserr.New("InvalidArgumentException", awserr.ErrInvalidParameter)
	// ErrBatchTooLarge is returned when a PutRecordBatch request exceeds the 500-record limit.
	ErrBatchTooLarge = awserr.New("InvalidArgumentException", awserr.ErrInvalidParameter)
	// ErrValidation is returned for invalid input parameters.
	ErrValidation = awserr.New("InvalidArgumentException", awserr.ErrInvalidParameter)
)
