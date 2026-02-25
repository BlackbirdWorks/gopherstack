package kinesis

import "errors"

// Sentinel errors for Kinesis operations.
var (
	ErrStreamNotFound     = errors.New("ResourceNotFoundException")
	ErrStreamAlreadyExists = errors.New("ResourceInUseException")
	ErrInvalidArgument    = errors.New("InvalidArgumentException")
	ErrUnknownAction      = errors.New("UnknownOperationException")
	ErrShardIteratorExpired = errors.New("ExpiredIteratorException")
)
