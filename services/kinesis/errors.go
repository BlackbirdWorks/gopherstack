package kinesis

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Sentinel errors for Kinesis operations.
var (
	ErrStreamNotFound       = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	ErrStreamAlreadyExists  = awserr.New("ResourceInUseException", awserr.ErrAlreadyExists)
	ErrInvalidArgument      = awserr.New("InvalidArgumentException", awserr.ErrInvalidParameter)
	ErrUnknownAction        = errors.New("UnknownOperationException")
	ErrShardIteratorExpired = errors.New("ExpiredIteratorException")
)
