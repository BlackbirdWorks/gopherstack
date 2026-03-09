package kinesis

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// errRateExceeded is the inner sentinel for ErrProvisionedThroughputExceeded.
var errRateExceeded = errors.New("rate exceeded for shard")

// Sentinel errors for Kinesis operations.
var (
	ErrStreamNotFound                = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	ErrStreamAlreadyExists           = awserr.New("ResourceInUseException", awserr.ErrAlreadyExists)
	ErrInvalidArgument               = awserr.New("InvalidArgumentException", awserr.ErrInvalidParameter)
	ErrUnknownAction                 = errors.New("UnknownOperationException")
	ErrShardIteratorExpired          = errors.New("ExpiredIteratorException")
	ErrConsumerNotFound              = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	ErrConsumerAlreadyExists         = awserr.New("ResourceInUseException", awserr.ErrAlreadyExists)
	ErrProvisionedThroughputExceeded = awserr.New(
		"ProvisionedThroughputExceededException",
		errRateExceeded,
	)
)
