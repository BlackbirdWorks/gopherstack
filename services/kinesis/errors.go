package kinesis

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// errRateExceeded is the inner sentinel for ErrProvisionedThroughputExceeded.
var errRateExceeded = errors.New("rate exceeded for shard")

// ErrValidation is the sentinel error for Kinesis input validation failures.
var ErrValidation = errors.New("kinesis: validation error")

// Sentinel errors for Kinesis operations.
var (
	ErrStreamNotFound                = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	ErrStreamAlreadyExists           = awserr.New("ResourceInUseException", awserr.ErrAlreadyExists)
	ErrInvalidArgument               = awserr.New("InvalidArgumentException", awserr.ErrInvalidParameter)
	ErrUnknownAction                 = errors.New("UnknownOperationException")
	ErrShardIteratorExpired          = errors.New("ExpiredIteratorException")
	ErrConsumerNotFound              = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	ErrConsumerAlreadyExists         = awserr.New("ResourceInUseException", awserr.ErrAlreadyExists)
	ErrResourcePolicyNotFound        = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	ErrProvisionedThroughputExceeded = awserr.New(
		"ProvisionedThroughputExceededException",
		errRateExceeded,
	)
	ErrTagLimitExceeded = awserr.New("LimitExceededException", awserr.ErrInvalidParameter)
	ErrLimitExceeded    = awserr.New("LimitExceededException", awserr.ErrInvalidParameter)
)
