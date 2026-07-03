package kinesis

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// errTypeValidation is the AWS error-type string for validation failures.
const errTypeValidation = "ValidationException"

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

// ErrShardCountScaling indicates that an UpdateShardCount target fell outside
// the AWS per-call scaling window: within a single call the target shard count
// may not be more than double or less than half of the current open shard
// count. AWS surfaces this as ValidationException. The sentinel's message is the
// error text returned to the client.
var ErrShardCountScaling = errors.New(
	"UpdateShardCount cannot scale by more than double or less than half " +
		"of the current shard count within a single call",
)
