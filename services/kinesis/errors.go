package kinesis

import (
	"errors"
	"fmt"

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

	// ErrKMSNotFound indicates the KMS key referenced by StartStreamEncryption's
	// KeyId does not exist. Only reachable when a KMSKeyValidator is wired via
	// WithKMSValidator (see stream_encryption.go); with no validator wired,
	// KeyId is format-checked only, matching pre-existing behavior for
	// deployments that don't wire cross-service KMS validation.
	ErrKMSNotFound = errors.New("KMSNotFoundException")
	// ErrKMSDisabled indicates the KMS key exists but is disabled or pending
	// deletion/import, matching the real KMSDisabledException/
	// KMSInvalidStateException split (see stream_encryption.go for which
	// applies to which key state).
	ErrKMSDisabled = errors.New("KMSDisabledException")
	// ErrKMSInvalidState indicates the KMS key is in a state (e.g. pending
	// deletion, pending import) that doesn't allow use for encryption.
	ErrKMSInvalidState = errors.New("KMSInvalidStateException")
	// ErrKMSAccessDenied is modeled per the real aws-sdk-go-v2/service/kinesis
	// error set for StartStreamEncryption/StopStreamEncryption/
	// UpdateMaxRecordSize (types.KMSAccessDeniedException), but gopherstack has
	// no IAM/key-policy evaluation engine to ever produce it -- defined for
	// wire-shape completeness and left with no trigger path (see PARITY.md gaps).
	ErrKMSAccessDenied = errors.New("KMSAccessDeniedException")
)

// errInvalidKMSKeyID is the InvalidArgumentException reported when a
// StartStreamEncryption/StopStreamEncryption KeyId does not match any of the
// shapes AWS accepts (UUID, key ARN, alias ARN, or "alias/..." name). Wraps
// ErrInvalidArgument itself (not just the shared awserr.ErrInvalidParameter
// sentinel) so errors.Is(err, ErrInvalidArgument) matches it directly.
var errInvalidKMSKeyID = fmt.Errorf(
	"%w: KeyId must be a key ID, key ARN, alias name, or alias ARN",
	ErrInvalidArgument,
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
