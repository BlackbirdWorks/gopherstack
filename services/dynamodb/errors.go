package dynamodb

import (
	"errors"
	"fmt"
)

const (
	errInternalServerErrorType       = "com.amazonaws.dynamodb.v20120810#InternalServerError"
	errResourceNotFoundExceptionType = "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException"
)

// ErrValidation is a sentinel used for validation-related errors so callers can
// use [errors.Is] without parsing error message strings.
var ErrValidation = errors.New("validation error")

// Sentinel errors for path operations.
var (
	ErrUnclosedBracket = errors.New("unclosed bracket in path")
	ErrInvalidIndex    = errors.New("invalid list index")
	ErrNonMapAccess    = errors.New("cannot access key on non-map")
	ErrNonMapItem      = errors.New("item is not a Map")
	ErrNonListAccess   = errors.New("cannot access index on non-list")
	ErrIndexOutOfRange = errors.New("index out of bounds")
)

type Error struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
	// Item carries the existing item on a ConditionalCheckFailedException when the
	// request set ReturnValuesOnConditionCheckFailure=ALL_OLD. AWS returns it so
	// optimistic-locking clients can inspect the current item without a re-read.
	Item                any                  `json:"Item,omitempty"`
	CancellationReasons []CancellationReason `json:"CancellationReasons,omitempty"`
}

type CancellationReason struct {
	Item    any    `json:"Item,omitempty"`
	Code    string `json:"Code"`
	Message string `json:"Message,omitempty"`
}

func NewResourceNotFoundException(msg string) *Error {
	return &Error{
		Type:    errResourceNotFoundExceptionType,
		Message: msg,
	}
}

func NewConditionalCheckFailedException(msg string) *Error {
	return &Error{
		Type:    "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException",
		Message: msg,
	}
}

// NewConditionalCheckFailedExceptionWithItem returns a ConditionalCheckFailedException
// that also carries the existing item (already in DynamoDB wire/SDK attribute form).
// Pass a nil item to omit it.
func NewConditionalCheckFailedExceptionWithItem(msg string, item any) *Error {
	return &Error{
		Type:    "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException",
		Message: msg,
		Item:    item,
	}
}

func NewInternalServerError(msg string) *Error {
	return &Error{
		Type:    errInternalServerErrorType,
		Message: msg,
	}
}

func NewValidationException(msg string) *Error {
	return &Error{
		Type:    "com.amazonaws.dynamodb.v20120810#ValidationException",
		Message: msg,
	}
}

func NewItemCollectionSizeLimitExceededException(msg string) *Error {
	return &Error{
		Type:    "com.amazonaws.dynamodb.v20120810#ItemCollectionSizeLimitExceededException",
		Message: msg,
	}
}

func NewLimitExceededException(msg string) *Error {
	return &Error{
		Type:    "com.amazonaws.dynamodb.v20120810#LimitExceededException",
		Message: msg,
	}
}

func NewResourceInUseException(msg string) *Error {
	return &Error{
		Type:    "com.amazonaws.dynamodb.v20120810#ResourceInUseException",
		Message: msg,
	}
}

func NewProvisionedThroughputExceededException(msg string) *Error {
	return &Error{
		Type:    "com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException",
		Message: msg,
	}
}

func NewThrottlingException(msg string) *Error {
	return &Error{
		Type:    "com.amazonaws.dynamodb.v20120810#ThrottlingException",
		Message: msg,
	}
}

func NewRequestLimitExceeded(msg string) *Error {
	return &Error{
		Type:    "com.amazonaws.dynamodb.v20120810#RequestLimitExceeded",
		Message: msg,
	}
}

func NewTransactionConflictException(msg string) *Error {
	return &Error{
		Type:    "com.amazonaws.dynamodb.v20120810#TransactionConflictException",
		Message: msg,
	}
}

func NewReplicatedWriteConflictException(msg string) *Error {
	return &Error{
		Type:    "com.amazonaws.dynamodb.v20120810#ReplicatedWriteConflictException",
		Message: msg,
	}
}

func NewPolicyNotFoundException(msg string) *Error {
	return &Error{
		Type:    "com.amazonaws.dynamodb.v20120810#PolicyNotFoundException",
		Message: msg,
	}
}

func NewTransactionCanceledException(msg string, reasons []CancellationReason) *Error {
	return &Error{
		Type:                "com.amazonaws.dynamodb.v20120810#TransactionCanceledException",
		Message:             msg,
		CancellationReasons: reasons,
	}
}

func NewTransactionInProgressException(msg string) *Error {
	return &Error{
		Type:    "com.amazonaws.dynamodb.v20120810#TransactionInProgressException",
		Message: msg,
	}
}

func NewExpiredIteratorException(msg string) *Error {
	return &Error{
		Type:    "com.amazonaws.dynamodb.v20120810#ExpiredIteratorException",
		Message: msg,
	}
}

func NewTrimmedDataAccessException(msg string) *Error {
	return &Error{
		Type:    "com.amazonaws.dynamodb.v20120810#TrimmedDataAccessException",
		Message: msg,
	}
}

func NewShardIteratorCreationException(msg string) *Error {
	return &Error{
		Type:    errResourceNotFoundExceptionType,
		Message: msg,
	}
}

// NewBackupInUseException returns an error indicating that a backup with the same name
// already exists for the table, or the backup ARN is already in use.
func NewBackupInUseException(msg string) *Error {
	return &Error{
		Type:    "com.amazonaws.dynamodb.v20120810#BackupInUseException",
		Message: msg,
	}
}

// NewImportNotFoundException indicates the requested import ARN does not exist.
func NewImportNotFoundException(msg string) *Error {
	return &Error{
		Type:    "com.amazonaws.dynamodb.v20120810#ImportNotFoundException",
		Message: msg,
	}
}

// NewExportNotFoundException indicates the requested export ARN does not exist.
func NewExportNotFoundException(msg string) *Error {
	return &Error{
		Type:    "com.amazonaws.dynamodb.v20120810#ExportNotFoundException",
		Message: msg,
	}
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Type, e.Message)
}
