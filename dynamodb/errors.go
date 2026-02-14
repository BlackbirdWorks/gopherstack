package dynamodb

import (
	"errors"
	"fmt"
)

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
}

func NewResourceNotFoundException(msg string) *Error {
	return &Error{
		Type:    "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException",
		Message: msg,
	}
}

func NewConditionalCheckFailedException(msg string) *Error {
	return &Error{
		Type:    "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException",
		Message: msg,
	}
}

func NewInternalServerError(msg string) *Error {
	return &Error{
		Type:    "com.amazonaws.dynamodb.v20120810#InternalServerError",
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

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Type, e.Message)
}
