package dynamodb

import "fmt"

type DynamoDBError struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

func (e *DynamoDBError) Error() string {
	return fmt.Sprintf("%s: %s", e.Type, e.Message)
}

func NewResourceNotFoundException(msg string) *DynamoDBError {
	return &DynamoDBError{
		Type:    "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException",
		Message: msg,
	}
}

func NewConditionalCheckFailedException(msg string) *DynamoDBError {
	return &DynamoDBError{
		Type:    "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException",
		Message: msg,
	}
}

func NewInternalServerError(msg string) *DynamoDBError {
	return &DynamoDBError{
		Type:    "com.amazonaws.dynamodb.v20120810#InternalServerError",
		Message: msg,
	}
}

func NewValidationException(msg string) *DynamoDBError {
	return &DynamoDBError{
		Type:    "com.amazonaws.dynamodb.v20120810#ValidationException",
		Message: msg,
	}
}

func NewItemCollectionSizeLimitExceededException(msg string) *DynamoDBError {
	return &DynamoDBError{
		Type:    "com.amazonaws.dynamodb.v20120810#ItemCollectionSizeLimitExceededException",
		Message: msg,
	}
}
