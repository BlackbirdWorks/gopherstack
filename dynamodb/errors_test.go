package dynamodb_test

import (
	"Gopherstack/dynamodb"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrorHelpers(t *testing.T) {
	t.Parallel()
	errInternal := dynamodb.NewInternalServerError("internal error")
	assert.Equal(t, "com.amazonaws.dynamodb.v20120810#InternalServerError", errInternal.Type)
	assert.Equal(t, "internal error", errInternal.Message)

	errLimit := dynamodb.NewLimitExceededException("limit exceeded")
	assert.Equal(t, "com.amazonaws.dynamodb.v20120810#LimitExceededException", errLimit.Type)
	assert.Equal(t, "limit exceeded", errLimit.Message)

	errItemLimit := dynamodb.NewItemCollectionSizeLimitExceededException("item limit exceeded")
	assert.Equal(t, "com.amazonaws.dynamodb.v20120810#ItemCollectionSizeLimitExceededException", errItemLimit.Type)
	assert.Equal(t, "item limit exceeded", errItemLimit.Message)
}
