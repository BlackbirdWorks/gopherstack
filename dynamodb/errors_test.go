package dynamodb_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/dynamodb"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

func TestErrorHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errFunc func(string) *dynamodb.Error
		want    *dynamodb.Error
		name    string
		msg     string
	}{
		{
			name:    "InternalServerError",
			errFunc: dynamodb.NewInternalServerError,
			msg:     "internal error",
			want: &dynamodb.Error{
				Type:    "com.amazonaws.dynamodb.v20120810#InternalServerError",
				Message: "internal error",
			},
		},
		{
			name:    "LimitExceededException",
			errFunc: dynamodb.NewLimitExceededException,
			msg:     "limit exceeded",
			want: &dynamodb.Error{
				Type:    "com.amazonaws.dynamodb.v20120810#LimitExceededException",
				Message: "limit exceeded",
			},
		},
		{
			name:    "ItemCollectionSizeLimitExceeded",
			errFunc: dynamodb.NewItemCollectionSizeLimitExceededException,
			msg:     "item limit exceeded",
			want: &dynamodb.Error{
				Type:    "com.amazonaws.dynamodb.v20120810#ItemCollectionSizeLimitExceededException",
				Message: "item limit exceeded",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.errFunc(tt.msg)
			assert.Empty(t, cmp.Diff(tt.want, got), "Error mismatch")
		})
	}
}
