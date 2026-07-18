package dynamodbstreams_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodbstreams"
)

// Cross-cutting error-propagation and error-namespace tests that exercise more
// than one operation (DescribeStream and GetShardIterator) per table case.

func TestHandler_ErrorPropagation_ResourceNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		operation string
		body      string
	}{
		{
			name:      "DescribeStream with unknown ARN",
			operation: "DescribeStream",
			body:      `{"StreamArn":"arn:aws:dynamodb:us-east-1:123456789012:table/NoSuch/stream/2024-01-01T00:00:00.000"}`,
		},
		{
			name:      "GetShardIterator with unknown ARN",
			operation: "GetShardIterator",
			body: `{"StreamArn":"arn:aws:dynamodb:us-east-1:123456789012:table/NoSuch/stream/` +
				`2024-01-01T00:00:00.000","ShardId":"shardId-00000000000000000001-00000001",` +
				`"ShardIteratorType":"TRIM_HORIZON"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := ddbbackend.NewInMemoryDB()
			handler := dynamodbstreams.NewHandler(db)

			w := doRequest(t, handler, tt.operation, tt.body)

			assert.Equal(t, http.StatusBadRequest, w.Code)

			var errBody map[string]string
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &errBody))

			// Must use a structured error type (not just the operation name).
			assert.Contains(t, errBody["__type"], "ResourceNotFoundException",
				"ResourceNotFoundException must be propagated in __type field")
			assert.NotEmpty(t, errBody["message"], "error response must include message")
		})
	}
}

func TestHandler_ErrorPropagation_ValidationException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		operation   string
		body        string
		wantErrType string
	}{
		{
			name:        "DescribeStream missing StreamArn",
			operation:   "DescribeStream",
			body:        `{}`,
			wantErrType: "ValidationException",
		},
		{
			name:      "GetShardIterator missing ShardId",
			operation: "GetShardIterator",
			body: `{"StreamArn":"arn:aws:dynamodb:us-east-1:123456789012:table/T/` +
				`stream/2024-01-01T00:00:00.000","ShardIteratorType":"TRIM_HORIZON"}`,
			wantErrType: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := ddbbackend.NewInMemoryDB()
			handler := dynamodbstreams.NewHandler(db)

			w := doRequest(t, handler, tt.operation, tt.body)

			assert.Equal(t, http.StatusBadRequest, w.Code)

			var errBody map[string]string
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &errBody))
			assert.Contains(t, errBody["__type"], tt.wantErrType,
				"error __type must contain %s", tt.wantErrType)
		})
	}
}

// TestHandler_ErrorNamespace_DynamoDBStreams verifies that error responses use the
// dynamodbstreams namespace, not the dynamodb namespace. Real AWS uses
// "com.amazonaws.dynamodbstreams.v20120810#ResourceNotFoundException".
func TestHandler_ErrorNamespace_DynamoDBStreams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    string
		name    string
		op      string
		wantErr string
	}{
		{
			name:    "DescribeStream unknown stream",
			op:      "DescribeStream",
			body:    `{"StreamArn":"arn:aws:dynamodb:us-east-1:123456789012:table/NoSuch/stream/2024-01-01T00:00:00.000"}`,
			wantErr: "ResourceNotFoundException",
		},
		{
			name: "GetShardIterator unknown stream",
			op:   "GetShardIterator",
			body: `{"StreamArn":"arn:aws:dynamodb:us-east-1:123456789012:table/NoSuch/stream/2024-01-01T00:00:00.000",` +
				`"ShardId":"shardId-00000000000000000001-00000001","ShardIteratorType":"TRIM_HORIZON"}`,
			wantErr: "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := ddbbackend.NewInMemoryDB()
			h := dynamodbstreams.NewHandler(db)

			resp := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, 400, resp.Code)

			var errBody map[string]string
			require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &errBody))

			errType := errBody["__type"]
			assert.Contains(t, errType, tt.wantErr)
			assert.Contains(t, errType, "dynamodbstreams",
				"error __type must use dynamodbstreams namespace, got: %s", errType)
			assert.NotContains(t, errType, "com.amazonaws.dynamodb.v20120810",
				"error __type must not use the plain dynamodb namespace for streams errors; got: %s", errType)
		})
	}
}
