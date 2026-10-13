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

func TestHandler_RequestErrorShapes(t *testing.T) {
	t.Parallel()

	const arn = "arn:aws:dynamodb:us-east-1:123456789012:table/NoSuch/stream/2024-01-01T00:00:00.000"

	tests := []struct {
		name        string
		operation   string
		body        string
		wantType    string
		wantMessage string
	}{
		{
			name: "malformed_json", operation: "DescribeStream", body: `{bad`,
			wantType: "com.amazon.coral.service#SerializationException",
		},
		{
			name: "list_limit_too_large", operation: "ListStreams", body: `{"Limit":200}`,
			wantType: "com.amazon.coral.service#ValidationException",
			wantMessage: "1 validation error detected: Value '200' at 'limit' failed to satisfy constraint: " +
				"Member must have value less than or equal to 100",
		},
		{
			name: "list_limit_zero", operation: "ListStreams", body: `{"Limit":0}`,
			wantType: "com.amazon.coral.service#ValidationException",
		},
		{
			name: "describe_limit_too_large", operation: "DescribeStream",
			body:     `{"StreamArn":"` + arn + `","Limit":101}`,
			wantType: "com.amazon.coral.service#ValidationException",
		},
		{
			name: "unknown_stream_wording", operation: "DescribeStream", body: `{"StreamArn":"` + arn + `"}`,
			wantType:    "com.amazonaws.dynamodbstreams.v20120810#ResourceNotFoundException",
			wantMessage: "Requested resource not found: Stream: " + arn + " not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler := dynamodbstreams.NewHandler(ddbbackend.NewInMemoryDB())
			w := doRequest(t, handler, tt.operation, tt.body)

			assert.Equal(t, http.StatusBadRequest, w.Code)

			var body map[string]string
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
			assert.Equal(t, tt.wantType, body["__type"])
			assert.NotEmpty(t, body["message"])

			if tt.wantMessage != "" {
				assert.Equal(t, tt.wantMessage, body["message"])
			}
		})
	}
}
