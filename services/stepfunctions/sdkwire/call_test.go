package sdkwire

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testTarget(h http.Handler) Target {
	return Target{
		Handler: h, Endpoint: testEndpoint, Region: "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
	}
}

func replying(status int, contentType, body string, seen *[]byte) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if seen != nil {
			*seen, _ = io.ReadAll(r.Body)
		}

		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	})
}

func TestCall_Request(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params   any
		wantJSON map[string]any
		name     string
		service  string
		op       string
	}{
		{
			name: "union_members", service: "dynamodb", op: "PutItem",
			params: map[string]any{
				"tableName": "t",
				"Item": map[string]any{
					"id": map[string]any{"S": "a"}, "n": map[string]any{"N": "1"},
					"l": map[string]any{"L": []any{map[string]any{"BOOL": true}}},
				},
			},
			wantJSON: map[string]any{
				"TableName": "t",
				"Item": map[string]any{
					"id": map[string]any{"S": "a"}, "n": map[string]any{"N": "1"},
					"l": map[string]any{"L": []any{map[string]any{"BOOL": true}}},
				},
			},
		},
		{
			name: "scalar_coercion", service: "sqs", op: "SendMessage",
			params:   map[string]any{"QueueUrl": "q", "MessageBody": float64(5), "DelaySeconds": "3"},
			wantJSON: map[string]any{"QueueUrl": "q", "MessageBody": "5", "DelaySeconds": float64(3)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, ok := Lookup(tt.service)
			require.True(t, ok)

			var seen []byte

			_, err := svc.Call(context.Background(), testTarget(replying(200, "application/json", "{}", &seen)),
				tt.op, tt.params)
			require.NoError(t, err)

			var got map[string]any
			require.NoError(t, json.Unmarshal(seen, &got))

			for k, v := range tt.wantJSON {
				assert.Equal(t, v, got[k], k)
			}
		})
	}
}

func TestCall_IdempotencyToken(t *testing.T) {
	t.Parallel()

	svc, ok := Lookup("ecs")
	require.True(t, ok)

	var seen []byte

	_, err := svc.Call(context.Background(), testTarget(replying(200, "application/x-amz-json-1.1", "{}", &seen)),
		"RunTask", map[string]any{"TaskDefinition": "td"})
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(seen, &got))
	assert.NotEmpty(t, got["clientToken"])
}

func TestCall_Response(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want    any
		name    string
		service string
		op      string
		body    string
	}{
		{
			name: "union_and_blob", service: "dynamodb", op: "GetItem",
			body: `{"Item":{"id":{"S":"a"},"b":{"B":"YmxvYg=="},"m":{"M":{"x":{"N":"1"}}}}}`,
			want: map[string]any{"Item": map[string]any{
				"id": map[string]any{"S": "a"}, "b": map[string]any{"B": "YmxvYg=="},
				"m": map[string]any{"M": map[string]any{"x": map[string]any{"N": "1"}}},
			}},
		},
		{
			name: "epoch_timestamp_becomes_iso", service: "sfn", op: "DescribeExecution",
			body: `{"executionArn":"a","startDate":1577934245.5,"status":"RUNNING"}`,
			want: map[string]any{"ExecutionArn": "a", "StartDate": "2020-01-02T03:04:05.500Z", "Status": "RUNNING"},
		},
		{
			name: "null_and_unknown_members_dropped", service: "sqs", op: "GetQueueUrl",
			body: `{"QueueUrl":null,"Unmodeled":1}`,
			want: map[string]any{},
		},
		{
			name: "empty_body", service: "sqs", op: "PurgeQueue",
			body: ``,
			want: map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, ok := Lookup(tt.service)
			require.True(t, ok)

			got, err := svc.Call(context.Background(),
				testTarget(replying(200, "application/x-amz-json-1.0", tt.body, nil)), tt.op, map[string]any{})
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCall_Errors(t *testing.T) {
	t.Parallel()

	svc, ok := Lookup("dynamodb")
	require.True(t, ok)

	tests := []struct {
		handler http.Handler
		params  any
		wantErr error
		name    string
		op      string
		code    string
	}{
		{
			name: "modeled_service_error", op: "GetItem", params: map[string]any{"TableName": "t"},
			handler: replying(400, "application/x-amz-json-1.0",
				`{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"gone"}`, nil),
			code: "ResourceNotFoundException",
		},
		{
			name: "unknown_operation", op: "NoSuchOp", params: map[string]any{},
			handler: replying(200, "application/json", "{}", nil), wantErr: ErrUnknownOp,
		},
		{
			name: "parameter_of_wrong_type", op: "GetItem", params: map[string]any{"TableName": map[string]any{"x": 1}},
			handler: replying(200, "application/json", "{}", nil), wantErr: ErrBadValue,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := svc.Call(context.Background(), testTarget(tt.handler), tt.op, tt.params)
			require.Error(t, err)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			apiErr, ok := errors.AsType[smithy.APIError](err)
			require.True(t, ok)
			assert.Equal(t, tt.code, apiErr.ErrorCode())
			assert.Equal(t, "gone", apiErr.ErrorMessage())
		})
	}
}

func TestLookup(t *testing.T) {
	t.Parallel()

	_, ok := Lookup("no-such-service")
	assert.False(t, ok)
	assert.NotEmpty(t, Keys())
}
