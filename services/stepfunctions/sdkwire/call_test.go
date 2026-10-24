package sdkwire_test

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

	"github.com/blackbirdworks/gopherstack/services/stepfunctions/sdkwire"
)

const testEndpoint = "http://sfn-sdk-integration.local"

func testTarget(h http.Handler) sdkwire.Target {
	return sdkwire.Target{
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

			svc, ok := sdkwire.Lookup(tt.service)
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

	svc, ok := sdkwire.Lookup("ecs")
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

			svc, ok := sdkwire.Lookup(tt.service)
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

	svc, ok := sdkwire.Lookup("dynamodb")
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
			handler: replying(200, "application/json", "{}", nil), wantErr: sdkwire.ErrUnknownOp,
		},
		{
			name: "parameter_of_wrong_type", op: "GetItem", params: map[string]any{"TableName": map[string]any{"x": 1}},
			handler: replying(200, "application/json", "{}", nil), wantErr: sdkwire.ErrBadValue,
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

			apiErr, isAPI := errors.AsType[smithy.APIError](err)
			require.True(t, isAPI)
			assert.Equal(t, tt.code, apiErr.ErrorCode())
			assert.Equal(t, "gone", apiErr.ErrorMessage())
		})
	}
}

func TestLookup(t *testing.T) {
	t.Parallel()

	_, ok := sdkwire.Lookup("no-such-service")
	assert.False(t, ok)
	assert.NotEmpty(t, sdkwire.Keys())
}

func TestCall_LegacyServices(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want       any
		params     any
		name       string
		service    string
		op         string
		respType   string
		respBody   string
		wantPath   string
		wantQuery  string
		wantInBody string
	}{
		{
			name: "query_xml_response", service: "iam", op: "CreateUser", respType: "text/xml",
			params:     map[string]any{"UserName": "bob", "Tags": []any{map[string]any{"Key": "k", "Value": "v"}}},
			wantInBody: "Tags.member.1.Key=k",
			respBody: `<CreateUserResponse><CreateUserResult><User><UserName>bob</UserName>` +
				`<CreateDate>2020-01-02T03:04:05Z</CreateDate></User></CreateUserResult></CreateUserResponse>`,
			want: map[string]any{"User": map[string]any{"UserName": "bob", "CreateDate": "2020-01-02T03:04:05.000Z"}},
		},
		{
			name: "ec2_flat_lists_and_item_response", service: "ec2", op: "DescribeInstances", respType: "text/xml",
			params: map[string]any{
				"InstanceIds": []any{"i-1"},
				"Filters":     []any{map[string]any{"Name": "n", "Values": []any{"v"}}},
			},
			wantInBody: "Filter.1.Name=n",
			respBody: `<DescribeInstancesResponse><reservationSet><item><reservationId>r-1</reservationId>` +
				`<instancesSet><item><instanceId>i-1</instanceId></item></instancesSet></item></reservationSet>` +
				`</DescribeInstancesResponse>`,
			want: map[string]any{"Reservations": []any{map[string]any{
				"ReservationId": "r-1", "Instances": []any{map[string]any{"InstanceId": "i-1"}},
			}}},
		},
		{
			name:       "awsjson_legacy_wire_names",
			service:    "ssm",
			op:         "GetParameter",
			respType:   "application/x-amz-json-1.1",
			params:     map[string]any{"Name": "n"},
			wantInBody: `"Name":"n"`,
			respBody:   `{"Parameter":{"Name":"n","Value":"v","Version":3}}`,
			want:       map[string]any{"Parameter": map[string]any{"Name": "n", "Value": "v", "Version": float64(3)}},
		},
		{
			name:      "restjson_legacy_label_query_body",
			service:   "scheduler",
			op:        "ListSchedules",
			respType:  "application/json",
			params:    map[string]any{"GroupName": "g", "MaxResults": float64(5)},
			wantPath:  "/schedules",
			wantQuery: "MaxResults=5&ScheduleGroup=g",
			respBody:  `{"Schedules":[{"Name":"s"}]}`,
			want:      map[string]any{"Schedules": []any{map[string]any{"Name": "s"}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, ok := sdkwire.Lookup(tt.service)
			require.True(t, ok)

			var (
				body []byte
				req  *http.Request
			)

			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				req = r
				body, _ = io.ReadAll(r.Body)

				w.Header().Set("Content-Type", tt.respType)
				_, _ = w.Write([]byte(tt.respBody))
			})

			got, err := svc.Call(context.Background(), testTarget(h), tt.op, tt.params)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			assert.Contains(t, string(body), tt.wantInBody)

			if tt.wantPath != "" {
				assert.Equal(t, tt.wantPath, req.URL.Path)
				assert.Equal(t, tt.wantQuery, req.URL.Query().Encode())
			}
		})
	}
}

func TestCall_LegacyIdempotencyToken(t *testing.T) {
	t.Parallel()

	svc, ok := sdkwire.Lookup("ec2")
	require.True(t, ok)

	var body []byte

	_, err := svc.Call(context.Background(), testTarget(replying(200, "text/xml", "<RunInstancesResponse/>", &body)),
		"RunInstances", map[string]any{"ImageId": "ami", "MinCount": float64(1), "MaxCount": float64(1)})
	require.NoError(t, err)
	assert.Contains(t, string(body), "ClientToken=")
}

func TestCall_QueryError(t *testing.T) {
	t.Parallel()

	svc, ok := sdkwire.Lookup("iam")
	require.True(t, ok)

	_, err := svc.Call(context.Background(), testTarget(replying(404, "text/xml",
		`<ErrorResponse><Error><Type>Sender</Type><Code>NoSuchEntity</Code><Message>gone</Message></Error>`+
			`<RequestId>rid</RequestId></ErrorResponse>`, nil)),
		"GetUser", map[string]any{"UserName": "x"})
	require.Error(t, err)

	apiErr, ok := errors.AsType[smithy.APIError](err)
	require.True(t, ok)
	assert.Equal(t, "NoSuchEntity", apiErr.ErrorCode())
	assert.Equal(t, "gone", apiErr.ErrorMessage())
}

func TestCall_RequestEnvelope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		service     string
		op          string
		wantSigning string
		wantAPI     string
		respBody    string
	}{
		{"schema_json", "sqs", "ListQueues", "/us-east-1/sqs/aws4_request", "api/sqs#", "{}"},
		{"schema_rest_without_body", "lambda", "ListFunctions", "/us-east-1/lambda/aws4_request", "api/lambda#", "{}"},
		{
			"legacy_query", "iam", "ListUsers", "/us-east-1/iam/aws4_request", "api/iam#",
			"<ListUsersResponse><ListUsersResult/></ListUsersResponse>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, ok := sdkwire.Lookup(tt.service)
			require.True(t, ok)

			var req *http.Request

			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				req = r
				_, _ = io.ReadAll(r.Body)

				_, _ = w.Write([]byte(tt.respBody))
			})

			tgt := testTarget(h)
			tgt.Account = "111122223333"

			_, err := svc.Call(context.Background(), tgt, tt.op, map[string]any{})
			require.NoError(t, err)

			require.NotNil(t, req.Body)
			assert.Contains(t, req.Header.Get("Authorization"), tt.wantSigning)
			assert.Contains(t, req.Header.Get("User-Agent"), tt.wantAPI)
			assert.Equal(t, "111122223333", req.Header.Get("X-Amz-Account-Id"))
			assert.GreaterOrEqual(t, req.ContentLength, int64(0))
		})
	}
}

func TestCall_VoidOutputIgnoresBody(t *testing.T) {
	t.Parallel()

	svc, ok := sdkwire.Lookup("ec2")
	require.True(t, ok)

	tests := []struct {
		name string
		body string
	}{
		{name: "empty", body: ""},
		{name: "xml_declaration_only", body: `<?xml version="1.0" encoding="UTF-8"?>`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := svc.Call(context.Background(), testTarget(replying(200, "text/xml", tt.body, nil)),
				"ModifyVpcAttribute", map[string]any{"VpcId": "vpc-1"})
			require.NoError(t, err)
			assert.Empty(t, got)
		})
	}
}
