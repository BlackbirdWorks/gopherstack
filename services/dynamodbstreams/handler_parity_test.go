package dynamodbstreams_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodbstreams"
)

// newParityBackend creates an InMemoryDB with a streams-enabled table and returns
// the db and the stream ARN.
func newParityBackend(t *testing.T, tableName string) (*ddbbackend.InMemoryDB, string) {
	t.Helper()

	db := ddbbackend.NewInMemoryDB()
	ctx := t.Context()

	_, err := db.CreateTable(ctx, &ddbsdk.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
		StreamSpecification: &ddbtypes.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: ddbtypes.StreamViewTypeNewAndOldImages,
		},
	})
	require.NoError(t, err)

	table, ok := db.GetTable(tableName)
	require.True(t, ok)

	return db, table.StreamARN
}

// TestParity_StreamARN_RealTimestampLabel verifies that the stream ARN embeds a real
// ISO 8601 timestamp label rather than a hardcoded placeholder. Real AWS DynamoDB
// Streams ARNs have the form .../stream/2024-01-15T12:00:00.000.
func TestParity_StreamARN_RealTimestampLabel(t *testing.T) {
	t.Parallel()

	_, streamARN := newParityBackend(t, "TimestampTable")

	const sep = "/stream/"
	idx := strings.LastIndex(streamARN, sep)
	require.Positive(t, idx, "stream ARN must contain /stream/ separator")

	label := streamARN[idx+len(sep):]
	assert.NotEqual(t, "latest", label, "stream label must not be the placeholder 'latest'")
	assert.NotEqual(t, "2024-01-01T00:00:00.000", label,
		"stream label must not be the hardcoded legacy placeholder")
	assert.Regexp(t, `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}$`, label,
		"stream label must be in ISO 8601 millisecond format")
}

// TestParity_DescribeStream_StreamLabel verifies that DescribeStream returns a
// StreamLabel that matches the label embedded in the StreamArn.
func TestParity_DescribeStream_StreamLabel(t *testing.T) {
	t.Parallel()

	db, streamARN := newParityBackend(t, "LabelTable")
	h := dynamodbstreams.NewHandler(db)

	resp := doRequest(t, h, "DescribeStream", fmt.Sprintf(`{"StreamArn":%q}`, streamARN))
	require.Equal(t, 200, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))

	desc, ok := out["StreamDescription"].(map[string]any)
	require.True(t, ok)

	streamLabel, ok := desc["StreamLabel"].(string)
	require.True(t, ok, "StreamLabel must be present in DescribeStream response")
	assert.NotEqual(t, "latest", streamLabel,
		"StreamLabel must not be the placeholder 'latest'")

	// StreamLabel must match the last path segment of the ARN after /stream/.
	const sep = "/stream/"
	idx := strings.LastIndex(streamARN, sep)
	require.Positive(t, idx)
	wantLabel := streamARN[idx+len(sep):]
	assert.Equal(t, wantLabel, streamLabel,
		"StreamLabel in DescribeStream must match the label embedded in the stream ARN")
}

// TestParity_DescribeStream_CreationRequestDateTime verifies that DescribeStream
// returns a non-nil CreationRequestDateTime, matching real AWS behavior.
func TestParity_DescribeStream_CreationRequestDateTime(t *testing.T) {
	t.Parallel()

	db, streamARN := newParityBackend(t, "CreationDateTable")
	h := dynamodbstreams.NewHandler(db)

	resp := doRequest(t, h, "DescribeStream", fmt.Sprintf(`{"StreamArn":%q}`, streamARN))
	require.Equal(t, 200, resp.Code)

	var rawOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &rawOut))

	desc, ok := rawOut["StreamDescription"].(map[string]any)
	require.True(t, ok)

	// CreationRequestDateTime must be present and non-nil (real AWS always sets it).
	// The Go SDK marshals *time.Time as an RFC3339 string rather than epoch float64,
	// so we verify presence and non-empty value rather than the specific format.
	creationDateTime, exists := desc["CreationRequestDateTime"]
	assert.True(t, exists, "DescribeStream must include CreationRequestDateTime")
	assert.NotNil(t, creationDateTime, "CreationRequestDateTime must not be nil")
}

// TestParity_ListStreams_StreamLabel verifies that ListStreams returns a StreamLabel
// that matches the ARN label for each stream entry.
func TestParity_ListStreams_StreamLabel(t *testing.T) {
	t.Parallel()

	db, streamARN := newParityBackend(t, "ListLabelTable")
	h := dynamodbstreams.NewHandler(db)

	resp := doRequest(t, h, "ListStreams", `{"TableName":"ListLabelTable"}`)
	require.Equal(t, 200, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))

	streams, ok := out["Streams"].([]any)
	require.True(t, ok)
	require.Len(t, streams, 1)

	entry, ok := streams[0].(map[string]any)
	require.True(t, ok)

	streamLabel, _ := entry["StreamLabel"].(string)
	assert.NotEqual(t, "latest", streamLabel,
		"ListStreams StreamLabel must not be the placeholder 'latest'")

	const sep = "/stream/"
	idx := strings.LastIndex(streamARN, sep)
	require.Positive(t, idx)
	wantLabel := streamARN[idx+len(sep):]
	assert.Equal(t, wantLabel, streamLabel,
		"ListStreams StreamLabel must match the label embedded in the stream ARN")
}

// TestParity_ErrorNamespace_DynamoDBStreams verifies that error responses use the
// dynamodbstreams namespace, not the dynamodb namespace. Real AWS uses
// "com.amazonaws.dynamodbstreams.v20120810#ResourceNotFoundException".
func TestParity_ErrorNamespace_DynamoDBStreams(t *testing.T) {
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

// TestParity_EnableStreamViaUpdateTable_RealLabel verifies that streams enabled via
// UpdateTable (not CreateTable) also get a real timestamp label in their ARN.
func TestParity_EnableStreamViaUpdateTable_RealLabel(t *testing.T) {
	t.Parallel()

	db := ddbbackend.NewInMemoryDB()
	ctx := t.Context()

	_, err := db.CreateTable(ctx, &ddbsdk.CreateTableInput{
		TableName: aws.String("UpdateStreamTable"),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	_, err = db.UpdateTable(ctx, &ddbsdk.UpdateTableInput{
		TableName: aws.String("UpdateStreamTable"),
		StreamSpecification: &ddbtypes.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: ddbtypes.StreamViewTypeNewImage,
		},
	})
	require.NoError(t, err)

	table, ok := db.GetTable("UpdateStreamTable")
	require.True(t, ok)
	require.NotEmpty(t, table.StreamARN)

	const sep = "/stream/"
	idx := strings.LastIndex(table.StreamARN, sep)
	require.Positive(t, idx)
	label := table.StreamARN[idx+len(sep):]

	assert.NotEqual(t, "2024-01-01T00:00:00.000", label,
		"UpdateTable stream ARN must not use the legacy hardcoded timestamp")
	assert.Regexp(t, `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}$`, label)
}

// TestParity_DescribeStream_StreamARNConsistency verifies that StreamArn in the
// DescribeStream response matches the ARN used in the request.
func TestParity_DescribeStream_StreamARNConsistency(t *testing.T) {
	t.Parallel()

	db, streamARN := newParityBackend(t, "ConsistencyTable")
	h := dynamodbstreams.NewHandler(db)

	resp := doRequest(t, h, "DescribeStream", fmt.Sprintf(`{"StreamArn":%q}`, streamARN))
	require.Equal(t, 200, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))

	desc, ok := out["StreamDescription"].(map[string]any)
	require.True(t, ok)

	gotARN, _ := desc["StreamArn"].(string)
	assert.Equal(t, streamARN, gotARN, "StreamArn in response must match the ARN in the request")
}
