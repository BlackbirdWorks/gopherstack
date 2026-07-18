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

// DescribeStream / ListStreams family: stream metadata, ARN/label accuracy, and
// shard genealogy.

// newStreamsBackend creates an InMemoryDB with a streams-enabled table and returns
// the db and the stream ARN.
func newStreamsBackend(t *testing.T, tableName string) (*ddbbackend.InMemoryDB, string) {
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

func TestHandler_DescribeStream_ShardGenealogy(t *testing.T) {
	t.Parallel()

	db := ddbbackend.NewInMemoryDB()
	ctx := t.Context()

	_, err := db.CreateTable(ctx, &ddbsdk.CreateTableInput{
		TableName: aws.String("GenealogyTable"),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
		StreamSpecification: &ddbtypes.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: ddbtypes.StreamViewTypeKeysOnly,
		},
	})
	require.NoError(t, err)

	// Write 1001 items to trigger a shard split.
	for i := range 1001 {
		_, err = db.PutItem(ctx, &ddbsdk.PutItemInput{
			TableName: aws.String("GenealogyTable"),
			Item: map[string]ddbtypes.AttributeValue{
				"pk": &ddbtypes.AttributeValueMemberS{Value: fmt.Sprintf("key-%d", i)},
			},
		})
		require.NoError(t, err)
	}

	table, ok := db.GetTable("GenealogyTable")
	require.True(t, ok)

	handler := dynamodbstreams.NewHandler(db)

	descResp := doRequest(t, handler, "DescribeStream",
		fmt.Sprintf(`{"StreamArn":"%s"}`, table.StreamARN))
	require.Equal(t, 200, descResp.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descResp.Body.Bytes(), &descOut))
	streamDesc := descOut["StreamDescription"].(map[string]any)
	shards := streamDesc["Shards"].([]any)

	require.GreaterOrEqual(t, len(shards), 2, "must have at least 2 shards after split")

	shard0 := shards[0].(map[string]any)
	shard1 := shards[1].(map[string]any)

	// First shard must be closed (has EndingSequenceNumber).
	snr0 := shard0["SequenceNumberRange"].(map[string]any)
	assert.NotEmpty(t, snr0["EndingSequenceNumber"],
		"closed shard must have EndingSequenceNumber in DescribeStream response")

	// Second shard must have ParentShardId pointing to first shard.
	assert.Equal(t, shard0["ShardId"], shard1["ParentShardId"],
		"child shard must reference parent via ParentShardId")
}

func TestHandler_ListStreams_PaginationViaHTTP(t *testing.T) {
	t.Parallel()

	db := ddbbackend.NewInMemoryDB()
	ctx := t.Context()

	for i := range 3 {
		name := fmt.Sprintf("HttpPagTable%d", i)
		_, err := db.CreateTable(ctx, &ddbsdk.CreateTableInput{
			TableName: aws.String(name),
			KeySchema: []ddbtypes.KeySchemaElement{
				{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash},
			},
			AttributeDefinitions: []ddbtypes.AttributeDefinition{
				{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			},
			BillingMode: ddbtypes.BillingModePayPerRequest,
			StreamSpecification: &ddbtypes.StreamSpecification{
				StreamEnabled:  aws.Bool(true),
				StreamViewType: ddbtypes.StreamViewTypeKeysOnly,
			},
		})
		require.NoError(t, err)
	}

	handler := dynamodbstreams.NewHandler(db)

	// First page: Limit=1.
	w1 := doRequest(t, handler, "ListStreams", `{"Limit":1}`)
	require.Equal(t, 200, w1.Code)
	var out1 map[string]any
	require.NoError(t, json.Unmarshal(w1.Body.Bytes(), &out1))
	streams1 := out1["Streams"].([]any)
	require.Len(t, streams1, 1)
	lastArn1, ok := out1["LastEvaluatedStreamArn"].(string)
	require.True(t, ok, "first page must set LastEvaluatedStreamArn")

	// Second page.
	w2 := doRequest(t, handler, "ListStreams",
		fmt.Sprintf(`{"Limit":1,"ExclusiveStartStreamArn":"%s"}`, lastArn1))
	require.Equal(t, 200, w2.Code)
	var out2 map[string]any
	require.NoError(t, json.Unmarshal(w2.Body.Bytes(), &out2))
	streams2 := out2["Streams"].([]any)
	require.Len(t, streams2, 1)

	// Verify no overlap.
	arn1 := streams1[0].(map[string]any)["StreamArn"].(string)
	arn2 := streams2[0].(map[string]any)["StreamArn"].(string)
	assert.NotEqual(t, arn1, arn2, "pages must not overlap")
}

// TestHandler_StreamARN_RealTimestampLabel verifies that the stream ARN embeds a real
// ISO 8601 timestamp label rather than a hardcoded placeholder. Real AWS DynamoDB
// Streams ARNs have the form .../stream/2024-01-15T12:00:00.000.
func TestHandler_StreamARN_RealTimestampLabel(t *testing.T) {
	t.Parallel()

	_, streamARN := newStreamsBackend(t, "TimestampTable")

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

// TestHandler_DescribeStream_StreamLabel verifies that DescribeStream returns a
// StreamLabel that matches the label embedded in the StreamArn.
func TestHandler_DescribeStream_StreamLabel(t *testing.T) {
	t.Parallel()

	db, streamARN := newStreamsBackend(t, "LabelTable")
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

// TestHandler_DescribeStream_CreationRequestDateTime verifies that DescribeStream
// returns a non-nil CreationRequestDateTime, matching real AWS behavior.
func TestHandler_DescribeStream_CreationRequestDateTime(t *testing.T) {
	t.Parallel()

	db, streamARN := newStreamsBackend(t, "CreationDateTable")
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

// TestHandler_ListStreams_StreamLabel verifies that ListStreams returns a StreamLabel
// that matches the ARN label for each stream entry.
func TestHandler_ListStreams_StreamLabel(t *testing.T) {
	t.Parallel()

	db, streamARN := newStreamsBackend(t, "ListLabelTable")
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

// TestHandler_EnableStreamViaUpdateTable_RealLabel verifies that streams enabled via
// UpdateTable (not CreateTable) also get a real timestamp label in their ARN.
func TestHandler_EnableStreamViaUpdateTable_RealLabel(t *testing.T) {
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

// TestHandler_DescribeStream_StreamARNConsistency verifies that StreamArn in the
// DescribeStream response matches the ARN used in the request.
func TestHandler_DescribeStream_StreamARNConsistency(t *testing.T) {
	t.Parallel()

	db, streamARN := newStreamsBackend(t, "ConsistencyTable")
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
