package dynamodbstreams_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	streamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodbstreams"
)

// ============================================================
// Section 1: Error type propagation
// ============================================================

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

func TestHandler_ErrorPropagation_InvalidShardIterator(t *testing.T) {
	t.Parallel()

	db := ddbbackend.NewInMemoryDB()
	handler := dynamodbstreams.NewHandler(db)

	w := doRequest(t, handler, "GetRecords", `{"ShardIterator":"totallyinvalidtoken"}`)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	var errBody map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &errBody))
	assert.NotEmpty(t, errBody["__type"], "error response must include __type")
}

// ============================================================
// Section 2: Wire format accuracy
// ============================================================

func TestHandler_WireFormat_CRC32Header(t *testing.T) {
	t.Parallel()

	db, _ := newTestBackend(t)
	handler := dynamodbstreams.NewHandler(db)

	w := doRequest(t, handler, "ListStreams", `{}`)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.NotEmpty(t, w.Header().Get("X-Amz-Crc32"),
		"all successful responses must include X-Amz-Crc32 header")
}

func TestHandler_WireFormat_ContentType(t *testing.T) {
	t.Parallel()

	db, _ := newTestBackend(t)
	handler := dynamodbstreams.NewHandler(db)

	w := doRequest(t, handler, "ListStreams", `{}`)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/x-amz-json-1.0",
		w.Header().Get("Content-Type"),
		"DynamoDB Streams responses must use application/x-amz-json-1.0")
}

func TestHandler_WireFormat_ApproximateCreationDateTimeIsFloat(t *testing.T) {
	t.Parallel()

	db, streamARN := newTestBackend(t)
	handler := dynamodbstreams.NewHandler(db)

	// Get a shard iterator for TRIM_HORIZON.
	iterBody := fmt.Sprintf(
		`{"StreamArn":"%s","ShardId":"shardId-00000000000000000001-00000001","ShardIteratorType":"TRIM_HORIZON"}`,
		streamARN,
	)
	iterResp := doRequest(t, handler, "GetShardIterator", iterBody)
	require.Equal(t, http.StatusOK, iterResp.Code)

	var iterOut map[string]any
	require.NoError(t, json.Unmarshal(iterResp.Body.Bytes(), &iterOut))
	shardIter, ok := iterOut["ShardIterator"].(string)
	require.True(t, ok)

	// Get records.
	recResp := doRequest(t, handler, "GetRecords",
		fmt.Sprintf(`{"ShardIterator":"%s"}`, shardIter))
	require.Equal(t, http.StatusOK, recResp.Code)

	// Parse raw JSON to verify ApproximateCreationDateTime is a number (float64),
	// not a string. The DynamoDB Streams JSON 1.0 protocol encodes it as epoch seconds.
	var rawOut map[string]any
	require.NoError(t, json.Unmarshal(recResp.Body.Bytes(), &rawOut))

	records, ok := rawOut["Records"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, records, "must have at least one record")

	rec0 := records[0].(map[string]any)
	dynamodb, ok := rec0["dynamodb"].(map[string]any)
	require.True(t, ok)

	acdt, exists := dynamodb["ApproximateCreationDateTime"]
	require.True(t, exists, "record must include ApproximateCreationDateTime")

	// Must be a JSON number (float64), not a string.
	_, isFloat := acdt.(float64)
	assert.True(t, isFloat,
		"ApproximateCreationDateTime must be a float64 epoch seconds per DynamoDB Streams JSON 1.0 protocol, got %T",
		acdt)
}

func TestHandler_WireFormat_AttributeValueEncoding(t *testing.T) {
	t.Parallel()

	db := ddbbackend.NewInMemoryDB()
	ctx := t.Context()

	const tableName = "WireFormatTable"
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
			StreamViewType: ddbtypes.StreamViewTypeNewImage,
		},
	})
	require.NoError(t, err)

	// Write an item with complex attributes.
	_, err = db.PutItem(ctx, &ddbsdk.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]ddbtypes.AttributeValue{
			"pk":   &ddbtypes.AttributeValueMemberS{Value: "wire-test"},
			"num":  &ddbtypes.AttributeValueMemberN{Value: "42"},
			"flag": &ddbtypes.AttributeValueMemberBOOL{Value: true},
			"null": &ddbtypes.AttributeValueMemberNULL{Value: true},
			"ss":   &ddbtypes.AttributeValueMemberSS{Value: []string{"a", "b"}},
			"ns":   &ddbtypes.AttributeValueMemberNS{Value: []string{"1", "2"}},
			"nested": &ddbtypes.AttributeValueMemberM{Value: map[string]ddbtypes.AttributeValue{
				"inner": &ddbtypes.AttributeValueMemberS{Value: "val"},
			}},
			"list": &ddbtypes.AttributeValueMemberL{Value: []ddbtypes.AttributeValue{
				&ddbtypes.AttributeValueMemberN{Value: "1"},
				&ddbtypes.AttributeValueMemberS{Value: "x"},
			}},
		},
	})
	require.NoError(t, err)

	table, ok := db.GetTable(tableName)
	require.True(t, ok)

	handler := dynamodbstreams.NewHandler(db)

	// Get iterator and records.
	iterBody := fmt.Sprintf(
		`{"StreamArn":"%s","ShardId":"shardId-00000000000000000001-00000001","ShardIteratorType":"TRIM_HORIZON"}`,
		table.StreamARN,
	)
	iterResp := doRequest(t, handler, "GetShardIterator", iterBody)
	require.Equal(t, http.StatusOK, iterResp.Code)

	var iterOut map[string]any
	require.NoError(t, json.Unmarshal(iterResp.Body.Bytes(), &iterOut))
	shardIter, ok := iterOut["ShardIterator"].(string)
	require.True(t, ok)

	recResp := doRequest(t, handler, "GetRecords",
		fmt.Sprintf(`{"ShardIterator":"%s"}`, shardIter))
	require.Equal(t, http.StatusOK, recResp.Code)

	var rawOut map[string]any
	require.NoError(t, json.Unmarshal(recResp.Body.Bytes(), &rawOut))
	records := rawOut["Records"].([]any)
	require.Len(t, records, 1)

	newImage := records[0].(map[string]any)["dynamodb"].(map[string]any)["NewImage"].(map[string]any)

	// Verify each attribute type is encoded correctly.
	assert.Equal(t, map[string]any{"S": "wire-test"}, newImage["pk"])
	assert.Equal(t, map[string]any{"N": "42"}, newImage["num"])
	assert.Equal(t, map[string]any{"BOOL": true}, newImage["flag"])
	assert.NotNil(t, newImage["null"])
	assert.NotNil(t, newImage["nested"])
	assert.NotNil(t, newImage["list"])
}

// ============================================================
// Section 3: Opaque iterator accuracy via handler
// ============================================================

func TestHandler_OpaqueIterator_TokenNotDecodable(t *testing.T) {
	t.Parallel()

	db, streamARN := newTestBackend(t)
	handler := dynamodbstreams.NewHandler(db)

	iterBody := fmt.Sprintf(
		`{"StreamArn":"%s","ShardId":"shardId-00000000000000000001-00000001","ShardIteratorType":"TRIM_HORIZON"}`,
		streamARN,
	)
	w := doRequest(t, handler, "GetShardIterator", iterBody)
	require.Equal(t, http.StatusOK, w.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))

	token, ok := out["ShardIterator"].(string)
	require.True(t, ok)
	require.NotEmpty(t, token)

	// The token must not be in "tableName:seq:ts" format.
	parts := splitN(token, ":", 3)
	isLegacyFormat := len(parts) == 3
	assert.False(t, isLegacyFormat,
		"GetShardIterator must return an opaque token, not plain-text tableName:seq:ts")
}

// splitN splits s by sep, returning at most n parts. Helper to avoid import.
func splitN(s, sep string, n int) []string {
	var parts []string
	for range n - 1 {
		idx := indexOf(s, sep)
		if idx < 0 {
			break
		}
		parts = append(parts, s[:idx])
		s = s[idx+len(sep):]
	}
	parts = append(parts, s)

	return parts
}

func indexOf(s, sub string) int {
	for i := range len(s) - len(sub) + 1 {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}

	return -1
}

// ============================================================
// Section 4: Full lifecycle (enable → put → iterate → read)
// ============================================================

func TestHandler_FullLifecycle_StreamsFlow(t *testing.T) {
	t.Parallel()

	db := ddbbackend.NewInMemoryDB()
	ctx := t.Context()

	const tableName = "LifecycleTable"
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

	// Write 3 items: INSERT, MODIFY, REMOVE.
	_, err = db.PutItem(ctx, &ddbsdk.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "lifecycle-1"},
		},
	})
	require.NoError(t, err)

	_, err = db.PutItem(ctx, &ddbsdk.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "lifecycle-1"},
		},
	})
	require.NoError(t, err)

	_, err = db.DeleteItem(ctx, &ddbsdk.DeleteItemInput{
		TableName: aws.String(tableName),
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "lifecycle-1"},
		},
	})
	require.NoError(t, err)

	table, ok := db.GetTable(tableName)
	require.True(t, ok)

	handler := dynamodbstreams.NewHandler(db)

	// Step 1: ListStreams.
	listResp := doRequest(t, handler, "ListStreams",
		fmt.Sprintf(`{"TableName":"%s"}`, tableName))
	require.Equal(t, http.StatusOK, listResp.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	streams, ok := listOut["Streams"].([]any)
	require.True(t, ok)
	require.Len(t, streams, 1)

	// Step 2: DescribeStream.
	descResp := doRequest(t, handler, "DescribeStream",
		fmt.Sprintf(`{"StreamArn":"%s"}`, table.StreamARN))
	require.Equal(t, http.StatusOK, descResp.Code)
	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descResp.Body.Bytes(), &descOut))
	streamDesc := descOut["StreamDescription"].(map[string]any)
	shards := streamDesc["Shards"].([]any)
	require.NotEmpty(t, shards)

	// Step 3: GetShardIterator.
	shardID := shards[0].(map[string]any)["ShardId"].(string)
	iterResp := doRequest(t, handler, "GetShardIterator",
		fmt.Sprintf(`{"StreamArn":"%s","ShardId":"%s","ShardIteratorType":"TRIM_HORIZON"}`,
			table.StreamARN, shardID))
	require.Equal(t, http.StatusOK, iterResp.Code)
	var iterOut map[string]any
	require.NoError(t, json.Unmarshal(iterResp.Body.Bytes(), &iterOut))
	shardIter, ok := iterOut["ShardIterator"].(string)
	require.True(t, ok)

	// Step 4: GetRecords.
	recResp := doRequest(t, handler, "GetRecords",
		fmt.Sprintf(`{"ShardIterator":"%s"}`, shardIter))
	require.Equal(t, http.StatusOK, recResp.Code)
	var recOut map[string]any
	require.NoError(t, json.Unmarshal(recResp.Body.Bytes(), &recOut))
	records, ok := recOut["Records"].([]any)
	require.True(t, ok)
	require.Len(t, records, 3, "must return all 3 stream events")

	// Verify event names.
	assert.Equal(t, "INSERT", records[0].(map[string]any)["eventName"])
	assert.Equal(t, "MODIFY", records[1].(map[string]any)["eventName"])
	assert.Equal(t, "REMOVE", records[2].(map[string]any)["eventName"])

	// Step 5: Use NextShardIterator to confirm no more records.
	nextIter := recOut["NextShardIterator"].(string)
	emptyResp := doRequest(t, handler, "GetRecords",
		fmt.Sprintf(`{"ShardIterator":"%s"}`, nextIter))
	require.Equal(t, http.StatusOK, emptyResp.Code)
	var emptyOut map[string]any
	require.NoError(t, json.Unmarshal(emptyResp.Body.Bytes(), &emptyOut))
	emptyRecords, ok := emptyOut["Records"].([]any)
	if ok {
		assert.Empty(t, emptyRecords, "NextShardIterator must return no new records")
	}
}

// ============================================================
// Section 5: DescribeStream shard genealogy via handler
// ============================================================

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
	require.Equal(t, http.StatusOK, descResp.Code)

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

// ============================================================
// Section 6: ListStreams pagination via handler
// ============================================================

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
	require.Equal(t, http.StatusOK, w1.Code)
	var out1 map[string]any
	require.NoError(t, json.Unmarshal(w1.Body.Bytes(), &out1))
	streams1 := out1["Streams"].([]any)
	require.Len(t, streams1, 1)
	lastArn1, ok := out1["LastEvaluatedStreamArn"].(string)
	require.True(t, ok, "first page must set LastEvaluatedStreamArn")

	// Second page.
	w2 := doRequest(t, handler, "ListStreams",
		fmt.Sprintf(`{"Limit":1,"ExclusiveStartStreamArn":"%s"}`, lastArn1))
	require.Equal(t, http.StatusOK, w2.Code)
	var out2 map[string]any
	require.NoError(t, json.Unmarshal(w2.Body.Bytes(), &out2))
	streams2 := out2["Streams"].([]any)
	require.Len(t, streams2, 1)

	// Verify no overlap.
	arn1 := streams1[0].(map[string]any)["StreamArn"].(string)
	arn2 := streams2[0].(map[string]any)["StreamArn"].(string)
	assert.NotEqual(t, arn1, arn2, "pages must not overlap")
}

// ============================================================
// Section 7: GetShardIterator invalid ShardId via handler
// ============================================================

func TestHandler_GetShardIterator_UnknownShardId(t *testing.T) {
	t.Parallel()

	db, streamARN := newTestBackend(t)
	handler := dynamodbstreams.NewHandler(db)

	body := fmt.Sprintf(
		`{"StreamArn":"%s","ShardId":"shardId-99999999999999999999-00000001","ShardIteratorType":"TRIM_HORIZON"}`,
		streamARN,
	)
	w := doRequest(t, handler, "GetShardIterator", body)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	var errBody map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &errBody))
	assert.Contains(t, errBody["__type"], "ResourceNotFoundException")
}

// ============================================================
// Section 8: GetRecords validation via handler
// ============================================================

func TestHandler_GetRecords_MissingShardIterator(t *testing.T) {
	t.Parallel()

	db := ddbbackend.NewInMemoryDB()
	handler := dynamodbstreams.NewHandler(db)

	w := doRequest(t, handler, "GetRecords", `{}`)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandler_GetRecords_EventFields(t *testing.T) {
	t.Parallel()

	db, streamARN := newTestBackend(t)
	handler := dynamodbstreams.NewHandler(db)

	iterBody := fmt.Sprintf(
		`{"StreamArn":"%s","ShardId":"shardId-00000000000000000001-00000001","ShardIteratorType":"TRIM_HORIZON"}`,
		streamARN,
	)
	iterResp := doRequest(t, handler, "GetShardIterator", iterBody)
	require.Equal(t, http.StatusOK, iterResp.Code)
	var iterOut map[string]any
	require.NoError(t, json.Unmarshal(iterResp.Body.Bytes(), &iterOut))
	shardIter := iterOut["ShardIterator"].(string)

	recResp := doRequest(t, handler, "GetRecords",
		fmt.Sprintf(`{"ShardIterator":"%s"}`, shardIter))
	require.Equal(t, http.StatusOK, recResp.Code)

	var recOut map[string]any
	require.NoError(t, json.Unmarshal(recResp.Body.Bytes(), &recOut))
	records := recOut["Records"].([]any)
	require.NotEmpty(t, records)

	rec := records[0].(map[string]any)

	// Verify all required AWS record fields are present.
	assert.NotEmpty(t, rec["eventID"], "record must have eventID")
	assert.Equal(t, "INSERT", rec["eventName"], "first record must be INSERT")
	assert.Equal(t, "1.0", rec["eventVersion"], "eventVersion must be 1.0")
	assert.Equal(t, "aws:dynamodb", rec["eventSource"], "eventSource must be aws:dynamodb")
	assert.NotEmpty(t, rec["awsRegion"], "record must have awsRegion")

	dynamodb, ok := rec["dynamodb"].(map[string]any)
	require.True(t, ok, "record must have dynamodb field")
	assert.NotEmpty(t, dynamodb["SequenceNumber"], "dynamodb must have SequenceNumber")
	assert.NotNil(t, dynamodb["ApproximateCreationDateTime"],
		"dynamodb must have ApproximateCreationDateTime")
	assert.NotNil(t, dynamodb["Keys"], "dynamodb must have Keys")
}

// ============================================================
// Section 9: Metadata / handler introspection
// ============================================================

func TestHandler_Metadata_SupportedOperations(t *testing.T) {
	t.Parallel()

	h := dynamodbstreams.NewHandler(ddbbackend.NewInMemoryDB())

	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "DescribeStream")
	assert.Contains(t, ops, "GetRecords")
	assert.Contains(t, ops, "GetShardIterator")
	assert.Contains(t, ops, "ListStreams")
}

func TestHandler_GetShardIterator_AllIteratorTypes_ViaHTTP(t *testing.T) {
	t.Parallel()

	db := ddbbackend.NewInMemoryDB()
	ctx := t.Context()

	const tableName = "IterTypesTable"
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
			StreamViewType: ddbtypes.StreamViewTypeKeysOnly,
		},
	})
	require.NoError(t, err)

	for i := range 5 {
		_, err = db.PutItem(ctx, &ddbsdk.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]ddbtypes.AttributeValue{
				"pk": &ddbtypes.AttributeValueMemberS{Value: fmt.Sprintf("k%d", i)},
			},
		})
		require.NoError(t, err)
	}

	table, ok := db.GetTable(tableName)
	require.True(t, ok)

	handler := dynamodbstreams.NewHandler(db)
	shardID := "shardId-00000000000000000001-00000001"

	tests := []struct {
		name      string
		iterType  streamstypes.ShardIteratorType
		seqNum    string
		wantCount int
	}{
		{
			name:      "TRIM_HORIZON returns all",
			iterType:  streamstypes.ShardIteratorTypeTrimHorizon,
			wantCount: 5,
		},
		{
			name:      "LATEST returns none",
			iterType:  streamstypes.ShardIteratorTypeLatest,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			iterBody := fmt.Sprintf(
				`{"StreamArn":"%s","ShardId":"%s","ShardIteratorType":"%s"}`,
				table.StreamARN, shardID, string(tt.iterType),
			)
			if tt.seqNum != "" {
				iterBody = fmt.Sprintf(
					`{"StreamArn":"%s","ShardId":"%s","ShardIteratorType":"%s","SequenceNumber":"%s"}`,
					table.StreamARN, shardID, string(tt.iterType), tt.seqNum,
				)
			}

			iterResp := doRequest(t, handler, "GetShardIterator", iterBody)
			require.Equal(t, http.StatusOK, iterResp.Code)

			var iterOut map[string]any
			require.NoError(t, json.Unmarshal(iterResp.Body.Bytes(), &iterOut))
			shardIter := iterOut["ShardIterator"].(string)

			recResp := doRequest(t, handler, "GetRecords",
				fmt.Sprintf(`{"ShardIterator":"%s"}`, shardIter))
			require.Equal(t, http.StatusOK, recResp.Code)

			var recOut map[string]any
			require.NoError(t, json.Unmarshal(recResp.Body.Bytes(), &recOut))
			records, _ := recOut["Records"].([]any)
			assert.Len(t, records, tt.wantCount)
		})
	}
}
