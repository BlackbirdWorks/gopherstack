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

// GetShardIterator / GetRecords family: shard-iterator and record-read behavior.

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
