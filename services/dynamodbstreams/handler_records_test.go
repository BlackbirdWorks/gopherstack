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

// TestHandler_GetShardIterator_AtAfterSequenceNumber_ViaHTTP verifies the
// AT_SEQUENCE_NUMBER and AFTER_SEQUENCE_NUMBER iterator types end-to-end over
// HTTP, including the exact boundary semantics (AT includes the given
// sequence number, AFTER excludes it).
func TestHandler_GetShardIterator_AtAfterSequenceNumber_ViaHTTP(t *testing.T) {
	t.Parallel()

	db := ddbbackend.NewInMemoryDB()
	ctx := t.Context()

	const tableName = "AtAfterSeqTable"
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

	for i := range 3 {
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

	// Read all 3 records via TRIM_HORIZON to learn the middle record's sequence number.
	trimIterBody := fmt.Sprintf(
		`{"StreamArn":"%s","ShardId":"%s","ShardIteratorType":"TRIM_HORIZON"}`,
		table.StreamARN, shardID,
	)
	trimIterResp := doRequest(t, handler, "GetShardIterator", trimIterBody)
	require.Equal(t, http.StatusOK, trimIterResp.Code)
	var trimIterOut map[string]any
	require.NoError(t, json.Unmarshal(trimIterResp.Body.Bytes(), &trimIterOut))
	trimIter := trimIterOut["ShardIterator"].(string)

	allRecResp := doRequest(t, handler, "GetRecords", fmt.Sprintf(`{"ShardIterator":"%s"}`, trimIter))
	require.Equal(t, http.StatusOK, allRecResp.Code)
	var allRecOut map[string]any
	require.NoError(t, json.Unmarshal(allRecResp.Body.Bytes(), &allRecOut))
	allRecords := allRecOut["Records"].([]any)
	require.Len(t, allRecords, 3)
	midSeq := allRecords[1].(map[string]any)["dynamodb"].(map[string]any)["SequenceNumber"].(string)

	tests := []struct {
		name      string
		iterType  streamstypes.ShardIteratorType
		wantCount int
	}{
		{
			name:      "AT_SEQUENCE_NUMBER includes the given record",
			iterType:  streamstypes.ShardIteratorTypeAtSequenceNumber,
			wantCount: 2, // middle + last
		},
		{
			name:      "AFTER_SEQUENCE_NUMBER excludes the given record",
			iterType:  streamstypes.ShardIteratorTypeAfterSequenceNumber,
			wantCount: 1, // last only
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			iterBody := fmt.Sprintf(
				`{"StreamArn":"%s","ShardId":"%s","ShardIteratorType":"%s","SequenceNumber":"%s"}`,
				table.StreamARN, shardID, string(tt.iterType), midSeq,
			)
			iterResp := doRequest(t, handler, "GetShardIterator", iterBody)
			require.Equal(t, http.StatusOK, iterResp.Code)

			var iterOut map[string]any
			require.NoError(t, json.Unmarshal(iterResp.Body.Bytes(), &iterOut))
			shardIter := iterOut["ShardIterator"].(string)

			recResp := doRequest(t, handler, "GetRecords", fmt.Sprintf(`{"ShardIterator":"%s"}`, shardIter))
			require.Equal(t, http.StatusOK, recResp.Code)

			var recOut map[string]any
			require.NoError(t, json.Unmarshal(recResp.Body.Bytes(), &recOut))
			records, _ := recOut["Records"].([]any)
			assert.Len(t, records, tt.wantCount)
		})
	}
}

// TestHandler_GetShardIterator_AtAfterSequenceNumber_MissingSequenceNumber verifies
// that AT_SEQUENCE_NUMBER/AFTER_SEQUENCE_NUMBER without a SequenceNumber returns a
// ValidationException over HTTP, matching the real API's required-parameter semantics.
func TestHandler_GetShardIterator_AtAfterSequenceNumber_MissingSequenceNumber(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		iterType streamstypes.ShardIteratorType
	}{
		{name: "AT_SEQUENCE_NUMBER without SequenceNumber", iterType: streamstypes.ShardIteratorTypeAtSequenceNumber},
		{
			name:     "AFTER_SEQUENCE_NUMBER without SequenceNumber",
			iterType: streamstypes.ShardIteratorTypeAfterSequenceNumber,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, streamARN := newTestBackend(t)
			handler := dynamodbstreams.NewHandler(db)

			body := fmt.Sprintf(
				`{"StreamArn":"%s","ShardId":"shardId-00000000000000000001-00000001","ShardIteratorType":"%s"}`,
				streamARN, string(tt.iterType),
			)
			w := doRequest(t, handler, "GetShardIterator", body)

			assert.Equal(t, http.StatusBadRequest, w.Code)

			var errBody map[string]string
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &errBody))
			assert.Contains(t, errBody["__type"], "ValidationException")
			assert.Contains(t, errBody["__type"], "dynamodbstreams")
		})
	}
}

// TestHandler_ErrorPropagation_TrimmedDataAccess verifies that requesting stream
// records at a sequence number that has aged out of the ring buffer (past the
// 1000-record retention window) returns TrimmedDataAccessException with the
// dynamodbstreams namespace and HTTP 400, matching real AWS 24h-retention behavior.
func TestHandler_ErrorPropagation_TrimmedDataAccess(t *testing.T) {
	t.Parallel()

	db := ddbbackend.NewInMemoryDB()
	ctx := t.Context()

	const tableName = "TrimmedTable"
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

	// Write 1001 items: the ring buffer holds 1000, so the very first record
	// (sequence number 1) is now trimmed.
	for i := range 1001 {
		_, err = db.PutItem(ctx, &ddbsdk.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]ddbtypes.AttributeValue{
				"pk": &ddbtypes.AttributeValueMemberS{Value: fmt.Sprintf("key-%d", i)},
			},
		})
		require.NoError(t, err)
	}

	table, ok := db.GetTable(tableName)
	require.True(t, ok)

	handler := dynamodbstreams.NewHandler(db)

	// Sequence number 1 (the trimmed record) as a zero-padded 20-digit string.
	const trimmedSeq = "00000000000000000001"

	iterBody := fmt.Sprintf(
		`{"StreamArn":"%s","ShardId":"shardId-00000000000000000001-00000001",`+
			`"ShardIteratorType":"AT_SEQUENCE_NUMBER","SequenceNumber":"%s"}`,
		table.StreamARN, trimmedSeq,
	)
	w := doRequest(t, handler, "GetShardIterator", iterBody)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	var errBody map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &errBody))
	assert.Contains(t, errBody["__type"], "TrimmedDataAccessException")
	assert.Contains(t, errBody["__type"], "dynamodbstreams",
		"error __type must use the dynamodbstreams namespace")
	assert.NotEmpty(t, errBody["message"])
}

// TestHandler_GetRecords_StreamViewType_RecordShapes verifies that the
// Keys/NewImage/OldImage fields present in each stream record match the
// table's StreamViewType, per real AWS semantics:
//
//	KEYS_ONLY          -- Keys only
//	NEW_IMAGE          -- Keys + NewImage (INSERT/MODIFY); no OldImage ever
//	OLD_IMAGE          -- Keys + OldImage (MODIFY only, since INSERT has no old item); no NewImage ever
//	NEW_AND_OLD_IMAGES -- Keys + NewImage (INSERT/MODIFY) + OldImage (MODIFY)
func TestHandler_GetRecords_StreamViewType_RecordShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		viewType           ddbtypes.StreamViewType
		wantInsertNewImage bool
		wantInsertOldImage bool
		wantModifyNewImage bool
		wantModifyOldImage bool
	}{
		{
			name:               "KEYS_ONLY",
			viewType:           ddbtypes.StreamViewTypeKeysOnly,
			wantInsertNewImage: false,
			wantInsertOldImage: false,
			wantModifyNewImage: false,
			wantModifyOldImage: false,
		},
		{
			name:               "NEW_IMAGE",
			viewType:           ddbtypes.StreamViewTypeNewImage,
			wantInsertNewImage: true,
			wantInsertOldImage: false,
			wantModifyNewImage: true,
			wantModifyOldImage: false,
		},
		{
			name:               "OLD_IMAGE",
			viewType:           ddbtypes.StreamViewTypeOldImage,
			wantInsertNewImage: false,
			wantInsertOldImage: false,
			wantModifyNewImage: false,
			wantModifyOldImage: true,
		},
		{
			name:               "NEW_AND_OLD_IMAGES",
			viewType:           ddbtypes.StreamViewTypeNewAndOldImages,
			wantInsertNewImage: true,
			wantInsertOldImage: false,
			wantModifyNewImage: true,
			wantModifyOldImage: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := ddbbackend.NewInMemoryDB()
			ctx := t.Context()

			tableName := "ViewTypeTable" + tt.name
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
					StreamViewType: tt.viewType,
				},
			})
			require.NoError(t, err)

			// INSERT.
			_, err = db.PutItem(ctx, &ddbsdk.PutItemInput{
				TableName: aws.String(tableName),
				Item: map[string]ddbtypes.AttributeValue{
					"pk":   &ddbtypes.AttributeValueMemberS{Value: "row"},
					"attr": &ddbtypes.AttributeValueMemberS{Value: "v1"},
				},
			})
			require.NoError(t, err)

			// MODIFY.
			_, err = db.PutItem(ctx, &ddbsdk.PutItemInput{
				TableName: aws.String(tableName),
				Item: map[string]ddbtypes.AttributeValue{
					"pk":   &ddbtypes.AttributeValueMemberS{Value: "row"},
					"attr": &ddbtypes.AttributeValueMemberS{Value: "v2"},
				},
			})
			require.NoError(t, err)

			table, ok := db.GetTable(tableName)
			require.True(t, ok)

			handler := dynamodbstreams.NewHandler(db)

			iterBody := fmt.Sprintf(
				`{"StreamArn":"%s","ShardId":"shardId-00000000000000000001-00000001","ShardIteratorType":"TRIM_HORIZON"}`,
				table.StreamARN,
			)
			iterResp := doRequest(t, handler, "GetShardIterator", iterBody)
			require.Equal(t, http.StatusOK, iterResp.Code)
			var iterOut map[string]any
			require.NoError(t, json.Unmarshal(iterResp.Body.Bytes(), &iterOut))
			shardIter := iterOut["ShardIterator"].(string)

			recResp := doRequest(t, handler, "GetRecords", fmt.Sprintf(`{"ShardIterator":"%s"}`, shardIter))
			require.Equal(t, http.StatusOK, recResp.Code)
			var recOut map[string]any
			require.NoError(t, json.Unmarshal(recResp.Body.Bytes(), &recOut))
			records := recOut["Records"].([]any)
			require.Len(t, records, 2, "must have exactly INSERT + MODIFY records")

			insert := records[0].(map[string]any)
			require.Equal(t, "INSERT", insert["eventName"])
			insertData := insert["dynamodb"].(map[string]any)
			assert.NotNil(t, insertData["Keys"], "Keys must always be present")
			assertPresence(t, insertData, "NewImage", tt.wantInsertNewImage)
			assertPresence(t, insertData, "OldImage", tt.wantInsertOldImage)

			modify := records[1].(map[string]any)
			require.Equal(t, "MODIFY", modify["eventName"])
			modifyData := modify["dynamodb"].(map[string]any)
			assert.NotNil(t, modifyData["Keys"], "Keys must always be present")
			assertPresence(t, modifyData, "NewImage", tt.wantModifyNewImage)
			assertPresence(t, modifyData, "OldImage", tt.wantModifyOldImage)
		})
	}
}

// assertPresence asserts whether key is present (non-nil) in m according to want.
func assertPresence(t *testing.T, m map[string]any, key string, want bool) {
	t.Helper()

	v, exists := m[key]
	if want {
		assert.True(t, exists && v != nil, "%s must be present", key)
	} else {
		assert.False(t, exists && v != nil, "%s must be absent", key)
	}
}

// TestHandler_GetRecords_Limit verifies that GetRecords honors the Limit
// parameter, returning at most that many records and a NextShardIterator that
// can be used to fetch the remainder.
func TestHandler_GetRecords_Limit(t *testing.T) {
	t.Parallel()

	db := ddbbackend.NewInMemoryDB()
	ctx := t.Context()

	const tableName = "LimitTable"
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

	iterBody := fmt.Sprintf(
		`{"StreamArn":"%s","ShardId":"shardId-00000000000000000001-00000001","ShardIteratorType":"TRIM_HORIZON"}`,
		table.StreamARN,
	)
	iterResp := doRequest(t, handler, "GetShardIterator", iterBody)
	require.Equal(t, http.StatusOK, iterResp.Code)
	var iterOut map[string]any
	require.NoError(t, json.Unmarshal(iterResp.Body.Bytes(), &iterOut))
	shardIter := iterOut["ShardIterator"].(string)

	// First page: Limit=2.
	recResp := doRequest(t, handler, "GetRecords", fmt.Sprintf(`{"ShardIterator":"%s","Limit":2}`, shardIter))
	require.Equal(t, http.StatusOK, recResp.Code)
	var recOut map[string]any
	require.NoError(t, json.Unmarshal(recResp.Body.Bytes(), &recOut))
	records := recOut["Records"].([]any)
	require.Len(t, records, 2, "Limit must cap the number of records returned")

	nextIter, ok := recOut["NextShardIterator"].(string)
	require.True(t, ok, "NextShardIterator must be present so pollers can continue")

	// Second page: read the remainder.
	recResp2 := doRequest(t, handler, "GetRecords", fmt.Sprintf(`{"ShardIterator":"%s"}`, nextIter))
	require.Equal(t, http.StatusOK, recResp2.Code)
	var recOut2 map[string]any
	require.NoError(t, json.Unmarshal(recResp2.Body.Bytes(), &recOut2))
	records2 := recOut2["Records"].([]any)
	assert.Len(t, records2, 3, "remaining 3 records must be returned on the next page")
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
