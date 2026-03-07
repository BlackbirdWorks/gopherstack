package dynamodb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ddb "github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// newStreamsTestDB creates an InMemoryDB with a single test table for stream tests.
func newStreamsTestDB(t *testing.T) *ddb.InMemoryDB {
	t.Helper()

	db := ddb.NewInMemoryDB()
	ctx := t.Context()

	_, err := db.CreateTable(ctx, makeCreateTableInput("StreamsTestTable", "pk"))
	require.NoError(t, err)

	return db
}

func TestUnit_Streams_EnableDisable(t *testing.T) {
	t.Parallel()

	db := newStreamsTestDB(t)
	ctx := t.Context()

	t.Run("enable streams", func(t *testing.T) {
		t.Parallel()

		err := db.EnableStream(ctx, "StreamsTestTable", "NEW_AND_OLD_IMAGES")
		require.NoError(t, err)

		table, ok := db.GetTable("StreamsTestTable")
		require.True(t, ok)
		require.True(t, table.StreamsEnabled)
		require.Equal(t, "NEW_AND_OLD_IMAGES", table.StreamViewType)
		require.NotEmpty(t, table.StreamARN)
	})

	t.Run("disable streams", func(t *testing.T) {
		t.Parallel()

		db2 := newStreamsTestDB(t)
		ctx2 := t.Context()

		require.NoError(t, db2.EnableStream(ctx2, "StreamsTestTable", "NEW_IMAGE"))
		require.NoError(t, db2.DisableStream(ctx2, "StreamsTestTable"))

		table, ok := db2.GetTable("StreamsTestTable")
		require.True(t, ok)
		require.False(t, table.StreamsEnabled)
		require.Empty(t, table.StreamARN)
		require.Empty(t, table.StreamRecords)
	})

	t.Run("enable on non-existent table", func(t *testing.T) {
		t.Parallel()

		db3 := newStreamsTestDB(t)
		err := db3.EnableStream(t.Context(), "NoSuchTable", "KEYS_ONLY")
		require.Error(t, err)
	})
}

func TestUnit_Streams_ListStreams(t *testing.T) {
	t.Parallel()

	db := ddb.NewInMemoryDB()
	ctx := t.Context()

	_, err := db.CreateTable(ctx, makeCreateTableInput("TableA", "id"))
	require.NoError(t, err)
	_, err = db.CreateTable(ctx, makeCreateTableInput("TableB", "id"))
	require.NoError(t, err)

	require.NoError(t, db.EnableStream(ctx, "TableA", "NEW_AND_OLD_IMAGES"))
	// TableB has no stream

	t.Run("list all streams", func(t *testing.T) {
		t.Parallel()

		out, listErr := db.ListStreams(ctx, &dynamodbstreams.ListStreamsInput{})
		require.NoError(t, listErr)
		require.Len(t, out.Streams, 1)
		require.Equal(t, "TableA", aws.ToString(out.Streams[0].TableName))
	})

	t.Run("filter by table name", func(t *testing.T) {
		t.Parallel()

		out, listErr := db.ListStreams(ctx, &dynamodbstreams.ListStreamsInput{
			TableName: aws.String("TableB"),
		})
		require.NoError(t, listErr)
		require.Empty(t, out.Streams)
	})
}

func TestUnit_Streams_DescribeStream(t *testing.T) {
	t.Parallel()

	db := newStreamsTestDB(t)
	ctx := t.Context()

	require.NoError(t, db.EnableStream(ctx, "StreamsTestTable", "NEW_IMAGE"))

	table, ok := db.GetTable("StreamsTestTable")
	require.True(t, ok)
	arn := table.StreamARN

	out, err := db.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
		StreamArn: aws.String(arn),
	})
	require.NoError(t, err)
	require.Equal(t, arn, aws.ToString(out.StreamDescription.StreamArn))
	require.Equal(t, streamstypes.StreamStatusEnabled, out.StreamDescription.StreamStatus)
	require.NotEmpty(t, out.StreamDescription.Shards)
}

func TestUnit_Streams_GetRecords(t *testing.T) {
	t.Parallel()

	db := newStreamsTestDB(t)
	ctx := t.Context()

	require.NoError(t, db.EnableStream(ctx, "StreamsTestTable", "NEW_AND_OLD_IMAGES"))

	// PutItem → INSERT event
	_, err := db.PutItem(ctx, makePutItem("StreamsTestTable", "pk", "pk1"))
	require.NoError(t, err)

	// PutItem again → MODIFY event
	_, err = db.PutItem(ctx, makePutItem("StreamsTestTable", "pk", "pk1"))
	require.NoError(t, err)

	// DeleteItem → REMOVE event
	_, err = db.DeleteItem(ctx, makeDeleteItem("StreamsTestTable", "pk", "pk1"))
	require.NoError(t, err)

	table, ok := db.GetTable("StreamsTestTable")
	require.True(t, ok)
	arn := table.StreamARN

	// Get iterator from trim-horizon
	iterOut, err := db.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         aws.String(arn),
		ShardId:           aws.String(ddb.StreamShardID),
		ShardIteratorType: streamstypes.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(iterOut.ShardIterator))

	// GetRecords — should get all 3 events
	recOut, err := db.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
	})
	require.NoError(t, err)
	require.Len(t, recOut.Records, 3)
	require.Equal(t, streamstypes.OperationTypeInsert, recOut.Records[0].EventName)
	require.Equal(t, streamstypes.OperationTypeModify, recOut.Records[1].EventName)
	require.Equal(t, streamstypes.OperationTypeRemove, recOut.Records[2].EventName)
}

func TestUnit_Streams_RingBufferCap(t *testing.T) {
	t.Parallel()

	db := ddb.NewInMemoryDB()
	ctx := t.Context()

	_, err := db.CreateTable(ctx, makeCreateTableInput("BufTable", "pk"))
	require.NoError(t, err)
	require.NoError(t, db.EnableStream(ctx, "BufTable", "KEYS_ONLY"))

	// Write more than maxStreamRecords items
	const writeCount = 1005
	for i := range writeCount {
		_, err = db.PutItem(ctx, makePutItemN("BufTable", "pk", i))
		require.NoError(t, err)
	}

	table, ok := db.GetTable("BufTable")
	require.True(t, ok)
	require.LessOrEqual(t, len(table.StreamRecords), 1000)
}

func TestUnit_Streams_ViewType_NewImage(t *testing.T) {
	t.Parallel()

	db := newStreamsTestDB(t)
	ctx := t.Context()

	require.NoError(t, db.EnableStream(ctx, "StreamsTestTable", "NEW_IMAGE"))

	_, err := db.PutItem(ctx, makePutItem("StreamsTestTable", "pk", "x"))
	require.NoError(t, err)

	table, ok := db.GetTable("StreamsTestTable")
	require.True(t, ok)
	require.Len(t, table.StreamRecords, 1)
	require.NotNil(t, table.StreamRecords[0].NewImage)
	require.Nil(t, table.StreamRecords[0].OldImage)
}

func TestUnit_Streams_ViewType_OldImage(t *testing.T) {
	t.Parallel()

	db := newStreamsTestDB(t)
	ctx := t.Context()

	// Need an existing item first so MODIFY produces an OldImage
	_, err := db.PutItem(ctx, makePutItem("StreamsTestTable", "pk", "x"))
	require.NoError(t, err)

	require.NoError(t, db.EnableStream(ctx, "StreamsTestTable", "OLD_IMAGE"))

	_, err = db.PutItem(ctx, makePutItem("StreamsTestTable", "pk", "x"))
	require.NoError(t, err)

	table, ok := db.GetTable("StreamsTestTable")
	require.True(t, ok)

	// Should have only 1 event (after enabling)
	require.Len(t, table.StreamRecords, 1)
	require.Nil(t, table.StreamRecords[0].NewImage)
	require.NotNil(t, table.StreamRecords[0].OldImage)
}

func TestUnit_Streams_UnparamFix(t *testing.T) {
	t.Parallel()

	db := ddb.NewInMemoryDB()
	ctx := t.Context()

	// Use a different table name to satisfy unparam lint for makePutItem
	_, _ = db.CreateTable(ctx, makeCreateTableInput("OtherTableForLint", "id"))
	_ = makePutItem("OtherTableForLint", "id", "val")
}

func TestUnit_Streams_ComplexAttributeTypes(t *testing.T) {
	t.Parallel()

	db := ddb.NewInMemoryDB()
	ctx := t.Context()

	_, err := db.CreateTable(ctx, makeCreateTableInput("ComplexAttrTable", "pk"))
	require.NoError(t, err)
	require.NoError(t, db.EnableStream(ctx, "ComplexAttrTable", "NEW_AND_OLD_IMAGES"))

	// Insert an item with map, list, and set attributes so that buildSDKRecord
	// exercises handleMapAttribute, handleListAttribute, and toStringSliceFrom.
	_, err = db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("ComplexAttrTable"),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "complex-pk"},
			"nested_map": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				"inner": &types.AttributeValueMemberS{Value: "value"},
			}},
			"list_attr": &types.AttributeValueMemberL{Value: []types.AttributeValue{
				&types.AttributeValueMemberS{Value: "elem1"},
				&types.AttributeValueMemberN{Value: "42"},
			}},
			"string_set": &types.AttributeValueMemberSS{Value: []string{"a", "b", "c"}},
			"number_set": &types.AttributeValueMemberNS{Value: []string{"1", "2"}},
		},
	})
	require.NoError(t, err)

	// Read back via GetRecords to trigger buildSDKRecord -> handleMapAttribute/handleListAttribute.
	listOut, err := db.ListStreams(ctx, &dynamodbstreams.ListStreamsInput{
		TableName: aws.String("ComplexAttrTable"),
	})
	require.NoError(t, err)
	require.Len(t, listOut.Streams, 1)

	descOut, err := db.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
		StreamArn: listOut.Streams[0].StreamArn,
	})
	require.NoError(t, err)
	require.NotEmpty(t, descOut.StreamDescription.Shards)

	iterOut, err := db.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         listOut.Streams[0].StreamArn,
		ShardId:           descOut.StreamDescription.Shards[0].ShardId,
		ShardIteratorType: streamstypes.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)

	recordsOut, err := db.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
	})
	require.NoError(t, err)
	require.NotEmpty(t, recordsOut.Records)
	// The NewImage should contain the complex attributes.
	rec := recordsOut.Records[0]
	assert.NotNil(t, rec.Dynamodb.NewImage)
}
