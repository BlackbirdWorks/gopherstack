package dynamodb_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ddb "github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// ============================================================
// Section 1: Opaque shard iterator tokens
// ============================================================

func TestUnit_Streams_OpaqueIterator_TokenIsOpaque(t *testing.T) {
	t.Parallel()

	db := newStreamsTestDB(t)
	ctx := t.Context()

	require.NoError(t, db.EnableStream(ctx, "StreamsTestTable", "NEW_AND_OLD_IMAGES"))

	table, ok := db.GetTable("StreamsTestTable")
	require.True(t, ok)

	iterOut, err := db.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         aws.String(table.StreamARN),
		ShardId:           aws.String(ddb.StreamShardID),
		ShardIteratorType: streamstypes.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)

	token := aws.ToString(iterOut.ShardIterator)
	require.NotEmpty(t, token)

	// Token must NOT contain the table name (would indicate plain-text encoding).
	assert.NotContains(t, token, "StreamsTestTable",
		"shard iterator token must be opaque (must not contain table name)")

	// Token must NOT be colon-separated (would indicate legacy plain-text format).
	assert.False(t, strings.Contains(token, ":") && strings.Count(token, ":") == 2,
		"shard iterator token must not be in legacy tableName:seq:ts format")
}

func TestUnit_Streams_OpaqueIterator_RegistrationInStore(t *testing.T) {
	t.Parallel()

	db := newStreamsTestDB(t)
	ctx := t.Context()

	require.NoError(t, db.EnableStream(ctx, "StreamsTestTable", "NEW_AND_OLD_IMAGES"))

	table, ok := db.GetTable("StreamsTestTable")
	require.True(t, ok)

	sizeBefore := db.IteratorStoreSize()

	_, err := db.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         aws.String(table.StreamARN),
		ShardId:           aws.String(ddb.StreamShardID),
		ShardIteratorType: streamstypes.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)

	assert.Equal(t, sizeBefore+1, db.IteratorStoreSize(),
		"GetShardIterator must register a token in the iterator store")
}

func TestUnit_Streams_OpaqueIterator_GetRecordsRegistersNextToken(t *testing.T) {
	t.Parallel()

	db := newStreamsTestDB(t)
	ctx := t.Context()

	require.NoError(t, db.EnableStream(ctx, "StreamsTestTable", "NEW_AND_OLD_IMAGES"))

	_, err := db.PutItem(ctx, makePutItem("StreamsTestTable", "pk", "x"))
	require.NoError(t, err)

	table, ok := db.GetTable("StreamsTestTable")
	require.True(t, ok)

	iterOut, err := db.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         aws.String(table.StreamARN),
		ShardId:           aws.String(ddb.StreamShardID),
		ShardIteratorType: streamstypes.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)

	sizeBefore := db.IteratorStoreSize()

	recOut, err := db.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
	})
	require.NoError(t, err)
	require.NotEmpty(t, recOut.Records)

	// GetRecords must create a new opaque token for the next iterator.
	assert.Greater(t, db.IteratorStoreSize(), sizeBefore,
		"GetRecords must register a new next-iterator token in the store")
	assert.NotEmpty(t, aws.ToString(recOut.NextShardIterator),
		"GetRecords must return a non-empty NextShardIterator")
}

func TestUnit_Streams_OpaqueIterator_SweepRemovesExpiredTokens(t *testing.T) {
	t.Parallel()

	db := newStreamsTestDB(t)
	ctx := t.Context()

	require.NoError(t, db.EnableStream(ctx, "StreamsTestTable", "NEW_AND_OLD_IMAGES"))

	table, ok := db.GetTable("StreamsTestTable")
	require.True(t, ok)

	_, err := db.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         aws.String(table.StreamARN),
		ShardId:           aws.String(ddb.StreamShardID),
		ShardIteratorType: streamstypes.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)
	require.Positive(t, db.IteratorStoreSize())

	// Sweeping before TTL should leave the token in place.
	db.SweepIterators()
	assert.Positive(t, db.IteratorStoreSize(),
		"Sweep must not remove unexpired tokens")
}

// ============================================================
// Section 2: GetShardIterator validation
// ============================================================

func TestUnit_Streams_GetShardIterator_Validation(t *testing.T) {
	t.Parallel()

	db := newStreamsTestDB(t)
	ctx := t.Context()

	require.NoError(t, db.EnableStream(ctx, "StreamsTestTable", "NEW_AND_OLD_IMAGES"))

	table, ok := db.GetTable("StreamsTestTable")
	require.True(t, ok)
	streamARN := table.StreamARN

	tests := []struct {
		name            string
		input           *dynamodbstreams.GetShardIteratorInput
		wantErrContains string
	}{
		{
			name: "missing StreamArn",
			input: &dynamodbstreams.GetShardIteratorInput{
				ShardId:           aws.String(ddb.StreamShardID),
				ShardIteratorType: streamstypes.ShardIteratorTypeTrimHorizon,
			},
			wantErrContains: "StreamArn",
		},
		{
			name: "missing ShardId",
			input: &dynamodbstreams.GetShardIteratorInput{
				StreamArn:         aws.String(streamARN),
				ShardIteratorType: streamstypes.ShardIteratorTypeTrimHorizon,
			},
			wantErrContains: "ShardId",
		},
		{
			name: "unknown ShardId",
			input: &dynamodbstreams.GetShardIteratorInput{
				StreamArn:         aws.String(streamARN),
				ShardId:           aws.String("shardId-99999999999999999999-00000001"),
				ShardIteratorType: streamstypes.ShardIteratorTypeTrimHorizon,
			},
			wantErrContains: "does not exist",
		},
		{
			name: "stream not found",
			input: &dynamodbstreams.GetShardIteratorInput{
				StreamArn: aws.String(
					"arn:aws:dynamodb:us-east-1:123456789012:table/NoTable/stream/2024-01-01T00:00:00.000",
				),
				ShardId:           aws.String(ddb.StreamShardID),
				ShardIteratorType: streamstypes.ShardIteratorTypeTrimHorizon,
			},
			wantErrContains: "stream not found",
		},
		{
			name: "AT_SEQUENCE_NUMBER without SequenceNumber",
			input: &dynamodbstreams.GetShardIteratorInput{
				StreamArn:         aws.String(streamARN),
				ShardId:           aws.String(ddb.StreamShardID),
				ShardIteratorType: streamstypes.ShardIteratorTypeAtSequenceNumber,
			},
			wantErrContains: "SequenceNumber is required",
		},
		{
			name: "AFTER_SEQUENCE_NUMBER without SequenceNumber",
			input: &dynamodbstreams.GetShardIteratorInput{
				StreamArn:         aws.String(streamARN),
				ShardId:           aws.String(ddb.StreamShardID),
				ShardIteratorType: streamstypes.ShardIteratorTypeAfterSequenceNumber,
			},
			wantErrContains: "SequenceNumber is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := db.GetShardIterator(ctx, tt.input)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErrContains)
		})
	}
}

func TestUnit_Streams_GetShardIterator_AllIteratorTypes(t *testing.T) {
	t.Parallel()

	db := newStreamsTestDB(t)
	ctx := t.Context()

	require.NoError(t, db.EnableStream(ctx, "StreamsTestTable", "NEW_AND_OLD_IMAGES"))

	// Write 3 records to have a non-empty stream.
	for i := range 3 {
		_, err := db.PutItem(ctx, makePutItemN("StreamsTestTable", "pk", i))
		require.NoError(t, err)
	}

	table, ok := db.GetTable("StreamsTestTable")
	require.True(t, ok)
	streamARN := table.StreamARN

	// Get a sequence number for AT/AFTER tests.
	iterH, err := db.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         aws.String(streamARN),
		ShardId:           aws.String(ddb.StreamShardID),
		ShardIteratorType: streamstypes.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)
	recOut, err := db.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{ShardIterator: iterH.ShardIterator})
	require.NoError(t, err)
	require.Len(t, recOut.Records, 3)
	seqNum := aws.ToString(recOut.Records[1].Dynamodb.SequenceNumber) // middle record

	tests := []struct {
		name      string
		iterType  streamstypes.ShardIteratorType
		seqNum    string
		wantCount int // expected number of records returned
	}{
		{
			name:      "TrimHorizon returns all records",
			iterType:  streamstypes.ShardIteratorTypeTrimHorizon,
			wantCount: 3,
		},
		{
			name:      "Latest returns no records",
			iterType:  streamstypes.ShardIteratorTypeLatest,
			wantCount: 0,
		},
		{
			name:      "AtSequenceNumber returns records from that seq",
			iterType:  streamstypes.ShardIteratorTypeAtSequenceNumber,
			seqNum:    seqNum,
			wantCount: 2, // record[1] and record[2]
		},
		{
			name:      "AfterSequenceNumber returns records after that seq",
			iterType:  streamstypes.ShardIteratorTypeAfterSequenceNumber,
			seqNum:    seqNum,
			wantCount: 1, // only record[2]
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			inp := &dynamodbstreams.GetShardIteratorInput{
				StreamArn:         aws.String(streamARN),
				ShardId:           aws.String(ddb.StreamShardID),
				ShardIteratorType: tt.iterType,
			}
			if tt.seqNum != "" {
				inp.SequenceNumber = aws.String(tt.seqNum)
			}

			iterOut, iterErr := db.GetShardIterator(ctx, inp)
			require.NoError(t, iterErr)

			rOut, rErr := db.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
				ShardIterator: iterOut.ShardIterator,
			})
			require.NoError(t, rErr)
			assert.Len(t, rOut.Records, tt.wantCount)
		})
	}
}

// ============================================================
// Section 3: Shard genealogy and shard splits
// ============================================================

func TestUnit_Streams_Shards_InitialShardOnEnable(t *testing.T) {
	t.Parallel()

	db := newStreamsTestDB(t)
	ctx := t.Context()

	require.NoError(t, db.EnableStream(ctx, "StreamsTestTable", "NEW_IMAGE"))

	shards := db.StreamShards("StreamsTestTable")
	require.Len(t, shards, 1, "EnableStream must create exactly 1 initial shard")
	assert.Equal(t, ddb.StreamShardID, shards[0].ShardID)
	assert.Empty(t, shards[0].ParentShardID, "first shard must have no parent")
	assert.Equal(t, int64(0), shards[0].EndingSequenceNum, "first shard must be open")
}

func TestUnit_Streams_Shards_ShardSplitOnRingBufferWrap(t *testing.T) {
	t.Parallel()

	db := ddb.NewInMemoryDB()
	ctx := t.Context()

	_, err := db.CreateTable(ctx, makeCreateTableInput("SplitTable", "pk"))
	require.NoError(t, err)
	require.NoError(t, db.EnableStream(ctx, "SplitTable", "KEYS_ONLY"))

	// Write exactly maxStreamRecords (1000) items — buffer fills up but no wrap yet.
	for i := range 1000 {
		_, err = db.PutItem(ctx, makePutItemN("SplitTable", "pk", i))
		require.NoError(t, err)
	}

	// Buffer is now full. No split yet.
	shards := db.StreamShards("SplitTable")
	require.Len(t, shards, 1, "no shard split before ring buffer wraps")
	assert.Equal(t, int64(0), shards[0].EndingSequenceNum, "shard still open")

	// Write one more item — this is the first write to enter the else branch with StreamHead==0,
	// which triggers the first shard split.
	_, err = db.PutItem(ctx, makePutItemN("SplitTable", "pk", 1001))
	require.NoError(t, err)

	shards = db.StreamShards("SplitTable")
	require.GreaterOrEqual(t, len(shards), 2,
		"a shard split must have occurred after ring buffer completed a full rotation")

	// Verify genealogy.
	first := shards[0]
	second := shards[1]
	assert.Equal(t, ddb.StreamShardID, first.ShardID)
	assert.NotEqual(t, int64(0), first.EndingSequenceNum, "first shard must be closed after split")
	assert.Equal(t, first.ShardID, second.ParentShardID, "second shard's parent must be the first shard")
	assert.Equal(t, int64(0), second.EndingSequenceNum, "second shard must still be open")
}

func TestUnit_Streams_Shards_DescribeStreamReturnsGenealogy(t *testing.T) {
	t.Parallel()

	db := ddb.NewInMemoryDB()
	ctx := t.Context()

	_, err := db.CreateTable(ctx, makeCreateTableInput("GenTable", "pk"))
	require.NoError(t, err)
	require.NoError(t, db.EnableStream(ctx, "GenTable", "KEYS_ONLY"))

	// Force a shard split by writing 2001 items.
	for i := range 2001 {
		_, err = db.PutItem(ctx, makePutItemN("GenTable", "pk", i))
		require.NoError(t, err)
	}

	table, ok := db.GetTable("GenTable")
	require.True(t, ok)

	out, err := db.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
		StreamArn: aws.String(table.StreamARN),
	})
	require.NoError(t, err)

	shards := out.StreamDescription.Shards
	require.GreaterOrEqual(t, len(shards), 2,
		"DescribeStream must expose shard genealogy after a split")

	// First shard must have EndingSequenceNumber set (closed).
	require.NotNil(t, shards[0].SequenceNumberRange)
	assert.NotEmpty(t, aws.ToString(shards[0].SequenceNumberRange.EndingSequenceNumber),
		"closed shard must have EndingSequenceNumber set")

	// Second shard must reference the first as its parent.
	assert.Equal(t,
		aws.ToString(shards[0].ShardId),
		aws.ToString(shards[1].ParentShardId),
		"second shard must have ParentShardId pointing to first shard")
}

func TestUnit_Streams_Shards_DisableStreamClearsShards(t *testing.T) {
	t.Parallel()

	db := newStreamsTestDB(t)
	ctx := t.Context()

	require.NoError(t, db.EnableStream(ctx, "StreamsTestTable", "NEW_IMAGE"))

	// Write some records.
	_, err := db.PutItem(ctx, makePutItem("StreamsTestTable", "pk", "x"))
	require.NoError(t, err)

	require.NoError(t, db.DisableStream(ctx, "StreamsTestTable"))

	shards := db.StreamShards("StreamsTestTable")
	assert.Empty(t, shards, "DisableStream must clear shard history")
	assert.Equal(t, int64(0), db.StreamTrimSeq("StreamsTestTable"),
		"DisableStream must reset trim horizon")
}

func TestUnit_Streams_Shards_ReEnableCreatesNewShard(t *testing.T) {
	t.Parallel()

	db := newStreamsTestDB(t)
	ctx := t.Context()

	require.NoError(t, db.EnableStream(ctx, "StreamsTestTable", "NEW_IMAGE"))
	require.NoError(t, db.DisableStream(ctx, "StreamsTestTable"))
	require.NoError(t, db.EnableStream(ctx, "StreamsTestTable", "NEW_IMAGE"))

	shards := db.StreamShards("StreamsTestTable")
	require.Len(t, shards, 1, "re-enabling stream must create a fresh first shard")
	assert.Equal(t, int64(0), shards[0].EndingSequenceNum, "new shard must be open")
}

// ============================================================
// Section 4: Trim horizon and TrimmedDataAccessException
// ============================================================

func TestUnit_Streams_TrimHorizon_AdvancesWhenBufferWraps(t *testing.T) {
	t.Parallel()

	db := ddb.NewInMemoryDB()
	ctx := t.Context()

	_, err := db.CreateTable(ctx, makeCreateTableInput("TrimTable", "pk"))
	require.NoError(t, err)
	require.NoError(t, db.EnableStream(ctx, "TrimTable", "KEYS_ONLY"))

	// Buffer not full — trim horizon should be 0.
	for i := range 500 {
		_, err = db.PutItem(ctx, makePutItemN("TrimTable", "pk", i))
		require.NoError(t, err)
	}
	assert.Equal(t, int64(0), db.StreamTrimSeq("TrimTable"),
		"trim horizon must be 0 while buffer is not full")

	// Fill buffer to capacity.
	for i := 500; i < 1000; i++ {
		_, err = db.PutItem(ctx, makePutItemN("TrimTable", "pk", i))
		require.NoError(t, err)
	}
	assert.Equal(t, int64(0), db.StreamTrimSeq("TrimTable"),
		"trim horizon must still be 0 when buffer just hits capacity")

	// Write one more — buffer wraps, trim horizon advances.
	_, err = db.PutItem(ctx, makePutItemN("TrimTable", "pk", 1001))
	require.NoError(t, err)

	trimSeq := db.StreamTrimSeq("TrimTable")
	assert.Positive(t, trimSeq,
		"trim horizon must advance once the ring buffer starts overwriting records")
}

func TestUnit_Streams_TrimmedDataAccess_GetRecordsReturnsError(t *testing.T) {
	t.Parallel()

	db := ddb.NewInMemoryDB()
	ctx := t.Context()

	_, err := db.CreateTable(ctx, makeCreateTableInput("TrimErrTable", "pk"))
	require.NoError(t, err)
	require.NoError(t, db.EnableStream(ctx, "TrimErrTable", "KEYS_ONLY"))

	// Fill buffer and cause it to wrap — seq 1 will be evicted.
	for i := range 1002 {
		_, err = db.PutItem(ctx, makePutItemN("TrimErrTable", "pk", i))
		require.NoError(t, err)
	}

	trimSeq := db.StreamTrimSeq("TrimErrTable")
	require.Positive(t, trimSeq)

	table, ok := db.GetTable("TrimErrTable")
	require.True(t, ok)

	// Request an iterator pointing to a sequence that has been trimmed.
	trimmedSeqStr := fmt.Sprintf("%020d", trimSeq-1)
	_, iterErr := db.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         aws.String(table.StreamARN),
		ShardId:           aws.String(ddb.StreamShardID),
		ShardIteratorType: streamstypes.ShardIteratorTypeAtSequenceNumber,
		SequenceNumber:    aws.String(trimmedSeqStr),
	})
	require.Error(t, iterErr)
	assert.Contains(t, iterErr.Error(), "TrimmedDataAccessException",
		"requesting a trimmed sequence via GetShardIterator must return TrimmedDataAccessException")
}

// ============================================================
// Section 5: DescribeStream validation and pagination
// ============================================================

func TestUnit_Streams_DescribeStream_Validation(t *testing.T) {
	t.Parallel()

	db := newStreamsTestDB(t)
	ctx := t.Context()

	tests := []struct {
		name            string
		streamArn       string
		wantErrContains string
	}{
		{
			name:            "missing StreamArn returns validation error",
			streamArn:       "",
			wantErrContains: "StreamArn",
		},
		{
			name:            "unknown stream ARN returns ResourceNotFoundException",
			streamArn:       "arn:aws:dynamodb:us-east-1:123456789012:table/NoSuch/stream/2024-01-01T00:00:00.000",
			wantErrContains: "stream not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			inp := &dynamodbstreams.DescribeStreamInput{}
			if tt.streamArn != "" {
				inp.StreamArn = aws.String(tt.streamArn)
			}

			_, err := db.DescribeStream(ctx, inp)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErrContains)
		})
	}
}

func TestUnit_Streams_DescribeStream_Pagination(t *testing.T) {
	t.Parallel()

	db := ddb.NewInMemoryDB()
	ctx := t.Context()

	_, err := db.CreateTable(ctx, makeCreateTableInput("PagTable", "pk"))
	require.NoError(t, err)
	require.NoError(t, db.EnableStream(ctx, "PagTable", "KEYS_ONLY"))

	// Create 2 shards by causing a ring-buffer wrap.
	for i := range 2001 {
		_, err = db.PutItem(ctx, makePutItemN("PagTable", "pk", i))
		require.NoError(t, err)
	}

	table, ok := db.GetTable("PagTable")
	require.True(t, ok)

	// Describe with Limit=1 to get only the first shard.
	limit := int32(1)
	out1, err := db.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
		StreamArn: aws.String(table.StreamARN),
		Limit:     &limit,
	})
	require.NoError(t, err)
	require.Len(t, out1.StreamDescription.Shards, 1)
	require.NotNil(t, out1.StreamDescription.LastEvaluatedShardId,
		"Limit=1 must set LastEvaluatedShardId when more shards exist")

	// Page 2: use ExclusiveStartShardId.
	lastShardID := aws.ToString(out1.StreamDescription.LastEvaluatedShardId)
	out2, err := db.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
		StreamArn:             aws.String(table.StreamARN),
		ExclusiveStartShardId: aws.String(lastShardID),
	})
	require.NoError(t, err)
	require.NotEmpty(t, out2.StreamDescription.Shards,
		"second page must return the remaining shards")

	// Verify no overlap.
	firstPageIDs := make(map[string]bool)
	for _, s := range out1.StreamDescription.Shards {
		firstPageIDs[aws.ToString(s.ShardId)] = true
	}
	for _, s := range out2.StreamDescription.Shards {
		assert.False(t, firstPageIDs[aws.ToString(s.ShardId)],
			"second page must not overlap with first page")
	}
}

func TestUnit_Streams_DescribeStream_SequenceNumberRange(t *testing.T) {
	t.Parallel()

	db := newStreamsTestDB(t)
	ctx := t.Context()

	require.NoError(t, db.EnableStream(ctx, "StreamsTestTable", "NEW_AND_OLD_IMAGES"))

	// Write a few records.
	for i := range 5 {
		_, err := db.PutItem(ctx, makePutItemN("StreamsTestTable", "pk", i))
		require.NoError(t, err)
	}

	table, ok := db.GetTable("StreamsTestTable")
	require.True(t, ok)

	out, err := db.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
		StreamArn: aws.String(table.StreamARN),
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.StreamDescription.Shards)

	shard := out.StreamDescription.Shards[0]
	require.NotNil(t, shard.SequenceNumberRange)
	assert.NotEmpty(t, aws.ToString(shard.SequenceNumberRange.StartingSequenceNumber),
		"open shard must have StartingSequenceNumber")
	// Open shard must NOT have EndingSequenceNumber.
	assert.Empty(t, aws.ToString(shard.SequenceNumberRange.EndingSequenceNumber),
		"open shard must not have EndingSequenceNumber")
}

// ============================================================
// Section 6: ListStreams validation and pagination
// ============================================================

func TestUnit_Streams_ListStreams_Pagination(t *testing.T) {
	t.Parallel()

	db := ddb.NewInMemoryDB()
	ctx := t.Context()

	// Create 3 tables with streams enabled.
	for i := range 3 {
		name := fmt.Sprintf("PageStream%d", i)
		_, err := db.CreateTable(ctx, makeCreateTableInput(name, "pk"))
		require.NoError(t, err)
		require.NoError(t, db.EnableStream(ctx, name, "KEYS_ONLY"))
	}

	// Request Limit=1.
	limit := int32(1)
	out1, err := db.ListStreams(ctx, &dynamodbstreams.ListStreamsInput{Limit: &limit})
	require.NoError(t, err)
	require.Len(t, out1.Streams, 1)
	require.NotNil(t, out1.LastEvaluatedStreamArn,
		"ListStreams must set LastEvaluatedStreamArn when limit is reached")

	// Page 2.
	out2, err := db.ListStreams(ctx, &dynamodbstreams.ListStreamsInput{
		Limit:                   &limit,
		ExclusiveStartStreamArn: out1.LastEvaluatedStreamArn,
	})
	require.NoError(t, err)
	require.Len(t, out2.Streams, 1)

	// Page 3.
	out3, err := db.ListStreams(ctx, &dynamodbstreams.ListStreamsInput{
		Limit:                   &limit,
		ExclusiveStartStreamArn: out2.LastEvaluatedStreamArn,
	})
	require.NoError(t, err)
	require.Len(t, out3.Streams, 1)
	assert.Nil(t, out3.LastEvaluatedStreamArn,
		"last page must not set LastEvaluatedStreamArn")

	// Verify no overlap across pages.
	allARNs := make(map[string]int)
	for _, s := range out1.Streams {
		allARNs[aws.ToString(s.StreamArn)]++
	}
	for _, s := range out2.Streams {
		allARNs[aws.ToString(s.StreamArn)]++
	}
	for _, s := range out3.Streams {
		allARNs[aws.ToString(s.StreamArn)]++
	}
	for arn, count := range allARNs {
		assert.Equal(t, 1, count, "ARN %s appeared on multiple pages", arn)
	}
}

func TestUnit_Streams_ListStreams_FilterByTableName(t *testing.T) {
	t.Parallel()

	db := ddb.NewInMemoryDB()
	ctx := t.Context()

	for _, name := range []string{"FilterA", "FilterB", "FilterC"} {
		_, err := db.CreateTable(ctx, makeCreateTableInput(name, "pk"))
		require.NoError(t, err)
		require.NoError(t, db.EnableStream(ctx, name, "KEYS_ONLY"))
	}

	out, err := db.ListStreams(ctx, &dynamodbstreams.ListStreamsInput{
		TableName: aws.String("FilterB"),
	})
	require.NoError(t, err)
	require.Len(t, out.Streams, 1)
	assert.Equal(t, "FilterB", aws.ToString(out.Streams[0].TableName))
}

// ============================================================
// Section 7: GetRecords accuracy
// ============================================================

func TestUnit_Streams_GetRecords_Limit(t *testing.T) {
	t.Parallel()

	db := newStreamsTestDB(t)
	ctx := t.Context()

	require.NoError(t, db.EnableStream(ctx, "StreamsTestTable", "KEYS_ONLY"))

	for i := range 10 {
		_, err := db.PutItem(ctx, makePutItemN("StreamsTestTable", "pk", i))
		require.NoError(t, err)
	}

	table, ok := db.GetTable("StreamsTestTable")
	require.True(t, ok)

	iterOut, err := db.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         aws.String(table.StreamARN),
		ShardId:           aws.String(ddb.StreamShardID),
		ShardIteratorType: streamstypes.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)

	limit := int32(3)
	recOut, err := db.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
		Limit:         &limit,
	})
	require.NoError(t, err)
	assert.Len(t, recOut.Records, 3, "GetRecords must honor the Limit parameter")
}

func TestUnit_Streams_GetRecords_EmptyStream(t *testing.T) {
	t.Parallel()

	db := newStreamsTestDB(t)
	ctx := t.Context()

	require.NoError(t, db.EnableStream(ctx, "StreamsTestTable", "KEYS_ONLY"))

	table, ok := db.GetTable("StreamsTestTable")
	require.True(t, ok)

	iterOut, err := db.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         aws.String(table.StreamARN),
		ShardId:           aws.String(ddb.StreamShardID),
		ShardIteratorType: streamstypes.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)

	recOut, err := db.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
	})
	require.NoError(t, err)
	assert.Empty(t, recOut.Records, "empty stream must return zero records")
	assert.NotEmpty(t, aws.ToString(recOut.NextShardIterator),
		"empty stream must still return a valid NextShardIterator")
}

func TestUnit_Streams_GetRecords_MissingShardIterator(t *testing.T) {
	t.Parallel()

	db := newStreamsTestDB(t)
	ctx := t.Context()

	_, err := db.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ShardIterator",
		"GetRecords without ShardIterator must return a validation error")
}

func TestUnit_Streams_GetRecords_InvalidToken(t *testing.T) {
	t.Parallel()

	db := newStreamsTestDB(t)
	ctx := t.Context()

	_, err := db.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
		ShardIterator: aws.String("completelygarbagetoken"),
	})
	require.Error(t, err)
}

func TestUnit_Streams_GetRecords_SequenceContinuation(t *testing.T) {
	t.Parallel()

	db := newStreamsTestDB(t)
	ctx := t.Context()

	require.NoError(t, db.EnableStream(ctx, "StreamsTestTable", "KEYS_ONLY"))

	for i := range 6 {
		_, err := db.PutItem(ctx, makePutItemN("StreamsTestTable", "pk", i))
		require.NoError(t, err)
	}

	table, ok := db.GetTable("StreamsTestTable")
	require.True(t, ok)

	// Read 3 at a time and verify we get all 6 with no overlap.
	iterOut, err := db.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         aws.String(table.StreamARN),
		ShardId:           aws.String(ddb.StreamShardID),
		ShardIteratorType: streamstypes.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)

	limit := int32(3)
	seen := make(map[string]bool)

	for range 2 {
		rOut, rErr := db.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
			ShardIterator: iterOut.ShardIterator,
			Limit:         &limit,
		})
		require.NoError(t, rErr)
		require.Len(t, rOut.Records, 3)
		for _, r := range rOut.Records {
			seq := aws.ToString(r.Dynamodb.SequenceNumber)
			assert.False(t, seen[seq], "duplicate sequence number: %s", seq)
			seen[seq] = true
		}
		iterOut.ShardIterator = rOut.NextShardIterator
	}

	assert.Len(t, seen, 6, "must have read exactly 6 distinct records")
}

// ============================================================
// Section 8: Sequence number format
// ============================================================

func TestUnit_Streams_SeqNum_ZeroPadded(t *testing.T) {
	t.Parallel()

	db := newStreamsTestDB(t)
	ctx := t.Context()

	require.NoError(t, db.EnableStream(ctx, "StreamsTestTable", "KEYS_ONLY"))

	_, err := db.PutItem(ctx, makePutItem("StreamsTestTable", "pk", "x"))
	require.NoError(t, err)

	table, ok := db.GetTable("StreamsTestTable")
	require.True(t, ok)
	require.Len(t, table.StreamRecords, 1)

	seq := table.StreamRecords[0].SequenceNumber
	assert.Len(t, seq, 20, "sequence number must be exactly 20 digits (zero-padded)")
	assert.True(t, strings.HasPrefix(seq, "0"), "sequence number must be zero-padded")
}

func TestUnit_Streams_SeqNum_ParseRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantStr string
		input   int64
	}{
		{name: "zero", input: 0, wantStr: ""},
		{name: "one", input: 1, wantStr: "00000000000000000001"},
		{name: "max 20-digit", input: 99999999999999999, wantStr: "00000000099999999999999999"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.input <= 0 {
				result := ddb.SeqNumString(tt.input)
				assert.Empty(t, result)

				return
			}

			s := ddb.SeqNumString(tt.input)
			assert.NotEmpty(t, s)

			parsed, err := ddb.ParseSeqNum(s)
			require.NoError(t, err)
			assert.Equal(t, tt.input, parsed)
		})
	}
}

// ============================================================
// Section 9: StreamViewType enforcement
// ============================================================

func TestUnit_Streams_ViewType_KeysOnly(t *testing.T) {
	t.Parallel()

	db := newStreamsTestDB(t)
	ctx := t.Context()

	require.NoError(t, db.EnableStream(ctx, "StreamsTestTable", "KEYS_ONLY"))

	_, err := db.PutItem(ctx, makePutItem("StreamsTestTable", "pk", "kval"))
	require.NoError(t, err)

	table, ok := db.GetTable("StreamsTestTable")
	require.True(t, ok)
	require.Len(t, table.StreamRecords, 1)

	rec := table.StreamRecords[0]
	assert.NotNil(t, rec.Keys, "KEYS_ONLY must include Keys")
	assert.Nil(t, rec.NewImage, "KEYS_ONLY must not include NewImage")
	assert.Nil(t, rec.OldImage, "KEYS_ONLY must not include OldImage")
}

func TestUnit_Streams_ViewType_NewAndOldImages_IncludesBothImages(t *testing.T) {
	t.Parallel()

	db := newStreamsTestDB(t)
	ctx := t.Context()

	require.NoError(t, db.EnableStream(ctx, "StreamsTestTable", "NEW_AND_OLD_IMAGES"))

	// First write — no old image.
	_, err := db.PutItem(ctx, makePutItem("StreamsTestTable", "pk", "val1"))
	require.NoError(t, err)

	// Second write — overwrites item, so old image should be present.
	_, err = db.PutItem(ctx, makePutItem("StreamsTestTable", "pk", "val1"))
	require.NoError(t, err)

	table, ok := db.GetTable("StreamsTestTable")
	require.True(t, ok)
	require.Len(t, table.StreamRecords, 2)

	insert := table.StreamRecords[0]
	modify := table.StreamRecords[1]

	assert.NotNil(t, insert.NewImage, "INSERT must have NewImage")
	assert.Nil(t, insert.OldImage, "INSERT must not have OldImage")

	assert.NotNil(t, modify.NewImage, "MODIFY must have NewImage")
	assert.NotNil(t, modify.OldImage, "MODIFY must have OldImage")
}

// ============================================================
// Section 10: Event types (INSERT / MODIFY / REMOVE)
// ============================================================

func TestUnit_Streams_EventTypes_CRUDSequence(t *testing.T) {
	t.Parallel()

	db := newStreamsTestDB(t)
	ctx := t.Context()

	require.NoError(t, db.EnableStream(ctx, "StreamsTestTable", "KEYS_ONLY"))

	_, err := db.PutItem(ctx, makePutItem("StreamsTestTable", "pk", "e1"))
	require.NoError(t, err)

	_, err = db.PutItem(ctx, makePutItem("StreamsTestTable", "pk", "e1"))
	require.NoError(t, err)

	_, err = db.DeleteItem(ctx, makeDeleteItem("StreamsTestTable", "pk", "e1"))
	require.NoError(t, err)

	table, ok := db.GetTable("StreamsTestTable")
	require.True(t, ok)
	require.Len(t, table.StreamRecords, 3)

	assert.Equal(t, "INSERT", table.StreamRecords[0].EventName)
	assert.Equal(t, "MODIFY", table.StreamRecords[1].EventName)
	assert.Equal(t, "REMOVE", table.StreamRecords[2].EventName)
}

// ============================================================
// Section 11: GetRecentEvents
// ============================================================

func TestUnit_Streams_GetRecentEvents(t *testing.T) {
	t.Parallel()

	db := newStreamsTestDB(t)
	ctx := t.Context()

	require.NoError(t, db.EnableStream(ctx, "StreamsTestTable", "NEW_IMAGE"))

	for i := range 3 {
		_, err := db.PutItem(ctx, makePutItemN("StreamsTestTable", "pk", i))
		require.NoError(t, err)
	}

	events := db.GetRecentEvents("StreamsTestTable")
	assert.Len(t, events, 3)
}

func TestUnit_Streams_GetRecentEvents_UnknownTable(t *testing.T) {
	t.Parallel()

	db := ddb.NewInMemoryDB()
	events := db.GetRecentEvents("NoSuchTable")
	assert.Nil(t, events)
}
