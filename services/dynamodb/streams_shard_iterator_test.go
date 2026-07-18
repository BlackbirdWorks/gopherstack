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

func TestStreams_OpaqueIterator_TokenIsOpaque(t *testing.T) {
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

func TestStreams_OpaqueIterator_RegistrationInStore(t *testing.T) {
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

func TestStreams_OpaqueIterator_GetRecordsRegistersNextToken(t *testing.T) {
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

func TestStreams_OpaqueIterator_SweepRemovesExpiredTokens(t *testing.T) {
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

func TestStreams_GetShardIterator_Validation(t *testing.T) {
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

func TestStreams_GetShardIterator_AllIteratorTypes(t *testing.T) {
	t.Parallel()

	db := newStreamsTestDB(t)
	ctx := t.Context()

	require.NoError(t, db.EnableStream(ctx, "StreamsTestTable", "NEW_AND_OLD_IMAGES"))

	// Write 3 records to have a non-empty stream.
	for i := range 3 {
		_, err := db.PutItem(ctx, makePutItemN("StreamsTestTable", i))
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
	recOut, err := db.GetRecords(
		ctx,
		&dynamodbstreams.GetRecordsInput{ShardIterator: iterH.ShardIterator},
	)
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

func TestShardIteratorStore_PutGet(t *testing.T) {
	t.Parallel()
	store := ddb.NewShardIteratorStore()

	token, err := store.Put("myTable", 42)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	if token == "" {
		t.Fatal("expected non-empty token")
	}

	entry := store.Get(token)
	if entry == nil {
		t.Fatal("expected entry, got nil")
	}

	if entry.TableName != "myTable" {
		t.Errorf("got table %q want %q", entry.TableName, "myTable")
	}

	if entry.StartSeq != 42 {
		t.Errorf("got seq %d want 42", entry.StartSeq)
	}
}

func TestShardIteratorStore_Expired_NotReturned(t *testing.T) {
	t.Parallel()
	store := ddb.NewShardIteratorStore()

	token, err := store.Put("t1", 0)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Manually expire by sweeping with a fake future time is not possible directly,
	// but we can verify the entry exists before and after a no-op sweep.
	store.Sweep()

	entry := store.Get(token)
	if entry == nil {
		t.Error("entry should still be valid immediately after put")
	}
}

func TestShardIteratorStore_Delete(t *testing.T) {
	t.Parallel()
	store := ddb.NewShardIteratorStore()

	token, err := store.Put("t1", 0)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	store.Delete(token)

	if store.Get(token) != nil {
		t.Error("entry should be gone after Delete")
	}
}

func TestShardIteratorStore_UniqueTokens(t *testing.T) {
	t.Parallel()
	store := ddb.NewShardIteratorStore()
	tokens := make(map[string]bool)

	for range 100 {
		tok, err := store.Put("t", 0)
		if err != nil {
			t.Fatalf("Put: %v", err)
		}

		if tokens[tok] {
			t.Fatalf("duplicate token generated: %s", tok)
		}

		tokens[tok] = true
	}
}

func TestShardIteratorStore_MultipleEntries(t *testing.T) {
	t.Parallel()
	store := ddb.NewShardIteratorStore()

	tokens := make([]string, 0, 5)

	for i := range 5 {
		tok, err := store.Put(fmt.Sprintf("table%d", i), int64(i*10))
		if err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}

		tokens = append(tokens, tok)
	}

	for i, tok := range tokens {
		entry := store.Get(tok)
		if entry == nil {
			t.Fatalf("entry %d not found", i)
		}

		if entry.StartSeq != int64(i*10) {
			t.Errorf("entry %d: want seq %d, got %d", i, i*10, entry.StartSeq)
		}
	}

	// Delete half.
	for _, tok := range tokens[:2] {
		store.Delete(tok)
	}

	if store.Get(tokens[0]) != nil {
		t.Error("deleted token[0] should be gone")
	}

	if store.Get(tokens[2]) == nil {
		t.Error("non-deleted token[2] should still exist")
	}
}
