package dynamodb

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdkdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	sdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newStreamTable(t *testing.T, db *InMemoryDB, name string) string {
	t.Helper()

	in := createDelayedTableInput(name)
	in.StreamSpecification = &sdktypes.StreamSpecification{
		StreamEnabled:  aws.Bool(true),
		StreamViewType: sdktypes.StreamViewTypeNewAndOldImages,
	}

	_, err := db.CreateTable(t.Context(), in)
	require.NoError(t, err)

	_, err = db.PutItem(t.Context(), &sdkdynamodb.PutItemInput{
		TableName: aws.String(name),
		Item:      map[string]sdktypes.AttributeValue{"pk": &sdktypes.AttributeValueMemberS{Value: "a"}},
	})
	require.NoError(t, err)

	tbl, ok := db.GetTable(name)
	require.True(t, ok)

	return tbl.StreamARN
}

func disableStreamVia(t *testing.T, db *InMemoryDB, how, name string) {
	t.Helper()

	switch how {
	case "updatetable":
		_, err := db.UpdateTable(t.Context(), &sdkdynamodb.UpdateTableInput{
			TableName:           aws.String(name),
			StreamSpecification: &sdktypes.StreamSpecification{StreamEnabled: aws.Bool(false)},
		})
		require.NoError(t, err)
	default:
		require.NoError(t, db.DisableStream(t.Context(), name))
	}
}

func TestRetiredStream_StaysReadableAfterDisable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		how  string
	}{
		{name: "updatetable", how: "updatetable"},
		{name: "disablestream", how: "disablestream"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := NewInMemoryDB()
			ctx := t.Context()
			oldARN := newStreamTable(t, db, "rs-"+tt.name)

			disableStreamVia(t, db, tt.how, "rs-"+tt.name)

			list, err := db.ListStreams(ctx, &dynamodbstreams.ListStreamsInput{})
			require.NoError(t, err)
			require.Len(t, list.Streams, 1)
			assert.Equal(t, oldARN, aws.ToString(list.Streams[0].StreamArn))

			desc, err := db.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{StreamArn: aws.String(oldARN)})
			require.NoError(t, err)
			assert.Equal(t, streamstypes.StreamStatusDisabled, desc.StreamDescription.StreamStatus)
			require.Len(t, desc.StreamDescription.Shards, 1)
			shard := desc.StreamDescription.Shards[0]
			assert.NotEmpty(t, aws.ToString(shard.SequenceNumberRange.EndingSequenceNumber))

			it, err := db.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
				StreamArn:         aws.String(oldARN),
				ShardId:           shard.ShardId,
				ShardIteratorType: streamstypes.ShardIteratorTypeTrimHorizon,
			})
			require.NoError(t, err)

			recs, err := db.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{ShardIterator: it.ShardIterator})
			require.NoError(t, err)
			require.Len(t, recs.Records, 1)
			assert.Nil(t, recs.NextShardIterator)

			_, err = db.UpdateTable(ctx, &sdkdynamodb.UpdateTableInput{
				TableName: aws.String("rs-" + tt.name),
				StreamSpecification: &sdktypes.StreamSpecification{
					StreamEnabled:  aws.Bool(true),
					StreamViewType: sdktypes.StreamViewTypeKeysOnly,
				},
			})
			require.NoError(t, err)

			list, err = db.ListStreams(ctx, &dynamodbstreams.ListStreamsInput{})
			require.NoError(t, err)
			require.Len(t, list.Streams, 2)

			tbl, ok := db.GetTable("rs-" + tt.name)
			require.True(t, ok)
			assert.NotEqual(t, oldARN, tbl.StreamARN)
		})
	}
}

func TestRetiredStream_DisablingDelayAndExpiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus streamstypes.StreamStatus
		delay      time.Duration
	}{
		{name: "default", delay: 0, wantStatus: streamstypes.StreamStatusDisabled},
		{name: "delayed", delay: time.Hour, wantStatus: streamstypes.StreamStatusDisabling},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := NewInMemoryDB()
			db.SetStreamDisablingDelay(tt.delay)
			ctx := t.Context()
			oldARN := newStreamTable(t, db, "rs-exp")
			disableStreamVia(t, db, "updatetable", "rs-exp")

			desc, err := db.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{StreamArn: aws.String(oldARN)})
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, desc.StreamDescription.StreamStatus)

			tbl, ok := db.GetTable("rs-exp")
			require.True(t, ok)
			assert.Zero(t, sweepRetiredStreams(tbl, time.Now().Add(retiredStreamRetention-time.Minute)))
			assert.Equal(t, 1, sweepRetiredStreams(tbl, time.Now().Add(retiredStreamRetention+time.Minute)))

			_, err = db.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{StreamArn: aws.String(oldARN)})
			require.Error(t, err)

			list, err := db.ListStreams(ctx, &dynamodbstreams.ListStreamsInput{})
			require.NoError(t, err)
			assert.Empty(t, list.Streams)
		})
	}
}

func TestRetiredStream_SurvivesSnapshotRestore(t *testing.T) {
	t.Parallel()

	db := NewInMemoryDB()
	oldARN := newStreamTable(t, db, "rs-snap")
	disableStreamVia(t, db, "updatetable", "rs-snap")

	restored := NewInMemoryDB()
	require.NoError(t, restored.Restore(t.Context(), db.Snapshot(t.Context())))

	desc, err := restored.DescribeStream(
		t.Context(), &dynamodbstreams.DescribeStreamInput{StreamArn: aws.String(oldARN)},
	)
	require.NoError(t, err)
	assert.Equal(t, streamstypes.StreamStatusDisabled, desc.StreamDescription.StreamStatus)

	it, err := restored.GetShardIterator(t.Context(), &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         aws.String(oldARN),
		ShardId:           desc.StreamDescription.Shards[0].ShardId,
		ShardIteratorType: streamstypes.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)

	recs, err := restored.GetRecords(t.Context(), &dynamodbstreams.GetRecordsInput{ShardIterator: it.ShardIterator})
	require.NoError(t, err)
	assert.Len(t, recs.Records, 1)
}
