package kinesis_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesissdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	kinesistypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

// TestGetRecords_ChildShards drives SplitShard then GetRecords through the
// real SDK client and asserts ChildShards, which real AWS returns "in the
// GetRecords API's response only when the end of the current shard is
// reached" (types.GetRecordsOutput doc comment) -- exactly when a shard is
// Closed and every record in it has been consumed, the same condition this
// backend already uses to null out NextShardIterator.
func TestGetRecords_ChildShards(t *testing.T) {
	t.Parallel()

	backend := kinesis.NewInMemoryBackend()
	client := newTestKinesisClient(t, kinesis.NewHandler(backend))

	streamName := "split-child-shards-stream"
	_, err := client.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	desc, err := client.DescribeStream(t.Context(), &kinesissdk.DescribeStreamInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)
	require.Len(t, desc.StreamDescription.Shards, 1)

	parentShardID := aws.ToString(desc.StreamDescription.Shards[0].ShardId)

	const splitKey = "170141183460469231731687303715884105728" // midpoint of 0..2^128-1
	_, err = client.SplitShard(t.Context(), &kinesissdk.SplitShardInput{
		StreamName:         aws.String(streamName),
		ShardToSplit:       aws.String(parentShardID),
		NewStartingHashKey: aws.String(splitKey),
	})
	require.NoError(t, err)

	iterOut, err := client.GetShardIterator(t.Context(), &kinesissdk.GetShardIteratorInput{
		StreamName:        aws.String(streamName),
		ShardId:           aws.String(parentShardID),
		ShardIteratorType: kinesistypes.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)

	recOut, err := client.GetRecords(t.Context(), &kinesissdk.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
	})
	require.NoError(t, err)

	// The parent shard is closed and has no records, so GetRecords reaches
	// its end immediately: NextShardIterator is nil and ChildShards is
	// populated with both children produced by the split.
	assert.Nil(t, recOut.NextShardIterator)
	require.Len(t, recOut.ChildShards, 2)

	for _, cs := range recOut.ChildShards {
		require.Len(t, cs.ParentShards, 1)
		assert.Equal(t, parentShardID, cs.ParentShards[0])
		assert.NotEmpty(t, aws.ToString(cs.ShardId))
		require.NotNil(t, cs.HashKeyRange)
		assert.NotEmpty(t, aws.ToString(cs.HashKeyRange.StartingHashKey))
		assert.NotEmpty(t, aws.ToString(cs.HashKeyRange.EndingHashKey))
	}

	gotChildIDs := map[string]bool{
		aws.ToString(recOut.ChildShards[0].ShardId): true,
		aws.ToString(recOut.ChildShards[1].ShardId): true,
	}
	assert.Len(t, gotChildIDs, 2, "child shard IDs must be distinct")
}
