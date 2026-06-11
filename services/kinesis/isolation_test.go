package kinesis //nolint:testpackage // needs access to the unexported region context key.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ctxRegion returns a context carrying the given AWS region, mirroring what the
// handler injects from the SigV4 credential scope.
func ctxRegion(region string) context.Context {
	return contextWithRegion(context.Background(), region)
}

// TestKinesisRegionIsolation proves that a same-named stream created in two
// regions stays fully isolated: ARNs, shards, and records do not leak across
// regions, and deleting in one region leaves the other intact.
func TestKinesisRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackendWithConfig("000000000000", "us-east-1")

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	// 1. Create a stream named "shared" in us-east-1.
	require.NoError(t, backend.CreateStream(ctxEast, &CreateStreamInput{
		StreamName: "shared",
		ShardCount: 1,
	}))

	// 2. Create a stream with the SAME NAME in us-west-2.
	require.NoError(t, backend.CreateStream(ctxWest, &CreateStreamInput{
		StreamName: "shared",
		ShardCount: 2,
	}))

	// 3. Each region's stream carries its own ARN region and shard count.
	eastDesc, err := backend.DescribeStream(ctxEast, &DescribeStreamInput{StreamName: "shared"})
	require.NoError(t, err)
	assert.Contains(t, eastDesc.StreamARN, "us-east-1")
	assert.Len(t, eastDesc.Shards, 1)

	westDesc, err := backend.DescribeStream(ctxWest, &DescribeStreamInput{StreamName: "shared"})
	require.NoError(t, err)
	assert.Contains(t, westDesc.StreamARN, "us-west-2")
	assert.Len(t, westDesc.Shards, 2)

	// 4. ListStreams in each region sees only its own stream.
	eastList, err := backend.ListStreams(ctxEast, &ListStreamsInput{})
	require.NoError(t, err)
	assert.Equal(t, []string{"shared"}, eastList.StreamNames)

	westList, err := backend.ListStreams(ctxWest, &ListStreamsInput{})
	require.NoError(t, err)
	assert.Equal(t, []string{"shared"}, westList.StreamNames)

	// A third region that was never written sees no streams.
	emptyList, err := backend.ListStreams(ctxRegion("eu-west-1"), &ListStreamsInput{})
	require.NoError(t, err)
	assert.Empty(t, emptyList.StreamNames)

	// 5. Delete the stream in us-east-1; us-west-2 still has its stream.
	require.NoError(t, backend.DeleteStream(ctxEast, &DeleteStreamInput{StreamName: "shared"}))

	_, err = backend.DescribeStream(ctxEast, &DescribeStreamInput{StreamName: "shared"})
	require.ErrorIs(t, err, ErrStreamNotFound)

	stillWest, err := backend.DescribeStream(ctxWest, &DescribeStreamInput{StreamName: "shared"})
	require.NoError(t, err)
	assert.Contains(t, stillWest.StreamARN, "us-west-2")
}

// TestKinesisRecordRegionIsolation proves records written to a same-named
// stream in two regions never cross over, and that a shard iterator issued in
// one region reads only that region's records even when GetRecords carries a
// different ctx region (the region is encoded into the iterator token).
func TestKinesisRecordRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackendWithConfig("000000000000", "us-east-1")

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	for _, c := range []context.Context{ctxEast, ctxWest} {
		require.NoError(t, backend.CreateStream(c, &CreateStreamInput{
			StreamName: "events",
			ShardCount: 1,
		}))
	}

	// Write distinct records into each region's stream.
	_, err := backend.PutRecord(ctxEast, &PutRecordInput{
		StreamName:   "events",
		PartitionKey: "pk",
		Data:         []byte("east-record"),
	})
	require.NoError(t, err)

	_, err = backend.PutRecord(ctxWest, &PutRecordInput{
		StreamName:   "events",
		PartitionKey: "pk",
		Data:         []byte("west-record"),
	})
	require.NoError(t, err)

	// us-east-1 reads only its own record.
	eastIt, err := backend.GetShardIterator(ctxEast, &GetShardIteratorInput{
		StreamName:        "events",
		ShardID:           "shardId-000000000000",
		ShardIteratorType: iteratorTypeTrimHorizon,
	})
	require.NoError(t, err)

	// Deliberately call GetRecords with the WEST ctx to prove the iterator's
	// embedded region — not the ctx region — selects the record store.
	eastRecs, err := backend.GetRecords(ctxWest, &GetRecordsInput{ShardIterator: eastIt.ShardIterator})
	require.NoError(t, err)
	require.Len(t, eastRecs.Records, 1)
	assert.Equal(t, []byte("east-record"), eastRecs.Records[0].Data)

	// us-west-2 reads only its own record.
	westIt, err := backend.GetShardIterator(ctxWest, &GetShardIteratorInput{
		StreamName:        "events",
		ShardID:           "shardId-000000000000",
		ShardIteratorType: iteratorTypeTrimHorizon,
	})
	require.NoError(t, err)

	westRecs, err := backend.GetRecords(ctxEast, &GetRecordsInput{ShardIterator: westIt.ShardIterator})
	require.NoError(t, err)
	require.Len(t, westRecs.Records, 1)
	assert.Equal(t, []byte("west-record"), westRecs.Records[0].Data)
}

// TestKinesisConsumerRegionIsolation proves enhanced fan-out consumers are
// isolated per region: registering a same-named consumer on a same-named
// stream in two regions does not collide, and each region lists only its own.
func TestKinesisConsumerRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackendWithConfig("000000000000", "us-east-1")

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	require.NoError(t, backend.CreateStream(ctxEast, &CreateStreamInput{StreamName: "s", ShardCount: 1}))
	require.NoError(t, backend.CreateStream(ctxWest, &CreateStreamInput{StreamName: "s", ShardCount: 1}))

	eastDesc, err := backend.DescribeStream(ctxEast, &DescribeStreamInput{StreamName: "s"})
	require.NoError(t, err)
	westDesc, err := backend.DescribeStream(ctxWest, &DescribeStreamInput{StreamName: "s"})
	require.NoError(t, err)

	// The consumer ARN carries its region via the stream ARN, so the backend
	// routes RegisterStreamConsumer to that region regardless of ctx.
	_, err = backend.RegisterStreamConsumer(ctxEast, &RegisterStreamConsumerInput{
		StreamARN:    eastDesc.StreamARN,
		ConsumerName: "fanout",
	})
	require.NoError(t, err)

	_, err = backend.RegisterStreamConsumer(ctxWest, &RegisterStreamConsumerInput{
		StreamARN:    westDesc.StreamARN,
		ConsumerName: "fanout",
	})
	require.NoError(t, err)

	eastConsumers, err := backend.ListStreamConsumers(ctxEast, &ListStreamConsumersInput{StreamARN: eastDesc.StreamARN})
	require.NoError(t, err)
	require.Len(t, eastConsumers.Consumers, 1)
	assert.Contains(t, eastConsumers.Consumers[0].ConsumerARN, "us-east-1")

	westConsumers, err := backend.ListStreamConsumers(ctxWest, &ListStreamConsumersInput{StreamARN: westDesc.StreamARN})
	require.NoError(t, err)
	require.Len(t, westConsumers.Consumers, 1)
	assert.Contains(t, westConsumers.Consumers[0].ConsumerARN, "us-west-2")
}
