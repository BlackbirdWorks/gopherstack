package kinesis_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesissdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	kinesissdktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

// TestSubscribeToShard_IdleCloseIsGraceful reproduces
// test/integration/kinesis_test.go's TestIntegration_Kinesis_EnhancedFanOut
// shape (gopherstack-j60e) over a real TCP loopback connection
// (httptest.NewServer, not the in-process httptest.ResponseRecorder the
// rest of this file's helpers use), to determine whether the handler's
// deadline close (handler_consumers.go's subscribeToShardStreamDuration,
// shortened here via WithSubscribeToShardTiming so the test doesn't wait
// out the real 5-minute window) ends the stream cleanly for a real SDK
// client, or surfaces as stream.Err() != nil. Before gopherstack-s0ju item
// 4, this stream closed after 3 empty polls (~600ms) instead of staying
// open with heartbeats until the documented deadline -- this test now also
// confirms at least one heartbeat (an empty-Records SubscribeToShardEvent
// with a non-empty ContinuationSequenceNumber) is observed before close,
// which the old idle-close behavior never sent at all.
func TestSubscribeToShard_IdleCloseIsGraceful(t *testing.T) {
	t.Parallel()

	const (
		streamDuration    = 300 * time.Millisecond
		pollInterval      = 10 * time.Millisecond
		heartbeatInterval = 40 * time.Millisecond
	)

	backend := kinesis.NewInMemoryBackend()
	client := newTestKinesisClient(
		t,
		kinesis.NewHandler(backend).WithSubscribeToShardTiming(streamDuration, pollInterval, heartbeatInterval),
	)

	streamName := "idle-close-stream"
	_, err := client.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	desc, err := client.DescribeStream(t.Context(), &kinesissdk.DescribeStreamInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)
	shardID := *desc.StreamDescription.Shards[0].ShardId

	consOut, err := client.RegisterStreamConsumer(t.Context(), &kinesissdk.RegisterStreamConsumerInput{
		StreamARN:    desc.StreamDescription.StreamARN,
		ConsumerName: aws.String("idle-close-consumer"),
	})
	require.NoError(t, err)

	_, err = client.PutRecord(t.Context(), &kinesissdk.PutRecordInput{
		StreamName:   aws.String(streamName),
		PartitionKey: aws.String("pk"),
		Data:         []byte("idle-close-payload"),
	})
	require.NoError(t, err)

	out, err := client.SubscribeToShard(t.Context(), &kinesissdk.SubscribeToShardInput{
		ConsumerARN: consOut.Consumer.ConsumerARN,
		ShardId:     aws.String(shardID),
		StartingPosition: &kinesissdktypes.StartingPosition{
			Type: kinesissdktypes.ShardIteratorTypeTrimHorizon,
		},
	})
	require.NoError(t, err, "SubscribeToShard call itself (opens event stream)")

	stream := out.GetStream()
	require.NotNil(t, stream)

	var got []string
	var sawHeartbeat bool
	for event := range stream.Events() {
		ev, ok := event.(*kinesissdktypes.SubscribeToShardEventStreamMemberSubscribeToShardEvent)
		if !ok {
			continue
		}
		for _, r := range ev.Value.Records {
			got = append(got, string(r.Data))
		}
		if len(ev.Value.Records) == 0 && ev.Value.ContinuationSequenceNumber != nil &&
			*ev.Value.ContinuationSequenceNumber != "" {
			sawHeartbeat = true
		}
	}
	require.NoError(t, stream.Err(),
		"the emulator's deadline close should end the SDK's event stream cleanly (io.EOF), not as a socket error")
	assert.Contains(t, got, "idle-close-payload")
	assert.True(t, sawHeartbeat,
		"expected at least one heartbeat SubscribeToShardEvent (empty Records, real ContinuationSequenceNumber) "+
			"before the stream's deadline closed it")
}
