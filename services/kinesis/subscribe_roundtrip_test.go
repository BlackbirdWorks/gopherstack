package kinesis_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesissdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	kinesissdktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

func TestSubscribeToShard_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := kinesis.NewInMemoryBackend()
	client := newTestKinesisClient(t, kinesis.NewHandler(backend))

	streamName := "subscribe-smoke-stream"
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
		ConsumerName: aws.String("smoke-consumer"),
	})
	require.NoError(t, err)

	_, err = client.PutRecord(t.Context(), &kinesissdk.PutRecordInput{
		StreamName:   aws.String(streamName),
		PartitionKey: aws.String("pk"),
		Data:         []byte("hello"),
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
	defer stream.Close()

	select {
	case ev := <-stream.Events():
		require.NotNil(t, ev,
			"expected at least one event from the real SDK event reader; stream.Err()=%v", stream.Err())
		if e, ok := ev.(*kinesissdktypes.SubscribeToShardEventStreamMemberSubscribeToShardEvent); ok {
			t.Logf("got SubscribeToShardEvent with %d records", len(e.Value.Records))
		} else {
			t.Fatalf("unexpected event type %T", ev)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for an event from the real SDK's event stream reader")
	}
}
