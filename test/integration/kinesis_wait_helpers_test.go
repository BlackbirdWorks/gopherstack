package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/stretchr/testify/require"
)

const kinesisWaiterMaxWait = 30 * time.Second

// waitKinesisStreamActive waits for a just-created stream to leave CREATING,
// the way a real SDK caller does before using a new stream.
func waitKinesisStreamActive(ctx context.Context, t *testing.T, client *kinesis.Client, streamName string) {
	t.Helper()

	waiter := kinesis.NewStreamExistsWaiter(client, func(o *kinesis.StreamExistsWaiterOptions) {
		o.MinDelay = 50 * time.Millisecond
		o.MaxDelay = 500 * time.Millisecond
	})
	err := waiter.Wait(ctx, &kinesis.DescribeStreamInput{StreamName: aws.String(streamName)}, kinesisWaiterMaxWait)
	require.NoError(t, err, "stream %q did not become ACTIVE", streamName)
}

// waitKinesisStreamGone waits for a deleted stream to actually disappear.
func waitKinesisStreamGone(ctx context.Context, t *testing.T, client *kinesis.Client, streamName string) {
	t.Helper()

	waiter := kinesis.NewStreamNotExistsWaiter(client, func(o *kinesis.StreamNotExistsWaiterOptions) {
		o.MinDelay = 50 * time.Millisecond
		o.MaxDelay = 500 * time.Millisecond
	})
	err := waiter.Wait(ctx, &kinesis.DescribeStreamInput{StreamName: aws.String(streamName)}, kinesisWaiterMaxWait)
	require.NoError(t, err, "stream %q was not removed", streamName)
}
