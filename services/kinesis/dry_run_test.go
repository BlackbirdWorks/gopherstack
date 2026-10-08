package kinesis_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesissdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	kinesistypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

func TestDryRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(t *testing.T, c *kinesissdk.Client, shardID, iterator string) error
		name string
	}{
		{
			name: "put record",
			call: func(t *testing.T, c *kinesissdk.Client, _, _ string) error {
				t.Helper()

				_, err := c.PutRecord(t.Context(), &kinesissdk.PutRecordInput{
					StreamName: aws.String(
						"s",
					), PartitionKey: aws.String("pk"), Data: []byte("x"), DryRun: aws.Bool(true),
				})

				return err
			},
		},
		{
			name: "put records",
			call: func(t *testing.T, c *kinesissdk.Client, _, _ string) error {
				t.Helper()

				_, err := c.PutRecords(t.Context(), &kinesissdk.PutRecordsInput{
					StreamName: aws.String("s"), DryRun: aws.Bool(true),
					Records: []kinesistypes.PutRecordsRequestEntry{{PartitionKey: aws.String("pk"), Data: []byte("x")}},
				})

				return err
			},
		},
		{
			name: "get shard iterator",
			call: func(t *testing.T, c *kinesissdk.Client, shardID, _ string) error {
				t.Helper()

				_, err := c.GetShardIterator(t.Context(), &kinesissdk.GetShardIteratorInput{
					StreamName: aws.String("s"), ShardId: aws.String(shardID),
					ShardIteratorType: kinesistypes.ShardIteratorTypeTrimHorizon, DryRun: aws.Bool(true),
				})

				return err
			},
		},
		{
			name: "get records",
			call: func(t *testing.T, c *kinesissdk.Client, _, iterator string) error {
				t.Helper()

				_, err := c.GetRecords(t.Context(), &kinesissdk.GetRecordsInput{
					ShardIterator: aws.String(iterator), DryRun: aws.Bool(true),
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			clock := newFakeClock(time.Now())
			backend := kinesis.NewInMemoryBackend().WithClock(clock.Now)
			client := newTestKinesisClient(t, kinesis.NewHandler(backend))

			_, err := client.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
				StreamName: aws.String("s"), ShardCount: aws.Int32(1),
			})
			require.NoError(t, err)
			clock.Advance(streamSettleWait)

			put, err := client.PutRecord(t.Context(), &kinesissdk.PutRecordInput{
				StreamName: aws.String("s"), PartitionKey: aws.String("pk"), Data: []byte("seed"),
			})
			require.NoError(t, err)

			it, err := client.GetShardIterator(t.Context(), &kinesissdk.GetShardIteratorInput{
				StreamName: aws.String("s"), ShardId: put.ShardId,
				ShardIteratorType: kinesistypes.ShardIteratorTypeTrimHorizon,
			})
			require.NoError(t, err)

			err = tt.call(t, client, aws.ToString(put.ShardId), aws.ToString(it.ShardIterator))

			var dry *kinesistypes.DryRunOperationException
			require.ErrorAs(t, err, &dry)

			out, err := client.GetRecords(t.Context(), &kinesissdk.GetRecordsInput{ShardIterator: it.ShardIterator})
			require.NoError(t, err)
			assert.Len(t, out.Records, 1, "a dry run must not write records or consume the iterator")
		})
	}
}
