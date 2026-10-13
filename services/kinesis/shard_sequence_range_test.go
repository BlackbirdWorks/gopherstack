package kinesis_test

import (
	"math/big"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesissdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

func seqNum(t *testing.T, s *string) *big.Int {
	t.Helper()

	n, ok := new(big.Int).SetString(aws.ToString(s), 10)
	require.True(t, ok, "sequence number %q is not numeric", aws.ToString(s))

	return n
}

func TestShardSequenceNumberRanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		split bool
	}{
		{name: "fresh open shard"},
		{name: "split parent and children", split: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			clock := newFakeClock(time.Now())
			backend := kinesis.NewInMemoryBackend().WithClock(clock.Now)
			client := newTestKinesisClient(t, kinesis.NewHandler(backend))

			_, err := client.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
				StreamName: aws.String("seq-ranges"), ShardCount: aws.Int32(1),
			})
			require.NoError(t, err)
			clock.Advance(streamSettleWait)

			if tt.split {
				_, err = client.SplitShard(t.Context(), &kinesissdk.SplitShardInput{
					StreamName:         aws.String("seq-ranges"),
					ShardToSplit:       aws.String("shardId-000000000000"),
					NewStartingHashKey: aws.String("170141183460469231731687303715884105728"),
				})
				require.NoError(t, err)
			}

			desc, err := client.DescribeStream(t.Context(), &kinesissdk.DescribeStreamInput{
				StreamName: aws.String("seq-ranges"),
			})
			require.NoError(t, err)

			for _, shard := range desc.StreamDescription.Shards {
				start := seqNum(t, shard.SequenceNumberRange.StartingSequenceNumber)
				assert.Positive(
					t,
					start.Sign(),
					"shard %s must start at a real sequence number",
					aws.ToString(shard.ShardId),
				)

				if shard.SequenceNumberRange.EndingSequenceNumber != nil {
					end := seqNum(t, shard.SequenceNumberRange.EndingSequenceNumber)
					assert.Positive(t, end.Cmp(start), "closed shard end must follow its start")
				}
			}
		})
	}
}
